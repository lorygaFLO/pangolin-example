"""Ad-hoc (non-pattern-match) processor: for each ticker's consolidated
history file, forecasts the next S.STOCK_FORECAST_HORIZON trading days with
Amazon's Chronos time-series model, and writes back ONE file per ticker
containing BOTH the historical rows and the forecast rows — distinguished
by a 'record_type' column ('history' | 'forecast'). No separate forecast
file, no database: history and forecast live side by side in the same CSV.

No registry, no pattern matching: it reads the persistent history via the
DataFacility / FSWrapper directly, following the same standalone-class
convention as pangolin's own BackupRestore processor.
"""

from __future__ import annotations

from datetime import timedelta

import numpy as np
import polars as pl
import torch
from chronos import ChronosPipeline

from pangolin.config.run_context import RunContext
from pangolin.config.settings import get_settings
from pangolin.engine.DataFacility import get_project_data
from pangolin.engine.common.exceptions import NoInputFilesError
from pangolin.engine.common.logger import ProcessorLogger
from pangolin.utils.fs_wrapper import FSWrapper

# Output schema, in this exact order — history rows and forecast rows share
# it, which is what lets them live in the same file. open/high/low/volume
# are left null on forecast rows: Chronos here forecasts 'close' only.
_OUTPUT_SCHEMA = {
    "ticker": pl.Utf8,
    "date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "record_type": pl.Utf8,
    "forecast_low": pl.Float64,
    "forecast_high": pl.Float64,
}
_OUTPUT_COLUMNS = list(_OUTPUT_SCHEMA.keys())


def _next_trading_days(start_date, count: int) -> list:
    """The next `count` trading days after start_date.

    Chronos has no notion of calendar time: it forecasts the next `count`
    points of whatever sequence it was given, and the history we feed it
    only contains trading days (yfinance never returns weekend rows) — so
    the forecast dates must skip weekends too, or they drift out of sync
    with what was actually predicted. This is a simple weekday (Mon-Fri)
    calendar; it does not account for market holidays, which is an
    acceptable simplification for this example.
    """
    dates = []
    current = start_date
    while len(dates) < count:
        current += timedelta(days=1)
        if current.weekday() < 5:  # Monday=0 ... Sunday=6
            dates.append(current)
    return dates


class ChronosForecaster:
    """Reads every '<TICKER>_STOCK_PRICES.csv' history file in input_folder,
    forecasts S.STOCK_FORECAST_HORIZON days ahead with Chronos, and writes
    '<TICKER>_STOCK_PRICES.csv' (history + forecast rows) to output_folder.
    """

    def __init__(self, CTX: RunContext, name: str, input_folder: str, output_folder: str):
        if not name:
            raise ValueError("Step name must be provided")
        if not input_folder:
            raise ValueError("input_folder must be provided")
        if not output_folder:
            raise ValueError("output_folder must be provided")

        S = get_settings()
        self.S = S
        self.CTX = CTX
        self.name = name
        self.log = ProcessorLogger(name)

        self.fs = FSWrapper(
            protocol=getattr(S, "FS_PROTOCOL", "file"),
            **getattr(S, "FS_OPTIONS", {}),
        )
        self.D = get_project_data(run_id=CTX.RUN_ID)
        self.input_node = self._get_node_by_path(input_folder)
        self.output_node = self._get_node_by_path(output_folder)
        self.fs.makedirs(str(self.output_node.path), exist_ok=True)

        # Loaded lazily on first use, not in __init__: instantiating this
        # processor (e.g. for testing) shouldn't require downloading a model.
        self._pipeline = None

    def _get_node_by_path(self, path_str: str):
        """Navigate to a node in DataFacility using dot notation."""
        node = self.D
        for part in path_str.split("."):
            node = getattr(node, part)
        return node

    def _load_pipeline(self) -> ChronosPipeline:
        if self._pipeline is None:
            self.log.info(f"Loading Chronos model '{self.S.CHRONOS_MODEL}' (first call only, may take a while)")
            self._pipeline = ChronosPipeline.from_pretrained(
                self.S.CHRONOS_MODEL,
                device_map="cpu",
                torch_dtype=torch.float32,
            )
        return self._pipeline

    def _forecast_ticker(self, history: pl.DataFrame) -> pl.DataFrame:
        """Run Chronos on one ticker's 'close' series and return
        prediction_length rows in _OUTPUT_SCHEMA (record_type='forecast').
        """
        horizon = self.S.STOCK_FORECAST_HORIZON
        ticker = history["ticker"][0]

        context = torch.tensor(history["close"].to_numpy(), dtype=torch.float32)
        forecast = self._load_pipeline().predict(context, prediction_length=horizon)
        samples = forecast[0].numpy()  # shape: (num_samples, horizon)
        low, median, high = np.quantile(samples, [0.1, 0.5, 0.9], axis=0)

        last_date = history["date"].max()
        forecast_dates = _next_trading_days(last_date, horizon)

        return pl.DataFrame(
            {
                "ticker": [ticker] * horizon,
                "date": forecast_dates,
                "open": [None] * horizon,
                "high": [None] * horizon,
                "low": [None] * horizon,
                "close": median.tolist(),
                "volume": [None] * horizon,
                "record_type": ["forecast"] * horizon,
                "forecast_low": low.tolist(),
                "forecast_high": high.tolist(),
            },
            schema=_OUTPUT_SCHEMA,
        ).select(_OUTPUT_COLUMNS)

    def execute(self):
        pattern = self.fs.join(str(self.input_node.path), "*_STOCK_PRICES.csv")
        files = [f for f in self.fs.glob(pattern) if self.fs.isfile(f)]
        if not files:
            raise NoInputFilesError(self.name, str(self.input_node.path))

        written = []
        for full_path in files:
            with self.fs.open(full_path, "rb") as f:
                history = pl.read_csv(f, separator=self.S.CSV_DELIMITER)
            if history.schema.get("date") != pl.Date:
                history = history.with_columns(pl.col("date").str.to_date("%Y-%m-%d"))

            ticker = history["ticker"][0]
            self.log.info(
                f"Forecasting '{ticker}': {self.S.STOCK_FORECAST_HORIZON} day(s) ahead "
                f"from {len(history)} historical row(s)"
            )

            history_rows = history.with_columns(
                pl.lit("history").alias("record_type"),
                pl.lit(None, dtype=pl.Float64).alias("forecast_low"),
                pl.lit(None, dtype=pl.Float64).alias("forecast_high"),
            ).select(_OUTPUT_COLUMNS)
            forecast_rows = self._forecast_ticker(history)

            combined = pl.concat([history_rows, forecast_rows], how="vertical").sort("date")

            output_filename = f"{ticker}_STOCK_PRICES.csv"
            output_path = self.fs.join(str(self.output_node.path), output_filename)
            with self.fs.open(output_path, "wb") as f:
                combined.write_csv(f, separator=self.S.CSV_DELIMITER)

            self.log.info(
                f"Wrote {len(combined)} row(s) ({len(history_rows)} history + "
                f"{len(forecast_rows)} forecast) to '{output_path}'"
            )
            written.append(ticker)

        return written
