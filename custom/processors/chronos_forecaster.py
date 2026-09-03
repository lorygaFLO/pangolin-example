"""Ad-hoc (non-pattern-match) processor: reads the single consolidated
history table (all tickers, one 'ticker' column), forecasts the next
S.STOCK_FORECAST_HORIZON trading days per ticker with Amazon's Chronos
time-series model, and writes back ONE combined table containing BOTH the
historical rows and the forecast rows for every ticker — distinguished by a
'record_type' column ('history' | 'forecast'). No per-ticker files, no
database: everything lives in one table, one write per run.

No registry, no pattern matching: it reads/writes through the DataFacility
directly, following the same standalone-class convention as pangolin's own
BackupRestore processor.
"""

from __future__ import annotations

from datetime import timedelta

import numpy as np
import polars as pl
import torch
from chronos import ChronosPipeline

from custom.processors._schema import STOCK_PRICE_COLUMNS
from pangolin.config.run_context import RunContext
from pangolin.config.settings import get_settings
from pangolin.engine.DataFacility import get_project_data
from pangolin.engine.common.exceptions import NoInputFilesError
from pangolin.engine.common.logger import ProcessorLogger
from pangolin.utils.fs_wrapper import FSWrapper

# Output column order, shared by history rows and forecast rows — that's
# what lets them live in the same table. Plain "vertical" concat matches by
# position, not by name, so both sides must select() into this exact order
# before being combined. Extends the shared base schema with the two
# columns unique to this step.
_OUTPUT_COLUMNS = STOCK_PRICE_COLUMNS + ["record_type", "forecast_low", "forecast_high"]


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
    """Reads the single consolidated history table at input_node, forecasts
    S.STOCK_FORECAST_HORIZON days ahead per ticker with Chronos, and writes
    one combined table (history + forecast rows, every ticker) to output_node.
    """

    def __init__(self, CTX: RunContext, name: str, input_node: str, output_node: str):
        if not name:
            raise ValueError("Step name must be provided")
        if not input_node:
            raise ValueError("input_node must be provided")
        if not output_node:
            raise ValueError("output_node must be provided")

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
        self.input_node = self.D.get_node(input_node)
        self.output_node = self.D.get_node(output_node)

        # Loaded lazily on first use, not in __init__: instantiating this
        # processor (e.g. for testing) shouldn't require downloading a model.
        self._pipeline = None

    def _load_pipeline(self) -> ChronosPipeline:
        if self._pipeline is None:
            self.log.info(f"Loading Chronos model '{self.S.CHRONOS_MODEL}' (first call only, may take a while)")
            self._pipeline = ChronosPipeline.from_pretrained(
                self.S.CHRONOS_MODEL,
                device_map="cpu",
                torch_dtype=torch.float32,
            )
        return self._pipeline

    def _forecast_ticker(self, ticker_history: pl.DataFrame) -> pl.DataFrame:
        """Run Chronos on one ticker's 'close' series and return
        prediction_length rows in _OUTPUT_COLUMNS order (record_type='forecast').
        """
        horizon = self.S.STOCK_FORECAST_HORIZON
        ticker = ticker_history["ticker"][0]

        context = torch.tensor(ticker_history["close"].to_numpy(), dtype=torch.float32)
        forecast = self._load_pipeline().predict(
            context, prediction_length=horizon, num_samples=self.S.STOCK_FORECAST_NUM_SAMPLES
        )
        samples = forecast[0].numpy()  # shape: (num_samples, horizon)
        low, median, high = np.quantile(samples, [0.1, 0.5, 0.9], axis=0)

        last_date = ticker_history["date"].max()
        forecast_dates = _next_trading_days(last_date, horizon)

        # Only close/forecast_low/forecast_high/record_type are real values
        # here — polars infers their dtype fine from the data itself.
        # open/high/low/volume aren't forecast (Chronos predicts 'close'
        # only), so they're added as explicitly-typed nulls: a bare `None`
        # list has no dtype for polars to infer, which would make the
        # concat below (history rows are real Float64/Int64) fail.
        return pl.DataFrame({
            "ticker": [ticker] * horizon,
            "date": forecast_dates,
            "close": median.tolist(),
            "record_type": ["forecast"] * horizon,
            "forecast_low": low.tolist(),
            "forecast_high": high.tolist(),
        }).with_columns(
            pl.lit(None, dtype=pl.Float64).alias("open"),
            pl.lit(None, dtype=pl.Float64).alias("high"),
            pl.lit(None, dtype=pl.Float64).alias("low"),
            pl.lit(None, dtype=pl.Int64).alias("volume"),
        ).select(_OUTPUT_COLUMNS)

    def execute(self):
        if not self.input_node.exists():
            raise NoInputFilesError(self.name, str(self.input_node.path))
        history = self.input_node.read()
        if history.schema.get("date") != pl.Date:
            history = history.with_columns(pl.col("date").str.to_date("%Y-%m-%d"))

        tickers = history["ticker"].unique().sort().to_list()
        if not tickers:
            raise NoInputFilesError(self.name, str(self.input_node.path))

        per_ticker_tables = []
        for ticker in tickers:
            ticker_history = history.filter(pl.col("ticker") == ticker).sort("date")
            self.log.info(
                f"Forecasting '{ticker}': {self.S.STOCK_FORECAST_HORIZON} day(s) ahead "
                f"from {len(ticker_history)} historical row(s)"
            )

            history_rows = ticker_history.with_columns(
                pl.lit("history").alias("record_type"),
                pl.lit(None, dtype=pl.Float64).alias("forecast_low"),
                pl.lit(None, dtype=pl.Float64).alias("forecast_high"),
            ).select(_OUTPUT_COLUMNS)
            forecast_rows = self._forecast_ticker(ticker_history)
            per_ticker_tables.append(pl.concat([history_rows, forecast_rows], how="vertical"))

        combined = pl.concat(per_ticker_tables, how="vertical").sort(["ticker", "date"])
        self.output_node.write(combined)

        self.log.info(
            f"Wrote {len(combined)} row(s) across {len(tickers)} ticker(s) to '{self.output_node.path}'"
        )
        return tickers
