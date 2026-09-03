"""Ad-hoc (non-pattern-match) processor: downloads historical OHLCV prices
for every ticker in S.STOCK_TICKERS from Yahoo Finance and writes one CSV
per ticker, named '<TICKER>_STOCK_PRICES.csv'.

This is the entry point of the stock forecast example pipeline
(pipelines/stock_forecast_pipeline.py). It deliberately does NOT subclass
BaseProcessor: there is no registry, no pattern matching to do here — it
*produces* the files that the next step matches by pattern (registry
config/registries/stock_prices_validator.yaml, pattern "*_STOCK_PRICES.csv").
Structurally it follows the same standalone-class convention as pangolin's
own BackupRestore processor (see pangolin.engine.processors.BackupRestore).
"""

from __future__ import annotations

import polars as pl
import yfinance as yf

from pangolin.config.run_context import RunContext
from pangolin.config.settings import get_settings
from pangolin.engine.DataFacility import get_project_data
from pangolin.engine.common.exceptions import PipelineError
from pangolin.engine.common.logger import ProcessorLogger
from pangolin.utils.fs_wrapper import FSWrapper

# Columns written to '<TICKER>_STOCK_PRICES.csv', in this exact order.
# The rest of the pipeline (validator registry, history consolidator,
# Chronos forecaster) all assume this schema.
_OUTPUT_COLUMNS = ["ticker", "date", "open", "high", "low", "close", "volume"]


class YahooDownloader:
    """Downloads OHLCV history for every S.STOCK_TICKERS entry and writes
    one '<TICKER>_STOCK_PRICES.csv' file per ticker into output_folder.
    """

    def __init__(self, CTX: RunContext, name: str, output_folder: str):
        if not name:
            raise ValueError("Step name must be provided")
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
        self.output_node = self._get_node_by_path(output_folder)
        self.fs.makedirs(str(self.output_node.path), exist_ok=True)

    def _get_node_by_path(self, path_str: str):
        """Navigate to a node in DataFacility using dot notation."""
        node = self.D
        for part in path_str.split("."):
            node = getattr(node, part)
        return node

    def _download_one(self, ticker: str) -> pl.DataFrame | None:
        """Download and normalize one ticker's history. Returns None if
        Yahoo Finance returned nothing for it (e.g. an unknown ticker).
        """
        raw = yf.download(
            ticker,
            period=self.S.STOCK_HISTORY_PERIOD,
            auto_adjust=True,
            progress=False,
        )
        if raw is None or raw.empty:
            return None

        # Recent yfinance versions can return a MultiIndex on columns even
        # for a single ticker (e.g. ('Close', 'AAPL')) — flatten to plain
        # names before handing off to polars, so no pandas quirk leaks
        # further down the pipeline.
        raw = raw.reset_index()
        raw.columns = [c[0] if isinstance(c, tuple) else c for c in raw.columns]

        return (
            pl.from_pandas(raw)
            .select(
                pl.col("Date").cast(pl.Date).alias("date"),
                pl.col("Open").cast(pl.Float64).alias("open"),
                pl.col("High").cast(pl.Float64).alias("high"),
                pl.col("Low").cast(pl.Float64).alias("low"),
                pl.col("Close").cast(pl.Float64).alias("close"),
                pl.col("Volume").cast(pl.Int64).alias("volume"),
            )
            .with_columns(pl.lit(ticker).alias("ticker"))
            .select(_OUTPUT_COLUMNS)
            .sort("date")
        )

    def execute(self):
        tickers = self.S.STOCK_TICKERS
        if not tickers:
            raise PipelineError(f"[{self.name}] S.STOCK_TICKERS is empty — nothing to download.")

        written = []
        for ticker in tickers:
            self.log.info(f"Downloading '{ticker}' from Yahoo Finance (period={self.S.STOCK_HISTORY_PERIOD})")
            df = self._download_one(ticker)
            if df is None:
                self.log.warning(f"No data returned for '{ticker}' — skipping")
                continue

            filename = f"{ticker}_STOCK_PRICES.csv"
            output_path = self.fs.join(str(self.output_node.path), filename)
            with self.fs.open(output_path, "wb") as f:
                df.write_csv(f, separator=self.S.CSV_DELIMITER)

            self.log.info(f"Wrote {len(df)} row(s) to '{output_path}'")
            written.append(filename)

        if not written:
            raise PipelineError(f"[{self.name}] Yahoo Finance returned no data for any of {tickers}.")

        return written
