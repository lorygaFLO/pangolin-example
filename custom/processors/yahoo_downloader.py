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

from custom.processors._schema import STOCK_PRICE_COLUMNS
from pangolin.config.run_context import RunContext
from pangolin.config.settings import get_settings
from pangolin.engine.DataCatalog import get_project_data
from pangolin.engine.common.exceptions import PipelineError
from pangolin.engine.common.logger import ProcessorLogger
from pangolin.utils.fs_wrapper import FSWrapper


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
        self.output_node = self.D.get_node(output_folder)
        self.fs.makedirs(str(self.output_node.path), exist_ok=True)

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
            .select(STOCK_PRICE_COLUMNS)
            .sort("date")
        )

    def execute(self, tickers=None):
        """
        tickers: optional override list, e.g. from a Prefect run parameter.
            Falls back to S.STOCK_TICKERS (.env) when not given.
        """
        tickers = tickers or self.S.STOCK_TICKERS
        if not tickers:
            raise PipelineError(f"[{self.name}] No tickers given (S.STOCK_TICKERS is also empty) — nothing to download.")

        self.log.info(
            f"Starting download for {len(tickers)} ticker(s): {tickers} "
            f"(period={self.S.STOCK_HISTORY_PERIOD}) -> '{self.output_node.path}'"
        )

        written = []
        skipped = []
        for i, ticker in enumerate(tickers, start=1):
            self.log.info(f"[{i}/{len(tickers)}] Downloading '{ticker}' from Yahoo Finance")
            df = self._download_one(ticker)
            if df is None:
                self.log.warning(f"[{i}/{len(tickers)}] No data returned for '{ticker}' — skipping")
                skipped.append(ticker)
                continue

            filename = f"{ticker}_STOCK_PRICES.csv"
            output_path = self.fs.join(str(self.output_node.path), filename)
            with self.fs.open(output_path, "wb") as f:
                df.write_csv(f, separator=self.S.CSV_DELIMITER)

            first_date, last_date = df["date"].min(), df["date"].max()
            self.log.info(
                f"[{i}/{len(tickers)}] Wrote {len(df)} row(s) for '{ticker}' "
                f"({first_date} -> {last_date}) to '{output_path}'"
            )
            written.append(filename)

        if not written:
            raise PipelineError(f"[{self.name}] Yahoo Finance returned no data for any of {tickers}.")

        self.log.info(
            f"Download complete: {len(written)}/{len(tickers)} ticker(s) written"
            + (f", {len(skipped)} skipped ({skipped})" if skipped else "")
        )
        return written
