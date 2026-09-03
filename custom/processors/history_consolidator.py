"""Ad-hoc (non-pattern-match) processor: merges each ticker's freshly
validated OHLCV file into a single, persistent per-ticker history file —
deduplicated by date, so running the pipeline again only adds genuinely new
trading days instead of piling up duplicates. This is the "storicization"
step of the stock forecast example pipeline.

No registry, no pattern matching: it reads every file the previous step
(the built-in Validator) produced and writes straight through the
DataFacility / FSWrapper, following the same standalone-class convention as
pangolin's own BackupRestore processor.
"""

from __future__ import annotations

import polars as pl

from pangolin.config.run_context import RunContext
from pangolin.config.settings import get_settings
from pangolin.engine.DataFacility import get_project_data
from pangolin.engine.common.exceptions import NoInputFilesError
from pangolin.engine.common.logger import ProcessorLogger
from pangolin.utils.fs_wrapper import FSWrapper


class HistoryConsolidator:
    """Reads every validated '<TICKER>_STOCK_PRICES.*' file in input_folder
    and merges it into '<TICKER>_STOCK_PRICES.csv' under output_folder,
    deduplicated by (ticker, date).
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

    def _get_node_by_path(self, path_str: str):
        """Navigate to a node in DataFacility using dot notation."""
        node = self.D
        for part in path_str.split("."):
            node = getattr(node, part)
        return node

    def _read_any(self, path: str) -> pl.DataFrame:
        """Read a '*_STOCK_PRICES.*' file regardless of format (the
        Validator step writes S.OUTPUT_FORMAT, csv or parquet) and make sure
        'date' always comes back as a proper pl.Date column.
        """
        suffix = self.fs.suffix(path).lower()
        with self.fs.open(path, "rb") as f:
            if suffix == ".csv":
                df = pl.read_csv(f, separator=self.S.CSV_DELIMITER)
            elif suffix in (".parquet", ".pq"):
                df = pl.read_parquet(f)
            else:
                raise ValueError(f"Unsupported file format for '{path}'")

        if df.schema.get("date") != pl.Date:
            df = df.with_columns(pl.col("date").str.to_date("%Y-%m-%d"))
        return df

    def execute(self):
        pattern = self.fs.join(str(self.input_node.path), "*_STOCK_PRICES.*")
        files = [f for f in self.fs.glob(pattern) if self.fs.isfile(f)]
        if not files:
            raise NoInputFilesError(self.name, str(self.input_node.path))

        updated = []
        for full_path in files:
            new_data = self._read_any(full_path)
            ticker = new_data["ticker"][0]
            history_filename = f"{ticker}_STOCK_PRICES.csv"
            history_path = self.fs.join(str(self.output_node.path), history_filename)

            if self.fs.exists(history_path):
                with self.fs.open(history_path, "rb") as f:
                    existing = pl.read_csv(f, separator=self.S.CSV_DELIMITER)
                if existing.schema.get("date") != pl.Date:
                    existing = existing.with_columns(pl.col("date").str.to_date("%Y-%m-%d"))
                combined = pl.concat([existing, new_data], how="vertical")
            else:
                combined = new_data

            # keep="last" -> when a date already exists, the freshly
            # downloaded row wins (e.g. a same-day close that gets revised).
            combined = combined.unique(subset=["ticker", "date"], keep="last").sort("date")

            with self.fs.open(history_path, "wb") as f:
                combined.write_csv(f, separator=self.S.CSV_DELIMITER)

            self.log.info(
                f"History for '{ticker}': {len(combined)} row(s) total "
                f"({len(new_data)} from this run) -> '{history_path}'"
            )
            updated.append(ticker)

        return updated
