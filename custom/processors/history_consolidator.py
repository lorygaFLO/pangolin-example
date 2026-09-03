"""Ad-hoc (non-pattern-match) processor: merges every ticker's freshly
validated OHLCV file into a single persistent table (one 'ticker' column
distinguishes assets, no per-ticker files) — deduplicated by (ticker, date),
so running the pipeline again only adds genuinely new trading days instead
of piling up duplicates. This is the "storicization" step of the stock
forecast example pipeline.

No registry, no pattern matching on its own output: it reads every file the
previous step (the built-in Validator) produced and writes straight through
the DataFacility, following the same standalone-class convention as
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
    """Reads every validated '<TICKER>_STOCK_PRICES.*' file under
    input_folder and merges them all into the single table at output_node,
    deduplicated by (ticker, date).
    """

    def __init__(self, CTX: RunContext, name: str, input_folder: str, output_node: str):
        if not name:
            raise ValueError("Step name must be provided")
        if not input_folder:
            raise ValueError("input_folder must be provided")
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
        self.input_node = self.D.get_node(input_folder)  # folder: still one file per ticker
        self.output_node = self.D.get_node(output_node)  # single file: all tickers together

    def _read_any(self, path: str) -> pl.DataFrame:
        """Read one glob-matched staging file regardless of format (the
        Validator step writes S.OUTPUT_FORMAT, csv or parquet) and make sure
        'date' always comes back as a proper pl.Date column. These are plain
        matched paths, not declared DataFacility nodes, so they can't go
        through node.read() — this is the one place a manual reader is
        actually needed.
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
        files = [f for f in self.input_node.list("*_STOCK_PRICES.*") if self.fs.isfile(f)]
        if not files:
            raise NoInputFilesError(self.name, str(self.input_node.path))

        new_data = pl.concat([self._read_any(f) for f in files], how="vertical")

        if self.output_node.exists():
            existing = self.output_node.read()
            if existing.schema.get("date") != pl.Date:
                existing = existing.with_columns(pl.col("date").str.to_date("%Y-%m-%d"))
            combined = pl.concat([existing, new_data], how="vertical")
        else:
            combined = new_data

        # keep="last" -> when a (ticker, date) already exists, the freshly
        # downloaded row wins (e.g. a same-day close that gets revised).
        combined = combined.unique(subset=["ticker", "date"], keep="last").sort(["ticker", "date"])
        self.output_node.write(combined)

        tickers = combined["ticker"].unique().sort().to_list()
        self.log.info(
            f"History store now holds {len(combined)} row(s) across {len(tickers)} ticker(s) "
            f"({len(new_data)} new row(s) from this run) -> '{self.output_node.path}'"
        )
        return tickers
