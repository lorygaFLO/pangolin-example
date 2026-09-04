"""
Stock forecast example pipeline: Yahoo Finance download -> backup -> validate
-> historicize -> Chronos forecast.

Demonstrates the two processor styles side by side:
- Steps 1, 3, 4, 5 are ad-hoc, non-pattern-match processors
  (custom/processors/yahoo_downloader.py, history_consolidator.py,
  chronos_forecaster.py) plus pangolin's own built-in BackupRestore. They
  talk to the DataFacility directly — no registry, no '_pattern_matching'
  involved.
- Step 3 reuses pangolin's built-in, registry-driven Validator, matching
  every '<TICKER>_STOCK_PRICES.csv' file via
  config/registries/stock_prices_validator.yaml (pattern "*_STOCK_PRICES.csv").

Data structure nodes are numbered (0_raw, 1_staging, 2_history, 3_forecast)
so they sort in pipeline order on disk, same convention as the sales example
pipeline's staging.0_validator/1_transform/2_audit/3_dispatcher steps.
'backup' isn't numbered — it's a side artifact of the download step, not a
pipeline stage of its own. 2_history and 3_forecast are single tables —
every ticker together, distinguished by the 'ticker' column — not one file
per ticker.

Try it: adjust STOCK_TICKERS in .env if you want (defaults to
["AAPL","MSFT","AMZN"]) and run `pangolin run stock_forecast_pipeline`.
Then open notebooks/stock_forecast_review.ipynb to review
data/stocks/3_forecast/stock_prices_forecast.csv (history + forecast, every
ticker, flagged by the 'record_type' column).

Restoring a previous run's raw downloads:
    pangolin restore <run_id> --pipeline stock_forecast_pipeline
This copies data/stocks/backup/<run_id>/*.csv back into stocks.0_raw for a
*new* run. To then reprocess that exact restored data with
`pangolin step stock_forecast_pipeline <step>`, set DEBUG=True in .env
first — that pins RUN_ID to DEBUG_RUN_ID, so restore and the following step
invocations agree on which run folder to use.
"""

from prefect import flow, get_run_logger

from custom.processors.chronos_forecaster import ChronosForecaster
from custom.processors.history_consolidator import HistoryConsolidator
from custom.processors.yahoo_downloader import YahooDownloader
from pangolin.config.run_context import RunContext
from pangolin.config.settings import get_settings
from pangolin.engine.processors.BackupRestore import BackupRestore
from pangolin.engine.processors.DataValidator import Validator


@flow(name="1 - Download (Yahoo Finance)")
def download_flow(CTX: RunContext):
    """Ad-hoc processor: one '<TICKER>_STOCK_PRICES.csv' per S.STOCK_TICKERS entry."""
    downloader = YahooDownloader(CTX, name="yahoo_downloader", output_folder="stocks.0_raw")
    downloader.execute()


@flow(name="2 - Backup Raw Downloads")
def backup_flow(CTX: RunContext):
    """Built-in BackupRestore: copies this run's raw downloads to
    stocks.backup/<RUN_ID>/, so they can be brought back later with
    restore_flow / `pangolin restore <run_id> --pipeline stock_forecast_pipeline`.
    """
    backup = BackupRestore(CTX, name="backup", input_folder="stocks.0_raw", output_folder="stocks.backup")
    backup.execute()


@flow(name="3 - Validate (registry pattern-match)")
def validate_flow(CTX: RunContext):
    """Built-in Validator, matched via config/registries/stock_prices_validator.yaml."""
    S = get_settings()
    validator = Validator(
        CTX,
        name="stock_prices_validator",
        report_folder=S.REPORTS_FOLDER_NAME,
        input_folder="stocks.0_raw",
        output_folder="stocks.1_staging.stock_prices_validator",
    )
    validator.execute()


@flow(name="4 - Historicize")
def history_flow(CTX: RunContext):
    """Ad-hoc processor: merge this run's validated prices into the persistent history table."""
    consolidator = HistoryConsolidator(
        CTX,
        name="history_consolidator",
        input_folder="stocks.1_staging.stock_prices_validator",
        output_node="stocks.2_history",
    )
    consolidator.execute()


@flow(name="5 - Forecast (Chronos)")
def forecast_flow(CTX: RunContext):
    """Ad-hoc processor: writes history + forecast together, every ticker, flagged by 'record_type'."""
    forecaster = ChronosForecaster(
        CTX,
        name="chronos_forecaster",
        input_node="stocks.2_history",
        output_node="stocks.3_forecast",
    )
    forecaster.execute()


@flow(name="Restore Raw Downloads")
def restore_flow(CTX: RunContext, run_id: str):
    """Restores a previous run's raw downloads from stocks.backup/<run_id>/
    into stocks.0_raw for the CURRENT run. Auto-discovered by name
    ('restore_flow' is the exact name pangolin.cli.restore_cmd looks for) —
    this is what makes
        pangolin restore <run_id> --pipeline stock_forecast_pipeline
    work with no further wiring.
    """
    backup = BackupRestore(CTX, name="backup", input_folder="stocks.0_raw", output_folder="stocks.backup")
    backup.restore(run_id)


@flow(
    name="Stock Forecast Pipeline",
    description="Download, backup, validate, historicize and forecast stock prices",
)
def stock_forecast_pipeline():
    logger = get_run_logger()
    CTX = RunContext()
    logger.info(f"Stock forecast pipeline started - {CTX.summary()}")

    s0 = download_flow(CTX, return_state=True)
    s1 = backup_flow(CTX, return_state=True, wait_for=[s0])
    s2 = validate_flow(CTX, return_state=True, wait_for=[s1])
    s3 = history_flow(CTX, return_state=True, wait_for=[s2])
    forecast_flow(CTX, return_state=True, wait_for=[s3])

    logger.info("Stock forecast pipeline ended successfully")


# Marks the flow to expose for this module (required by the pipeline registry).
PIPELINE = stock_forecast_pipeline
