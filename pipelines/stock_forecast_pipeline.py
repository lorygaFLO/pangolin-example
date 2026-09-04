"""
Stock forecast pipeline: Yahoo Finance download -> backup -> validate ->
transform -> historicize -> Chronos forecast.

A worked example of everything a pangolin project is made of:
- Ad-hoc, non-pattern-match processors (custom/processors/yahoo_downloader.py,
  history_consolidator.py, chronos_forecaster.py) plus pangolin's own
  built-in BackupRestore. They talk to the DataFacility directly — no
  registry, no '_pattern_matching' involved.
- Registry-driven, pattern-match processors: pangolin's built-in Validator
  and DataTransformer, matching every '<TICKER>_STOCK_PRICES.csv' file via
  config/registries/stock_prices_validator.yaml and
  config/registries/stock_transform.yaml (pattern "*_STOCK_PRICES.csv").
- A custom validator (custom/validators.py: max_daily_price_change) and a
  custom transformer (custom/transformers.py: add_daily_return_pct),
  registered via @register_validator / @register_transformer.
- A custom RunContext field (custom/run_context.py: TRIGGERED_BY) — hence
  get_run_context() below instead of instantiating RunContext() directly,
  so that subclass is actually picked up.

Data structure nodes are numbered (0_raw, 1_staging, 2_transform,
3_history, 4_forecast) so they sort in pipeline order on disk. 'backup'
isn't numbered — it's a side artifact of the download step, not a pipeline
stage of its own. 3_history and 4_forecast are single tables — every
ticker together, distinguished by the 'ticker' column — not one file per
ticker.

Try it: adjust STOCK_TICKERS in .env if you want (defaults to
["AAPL","MSFT","AMZN"]) and run `pangolin run stock_forecast_pipeline`.
Then open notebooks/stock_forecast_review.ipynb to review
data/stocks/4_forecast/stock_prices_forecast.csv (history + forecast, every
ticker, flagged by the 'record_type' column).

Restoring a previous run's raw downloads:
    pangolin restore <run_id> --pipeline stock_forecast_pipeline
This copies data/stocks/backup/<run_id>/*.csv back into stocks.0_raw for a
*new* run. To then reprocess that exact restored data with
`pangolin step stock_forecast_pipeline <step>`, set DEBUG=True in .env
first — that pins RUN_ID to DEBUG_RUN_ID, so restore and the following step
invocations agree on which run folder to use.

Both of the above are also available as ordinary flow **parameters** on
stock_forecast_pipeline() itself (`tickers`, `restore_from_run_id`) — once
this pipeline is served with `pangolin deploy`, Prefect's own UI builds a
parameter form for them straight from these type hints on the "Run"
button, no extra deployment or library change needed:
- `tickers`: override S.STOCK_TICKERS for this run only, e.g. ["NVDA"].
- `restore_from_run_id`: skip the Yahoo Finance download and restore
  stocks.0_raw from that backup run_id instead (same thing `pangolin
  restore` does, just triggered from the UI instead of the CLI).
"""

from typing import List, Optional

from prefect import flow, get_run_logger

# Importing these modules registers the project's custom validator and
# transformer into the dicts pangolin's Validator/DataTransformer look
# functions up in, by name, from the registries below.
import custom.transformers  # noqa: F401
import custom.validators  # noqa: F401

from custom.processors.chronos_forecaster import ChronosForecaster
from custom.processors.history_consolidator import HistoryConsolidator
from custom.processors.yahoo_downloader import YahooDownloader
from pangolin.config.run_context import RunContext, get_run_context
from pangolin.config.settings import get_settings
from pangolin.engine.processors.BackupRestore import BackupRestore
from pangolin.engine.processors.DataTransformer import DataTransformer
from pangolin.engine.processors.DataValidator import Validator


@flow(name="1 - Download (Yahoo Finance)")
def download_flow(CTX: RunContext, tickers: Optional[List[str]] = None):
    """Ad-hoc processor: one '<TICKER>_STOCK_PRICES.csv' per ticker.

    `tickers` overrides S.STOCK_TICKERS for this run only, when given.
    """
    downloader = YahooDownloader(CTX, name="yahoo_downloader", output_folder="stocks.0_raw")
    downloader.execute(tickers=tickers)


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


@flow(name="4 - Transform (registry pattern-match)")
def transform_flow(CTX: RunContext):
    """Built-in DataTransformer, matched via config/registries/stock_transform.yaml
    — adds a 'daily_return_pct' column (custom/transformers.py)."""
    S = get_settings()
    transformer = DataTransformer(
        CTX,
        name="stock_transform",
        report_folder=S.REPORTS_FOLDER_NAME,
        input_folder="stocks.1_staging.stock_prices_validator",
        output_folder="stocks.2_transform.stock_transform",
    )
    transformer.execute()


@flow(name="5 - Historicize")
def history_flow(CTX: RunContext):
    """Ad-hoc processor: merge this run's transformed prices into the persistent history table."""
    consolidator = HistoryConsolidator(
        CTX,
        name="history_consolidator",
        input_folder="stocks.2_transform.stock_transform",
        output_node="stocks.3_history",
    )
    consolidator.execute()


@flow(name="6 - Forecast (Chronos)")
def forecast_flow(CTX: RunContext):
    """Ad-hoc processor: writes history + forecast together, every ticker, flagged by 'record_type'."""
    forecaster = ChronosForecaster(
        CTX,
        name="chronos_forecaster",
        input_node="stocks.3_history",
        output_node="stocks.4_forecast",
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
    description="Download, backup, validate, transform, historicize and forecast stock prices",
)
def stock_forecast_pipeline(
    tickers: Optional[List[str]] = None,
    restore_from_run_id: Optional[str] = None,
):
    """
    tickers: override S.STOCK_TICKERS for this run only, e.g. ["NVDA"].
        Leave empty to use whatever's configured in .env.
    restore_from_run_id: instead of downloading from Yahoo Finance, restore
        stocks.0_raw from this backup run_id (see stocks.backup/<run_id>/).
        Leave empty for a normal fresh download.
    """
    logger = get_run_logger()
    # get_run_context() (not RunContext() directly) so that the project's
    # custom/run_context.py subclass (TRIGGERED_BY) is actually used.
    CTX = get_run_context()
    logger.info(f"Stock forecast pipeline started - {CTX.summary()}")

    if restore_from_run_id:
        logger.info(f"restore_from_run_id given: restoring stocks.0_raw from backup '{restore_from_run_id}' instead of downloading")
        s0 = restore_flow(CTX, restore_from_run_id, return_state=True)
    else:
        s0 = download_flow(CTX, tickers, return_state=True)

    s1 = backup_flow(CTX, return_state=True, wait_for=[s0])
    s2 = validate_flow(CTX, return_state=True, wait_for=[s1])
    s3 = transform_flow(CTX, return_state=True, wait_for=[s2])
    s4 = history_flow(CTX, return_state=True, wait_for=[s3])
    forecast_flow(CTX, return_state=True, wait_for=[s4])

    logger.info("Stock forecast pipeline ended successfully")


# Marks the flow to expose for this module (required by the pipeline registry).
PIPELINE = stock_forecast_pipeline
