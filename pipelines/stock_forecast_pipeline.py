"""
Stock forecast example pipeline: Yahoo Finance download -> validate ->
historicize -> Chronos forecast.

Demonstrates the two processor styles side by side:
- Steps 1, 3, 4 are ad-hoc, non-pattern-match processors
  (custom/processors/yahoo_downloader.py, history_consolidator.py,
  chronos_forecaster.py). They talk to the DataFacility directly — no
  registry, no '_pattern_matching' involved.
- Step 2 reuses pangolin's built-in, registry-driven Validator, matching
  every '<TICKER>_STOCK_PRICES.csv' file via
  config/registries/stock_prices_validator.yaml (pattern "*_STOCK_PRICES.csv").

Data structure nodes are numbered (0_raw, 1_staging, 2_history, 3_forecast)
so they sort in pipeline order on disk, same convention as the sales example
pipeline's staging.0_validator/1_transform/2_audit/3_dispatcher steps. 2_history
and 3_forecast are single tables — every ticker together, distinguished by
the 'ticker' column — not one file per ticker.

Try it: adjust STOCK_TICKERS in .env if you want (defaults to
["AAPL","MSFT","AMZN"]) and run `pangolin run stock_forecast_pipeline`.
Then open notebooks/stock_forecast_review.ipynb to review
data/stocks/3_forecast/stock_prices_forecast.csv (history + forecast, every
ticker, flagged by the 'record_type' column).
"""

from prefect import flow, get_run_logger

from custom.processors.chronos_forecaster import ChronosForecaster
from custom.processors.history_consolidator import HistoryConsolidator
from custom.processors.yahoo_downloader import YahooDownloader
from pangolin.config.run_context import RunContext
from pangolin.config.settings import get_settings
from pangolin.engine.processors.DataValidator import Validator


@flow(name="1 - Download (Yahoo Finance)")
def download_flow(CTX: RunContext):
    """Ad-hoc processor: one '<TICKER>_STOCK_PRICES.csv' per S.STOCK_TICKERS entry."""
    downloader = YahooDownloader(CTX, name="yahoo_downloader", output_folder="stocks.0_raw")
    downloader.execute()


@flow(name="2 - Validate (registry pattern-match)")
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


@flow(name="3 - Historicize")
def history_flow(CTX: RunContext):
    """Ad-hoc processor: merge this run's validated prices into the persistent history table."""
    consolidator = HistoryConsolidator(
        CTX,
        name="history_consolidator",
        input_folder="stocks.1_staging.stock_prices_validator",
        output_node="stocks.2_history",
    )
    consolidator.execute()


@flow(name="4 - Forecast (Chronos)")
def forecast_flow(CTX: RunContext):
    """Ad-hoc processor: writes history + forecast together, every ticker, flagged by 'record_type'."""
    forecaster = ChronosForecaster(
        CTX,
        name="chronos_forecaster",
        input_node="stocks.2_history",
        output_node="stocks.3_forecast",
    )
    forecaster.execute()


@flow(name="Stock Forecast Pipeline", description="Download, validate, historicize and forecast stock prices")
def stock_forecast_pipeline():
    logger = get_run_logger()
    CTX = RunContext()
    logger.info(f"Stock forecast pipeline started - PANGOLIN_RUN_ID: {CTX.RUN_ID}")

    s0 = download_flow(CTX, return_state=True)
    s1 = validate_flow(CTX, return_state=True, wait_for=[s0])
    s2 = history_flow(CTX, return_state=True, wait_for=[s1])
    forecast_flow(CTX, return_state=True, wait_for=[s2])

    logger.info("Stock forecast pipeline ended successfully")


# Marks the flow to expose for this module (required by the pipeline registry).
PIPELINE = stock_forecast_pipeline
