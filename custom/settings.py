"""Project-specific settings.

pangolin's SETTINGS (BASEPATH, CSV_DELIMITER, ...) is fixed — it lives in
the library and is the same for every project. This file is where YOUR
project adds its own fields, without touching the library. Anything you
declare on the SETTINGS class below becomes available as `S.<FIELD_NAME>`
everywhere `get_settings()` is called (processors, transformers, your own
pipeline code) — `pangolin.config.settings.get_settings()` auto-detects
this file and uses this class instead of the base one.

Values are read the same way as every other setting: from `.env`, or from
the environment. Example:

    # .env
    TRAINING_EPOCHS=20

    # anywhere in your code
    from pangolin.config.settings import get_settings
    S = get_settings()
    print(S.TRAINING_EPOCHS)  # 20

Delete this file if you don't need any project-specific settings — pangolin
falls back to the base SETTINGS automatically.
"""

from __future__ import annotations

from pangolin.config.settings import SETTINGS as _BaseSettings


class SETTINGS(_BaseSettings):
    # Add your fields here, same syntax as any pydantic-settings field:
    #
    #   TRAINING_EPOCHS: int = 10
    #   MY_API_KEY: str | None = None
    #   ENABLE_FEATURE_X: bool = False
    #
    # See pydantic-settings docs for supported types, or the "Adding Custom
    # Settings" page in the docs for validators / computed fields.

    # --- Stock forecast example pipeline (pipelines/stock_forecast_pipeline.py) ---
    STOCK_TICKERS: list[str] = ["AAPL", "MSFT", "AMZN"]  # Yahoo Finance tickers to download
    STOCK_HISTORY_PERIOD: str = "2y"  # yfinance 'period' string, e.g. "1y", "5y", "max"
    STOCK_FORECAST_HORIZON: int = 14  # trading days to forecast ahead with Chronos
    CHRONOS_MODEL: str = "amazon/chronos-t5-small"  # Hugging Face model id used by ChronosPipeline
    # Sample paths Chronos draws per forecast (default is 20 for chronos-t5-*
    # models). forecast_low/forecast_high (10th/90th percentile) are computed
    # from these — 20 makes that estimate noisy, so the band can jitter
    # wider/narrower day to day instead of widening smoothly. More samples
    # = smoother, more stable band, at the cost of a bit more CPU time.
    STOCK_FORECAST_NUM_SAMPLES: int = 100
