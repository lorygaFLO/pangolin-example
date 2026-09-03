"""Shared column schema for the stock-forecast example's custom processors —
single source of truth for the base OHLCV column list, instead of the same
literal column names repeated in yahoo_downloader.py and chronos_forecaster.py.

Not a processor itself (no execute()): the leading underscore keeps it out
of the way if this package is ever scanned for processor classes.

Must stay in sync with config/registries/stock_prices_validator.yaml's
required_columns — that one has to stay a plain YAML list (it's the
pattern-match step's declarative contract, not code), so it can't import
this constant, but the two should list the same columns.
"""

# Written by yahoo_downloader.py; read unchanged all the way through
# history_consolidator.py. chronos_forecaster.py extends this with its own
# three extra columns (record_type, forecast_low, forecast_high).
STOCK_PRICE_COLUMNS = ["ticker", "date", "open", "high", "low", "close", "volume"]
