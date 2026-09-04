"""Custom validators for the stock forecast example.

Every validator must follow the pangolin convention:

    def my_validator(df, messages, params=None) -> bool

Decorate it with @register_validator to make it available to the Validator
processor under its function name (referenced in
config/registries/stock_prices_validator.yaml). Built-in validators ship
with pangolin (pangolin/utils/validators.py).
"""

from pangolin.utils.validators import register_validator


@register_validator
def max_daily_price_change(df, messages, params=None):
    """Fail when any single-day % change in 'close' exceeds max_pct_change.

    A simple sanity check for obviously corrupted data (e.g. a
    decimal-point error, a stock split not accounted for) rather than
    genuine volatility — real single-day moves above ~50% are rare enough
    that this default is unlikely to flag a normal trading day.

    Assumes one ticker per file, which is true at this pipeline stage:
    the file being validated here is a single '<TICKER>_STOCK_PRICES.csv',
    before any multi-ticker table exists (see history_consolidator.py).
    """
    max_pct_change = (params or {}).get("max_pct_change", 0.5)
    if "close" not in df.columns:
        messages.append("Column 'close' not found in dataframe")
        return False

    changes = df.sort("date")["close"].pct_change().drop_nulls()
    bad = changes.filter(changes.abs() > max_pct_change)
    if len(bad) > 0:
        messages.append(
            f"{len(bad)} day(s) with a single-day 'close' change over "
            f"{max_pct_change:.0%} (largest: {bad.abs().max():.1%}) — "
            f"looks like corrupted data, not real volatility"
        )
        return False
    return True
