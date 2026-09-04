"""Custom transformers for the stock forecast example.

Every transformer must follow the pangolin convention:

    def my_transformer(df, <params from registry>, messages=None) -> polars.DataFrame

Registry params are forwarded as keyword arguments. Decorate it with
@register_transformer to make it available to the DataTransformer processor
under its function name (referenced in
config/registries/stock_transform.yaml). Built-in transformers ship with
pangolin (pangolin/utils/transformers.py).
"""

import polars as pl

from pangolin.utils.transformers import register_transformer


@register_transformer
def add_daily_return_pct(df, price_column="close", output_column="daily_return_pct", messages=None):
    """Add the day-over-day % change of `price_column`, sorted by date.

    Assumes one ticker per file, which is true at this pipeline stage (see
    max_daily_price_change in custom/validators.py for the same assumption).
    """
    result = df.sort("date").with_columns(
        (pl.col(price_column).pct_change() * 100).round(4).alias(output_column)
    )
    if messages is not None:
        messages.append(f"Added column '{output_column}' (day-over-day % change of '{price_column}')")
    return result
