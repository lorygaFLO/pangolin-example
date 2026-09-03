"""Example custom transformers.

Every transformer must follow the pangolin convention:

    def my_transformer(df, <params from registry>, messages=None) -> polars.DataFrame

Registry params are forwarded as keyword arguments. Decorate it with
@register_transformer to make it available to the DataTransformer processor
under its function name (referenced in config/registries/*.yaml).
Built-in transformers ship with pangolin (pangolin/utils/transformers.py).
"""

from datetime import datetime, timezone

import polars as pl

from pangolin.utils.transformers import register_transformer


@register_transformer
def add_ingestion_timestamp(df, column_name="ingested_at", messages=None):
    """Add a column with the UTC timestamp of when the file was processed."""
    timestamp = datetime.now(timezone.utc).isoformat()
    result = df.with_columns(pl.lit(timestamp).alias(column_name))
    if messages is not None:
        messages.append(f"Added column '{column_name}' = {timestamp}")
    return result
