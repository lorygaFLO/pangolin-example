"""Example custom validators.

Every validator must follow the pangolin convention:

    def my_validator(df, messages, params=None) -> bool

Decorate it with @register_validator to make it available to the Validator
processor under its function name (referenced in config/registries/*.yaml).
Built-in validators ship with pangolin (pangolin/utils/validators.py).
"""

import polars as pl

from pangolin.utils.validators import register_validator


@register_validator
def quantity_not_negative(df, messages, params):
    """Fail when the configured quantity column contains negative values."""
    column = params["quantity_column"]
    if column not in df.columns:
        messages.append(f"Column '{column}' not found in dataframe")
        return False
    negative_rows = df.filter(pl.col(column) < 0)
    if len(negative_rows) > 0:
        messages.append(f"{len(negative_rows)} row(s) have a negative '{column}'")
        return False
    return True
