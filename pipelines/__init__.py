"""
Pipeline registry.

Every non-private .py file in this package is a self-contained pipeline: it
may define as many internal @flow subflow steps as it needs, but must expose
a module-level `PIPELINE = <flow>` marker pointing at the one flow to run.

To add a new pipeline: drop a new file here (e.g. pipelines/my_pipeline.py)
defining its flow(s) and setting `PIPELINE = <the flow to run>`.

Modules prefixed with `_` are skipped (private helpers, not pipelines).
"""

from __future__ import annotations

import importlib
import pkgutil
from typing import Dict

from prefect.flows import Flow

PIPELINES: Dict[str, Flow] = {}

for _, module_name, is_pkg in pkgutil.iter_modules(__path__):
    if is_pkg or module_name.startswith("_"):
        continue
    module = importlib.import_module(f"{__name__}.{module_name}")
    pipeline_flow = getattr(module, "PIPELINE", None)
    if not isinstance(pipeline_flow, Flow):
        raise RuntimeError(
            f"Pipeline module 'pipelines.{module_name}' must set a module-level "
            f"'PIPELINE = <flow>' pointing at its runnable @flow."
        )
    PIPELINES[module_name] = pipeline_flow

__all__ = ["PIPELINES"]
