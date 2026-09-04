"""Project-specific RunContext extension.

pangolin's base RunContext (RUN_ID, GIT_BRANCH, GIT_SHA) is fixed — it
lives in the library and is the same for every project. This is where YOUR
project adds its own per-run fields, without touching the library.
`get_run_context()` auto-detects this file and returns an instance of this
subclass everywhere a RunContext is used (pipelines, processors), instead
of the base one — see `pipelines/stock_forecast_pipeline.py`, which calls
`get_run_context()` rather than instantiating `RunContext()` directly, for
exactly this reason.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from pangolin.config.run_context import RunContext as _BaseRunContext


def _default_triggered_by() -> str:
    return os.getenv("TRIGGERED_BY", "manual")


@dataclass
class RunContext(_BaseRunContext):
    # Who/what started this run. Set a TRIGGERED_BY env var before running
    # to have it show up here (e.g. TRIGGERED_BY=schedule in a cron
    # wrapper) — defaults to "manual" otherwise.
    TRIGGERED_BY: str = field(default_factory=_default_triggered_by)

    def summary(self) -> str:
        return f"{super().summary()} | triggered_by: {self.TRIGGERED_BY}"
