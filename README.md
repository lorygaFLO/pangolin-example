# Stock Forecast Example (pangolin)

Scaffolded by `pangolin init`, then built out into a single worked example:
Yahoo Finance download -> backup -> validate -> transform -> historicize ->
forecast with Amazon's Chronos model. This file is edited freely — it's not
regenerated after the initial `pangolin init`.

## Layout of a pangolin project

1. **`.env`** — fill in `BASEPATH`/`DATAPATH` for your machine (or your cloud
   container/prefix, if `FS_PROTOCOL` isn't `file`) and the IO options. See
   the settings reference below for every field pangolin reads.
2. **`config/data_structure.yaml`** — describes the project's folder layout.
   Every top-level node with a `_settings_key` becomes a setting you can
   override from `.env` (see "Folder settings" below). A node with
   `_pattern_matching: true` + `_registry: <path>` links it to one of the
   registry files below.
3. **`config/registries/*.yaml`** — one file per pattern-match step: which
   validators/transformers run, matched by file-name glob pattern.
4. **`custom/`** — validators/transformers/processors/settings/run-context
   specific to this project. Built-in ones ship with pangolin itself
   (`pangolin.utils.validators` / `pangolin.utils.transformers`) and don't
   need to be duplicated here.
5. **`pipelines/`** — one file per pipeline; each must expose a module-level
   `PIPELINE = <flow>`. See `pipelines/stock_forecast_pipeline.py`.

## Running it

1. `pip install -r requirements.txt` (also installs `pangolin` itself from
   `../pangolin`, in editable mode — adjust the path in `requirements.txt`
   if your checkout is laid out differently).

```bash
pangolin list                      # discovered pipelines + their debuggable steps
pangolin run                       # run the default pipeline
pangolin step <pipeline> <step>    # run one step in isolation (needs DEBUG=True in .env)
pangolin restore <run_id>          # restore raw downloads from a previous backup
pangolin deploy                    # serve every pipeline as a Prefect deployment
pangolin bootstrap                 # apply docker/prefect_manifest.yaml to a Prefect server
```

2. `pangolin run stock_forecast_pipeline`
3. Open `notebooks/stock_forecast_review.ipynb` to review the result: a
   single table for every ticker at
   `data/stocks/4_forecast/stock_prices_forecast.csv` (see the `ticker`
   column), holding **both** the historical prices and the forecast,
   distinguished by the `record_type` column (`"history"` / `"forecast"`) —
   no database, no separate forecast file.

## The pipeline, step by step

`pipelines/stock_forecast_pipeline.py` demonstrates every kind of processor
and extension point pangolin supports:

| Step | Processor | Style |
| --- | --- | --- |
| 1. Download | `custom/processors/yahoo_downloader.py` | ad-hoc (no registry) |
| 2. Backup | pangolin's built-in `BackupRestore` | ad-hoc (no registry) |
| 3. Validate | pangolin's built-in `Validator` | registry pattern-match, `config/registries/stock_prices_validator.yaml` (`"*_STOCK_PRICES.csv"`), incl. a custom validator (`custom/validators.py: max_daily_price_change`) |
| 4. Transform | pangolin's built-in `DataTransformer` | registry pattern-match, `config/registries/stock_transform.yaml`, with a custom transformer (`custom/transformers.py: add_daily_return_pct`) |
| 5. Historicize | `custom/processors/history_consolidator.py` | ad-hoc (no registry) |
| 6. Forecast | `custom/processors/chronos_forecaster.py` | ad-hoc (no registry) |

Every run also backs up its raw downloads (`data/stocks/backup/<run_id>/`),
using pangolin's built-in `BackupRestore`. To bring a previous run's raw
data back (e.g. to reprocess it without re-downloading):

```bash
pangolin restore <run_id> --pipeline stock_forecast_pipeline
```

This restores into a *new* run's `stocks.0_raw`. To then reprocess that
exact data with `pangolin step stock_forecast_pipeline <step>`, set
`DEBUG=True` in `.env` first — that pins `RUN_ID` to `DEBUG_RUN_ID`, so the
restore and the following step invocations agree on which run folder to use.

`stock_forecast_pipeline()` also takes two optional parameters — once served
with `pangolin deploy`, Prefect's own UI builds a run form for these from
the flow's type hints directly, no extra deployment needed:
- `tickers`: override `S.STOCK_TICKERS` for this run only (e.g. `["NVDA"]`).
- `restore_from_run_id`: skip the Yahoo Finance download and restore
  `stocks.0_raw` from that backup run instead — the UI equivalent of
  `pangolin restore`.

## `custom/run_context.py`: extending RunContext

`RunContext` (`RUN_ID`/`GIT_BRANCH`/`GIT_SHA`) is pangolin's fixed, built-in
per-run metadata. This project extends it with `TRIGGERED_BY` — set a
`TRIGGERED_BY` env var before running (e.g. `TRIGGERED_BY=schedule` in a
cron wrapper) to have it show up in `CTX.summary()`. The pipeline calls
`get_run_context()` rather than instantiating `RunContext()` directly —
that's what makes this subclass actually get picked up.

## Settings reference (`.env`)

Every field below can be set in `.env` or as an environment variable of the
same name. This table is generated from the pangolin version you have
installed — if it looks out of date, reinstall pangolin.

| Setting | Default | Description |
| --- | --- | --- |
| `PROJECT_NAME` | `'pangolin'` | Project identity. Used as the Prefect UI subdomain in docker-local mode, to isolate this project's local Prefect state directory (see PREFECT_HOME below) from other pangolin projects on the same machine, and for run tagging. |
| `BACKEND_ENGINE` | `'polars'` | Dataframe engine. Only 'polars' is supported in this release. |
| `DUCKDB_CHUNK_SIZE` | `100000` | Reserved for a future DuckDB backend; unused today. |
| `BASEPATH` | `'.'` | Project root. Resolved to an absolute path at startup; also the base DATAPATH is resolved against when DATAPATH is relative. |
| `DATAPATH` | *(none)* | Where data/ lives. Defaults to '<BASEPATH>/data' when FS_PROTOCOL='file'. REQUIRED (container/bucket name or prefix) for cloud protocols. |
| `DISABLE_REPORTS` | `False` | Skip HTML validation report generation. |
| `CSV_DELIMITER` | `';'` | Delimiter used when reading/writing CSV files. |
| `OUTPUT_FORMAT` | `'parquet'` | Staging/output file format: 'csv' or 'parquet'. |
| `DEBUG` | `False` | When True, pins RUN_ID to DEBUG_RUN_ID so `pangolin step` can find staging left by a previous run. |
| `DEBUG_RUN_ID` | `'debug_run'` | RUN_ID used when DEBUG=True. |
| `FS_PROTOCOL` | `'file'` | fsspec protocol: 'file' for local disk, or 'az' / 's3' / 'gcs' for cloud storage. |
| `FS_OPTIONS` | `{}` | Extra fsspec storage options (credentials, endpoint, etc.) for a non-local FS_PROTOCOL, as a JSON object. |

Plus this project's own, added in `custom/settings.py` (see below):
`STOCK_TICKERS`, `STOCK_HISTORY_PERIOD`, `STOCK_FORECAST_HORIZON`,
`STOCK_FORECAST_NUM_SAMPLES`, `CHRONOS_MODEL`.

Not in the table above because it's read directly by pangolin's CLI, not by
`SETTINGS`: **`PREFECT_HOME`** — this project's local Prefect state
directory, isolating it from other pangolin projects on this machine.
Defaults to `.prefect/<PROJECT_NAME>` automatically (nothing to set in
`.env`); `pangolin run` / `deploy` / `bootstrap` derive it from
`PROJECT_NAME` above. If you also run the bare `prefect server start` (e.g.
to use the dashboard), export the same value in that shell first — see
"Running via the Prefect UI" in the pangolin library's Getting Started doc.

## Folder settings (from `config/data_structure.yaml`)

These are **not fixed** — every project declares its own, by giving a
top-level node in `config/data_structure.yaml` a `_settings_key`. This
project currently declares:

| Env var (settings key) | Folder name |
| --- | --- |
| `REPORTS_FOLDER_NAME` | `reports` |

Everything under `stocks:` (`0_raw`, `backup`, `1_staging`, `2_transform`,
`3_history`, `4_forecast`) uses a fixed `_path` instead of a
`_settings_key` — this project doesn't need those to be independently
overridable per environment, so they're not settings.

## Adding your own settings

`S.TRAINING_EPOCHS`, `S.MY_API_KEY`, or anything else your own code needs
that isn't a folder and isn't one of pangolin's built-in fields above: add
it to the `SETTINGS` class in **`custom/settings.py`**:

```python
class SETTINGS(_BaseSettings):
    TRAINING_EPOCHS: int = 10
```

`get_settings()` picks it up automatically — `S.TRAINING_EPOCHS` then works
anywhere in the project, read from `.env`/the environment exactly like the
built-in fields, with the same Pydantic validation. No changes to the
pangolin library needed. This project already uses this for every
`STOCK_*` / `CHRONOS_MODEL` setting above.

## Running it in Docker

Not scaffolded for this project. Re-run `pangolin init --dockerization` (or `-d`) in this same folder to add it — existing files are left untouched unless you also pass `--force`.
