# Project

Scaffolded by `pangolin init`. This file is generated once and is never
touched again by `pangolin init --force` if it already exists — edit it
freely.

## Mandatory setup (in order)

1. **`.env`** — fill in `BASEPATH`/`DATAPATH` for your machine (or your cloud
   container/prefix, if `FS_PROTOCOL` isn't `file`) and the IO options. See
   the settings reference below for every field pangolin reads.
2. **`config/data_structure.yaml`** — describe your project's folder layout.
   Every top-level node with a `_settings_key` becomes a setting you can
   override from `.env` (see "Folder settings" below). A step node under
   `staging` with `_pattern_matching: true` + `_registry: <path>` links it to
   one of the registry files below.
3. **`config/registries/*.yaml`** — one file per pipeline step: which
   validators/transformers run, matched by file-name glob pattern.
4. **`custom/`** — validators/transformers/processors specific to this
   project. Built-in ones ship with pangolin itself
   (`pangolin.utils.validators` / `pangolin.utils.transformers`) and don't
   need to be duplicated here.
5. **`pipelines/`** — one file per pipeline; each must expose a module-level
   `PIPELINE = <flow>`. See `pipelines/example_pipeline.py`.
6. *(optional)* **`custom/settings.py`** — need a setting of your own (e.g.
   `S.TRAINING_EPOCHS`)? Add a field to the `SETTINGS` class there. It's
   auto-detected by `get_settings()` — no library changes needed. See
   "Adding your own settings" below.

## Running it

```bash
pangolin list                      # discovered pipelines + their debuggable steps
pangolin run                       # run the default pipeline
pangolin step <pipeline> <step>    # run one step in isolation (needs DEBUG=True in .env)
pangolin restore <run_id>          # restore input from a previous backup
pangolin deploy                    # serve every pipeline as a Prefect deployment
pangolin bootstrap                 # apply docker/prefect_manifest.yaml to a Prefect server
```

## Settings reference (`.env`)

Every field below can be set in `.env` or as an environment variable of the
same name. This table is generated from the pangolin version you have
installed — if it looks out of date, reinstall pangolin.

| Setting | Default | Description |
| --- | --- | --- |
| `PROJECT_NAME` | `'pangolin'` | Project identity. Used as the Prefect UI subdomain in docker-local mode and for run tagging. |
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

## Folder settings (from `config/data_structure.yaml`)

These are **not fixed** — every project declares its own, by giving a
top-level node in `config/data_structure.yaml` a `_settings_key`. This
project currently declares:

| Env var (settings key) | Folder name |
| --- | --- |
| `INPUT_FOLDER_NAME` | `input` |
| `STAGING_FOLDER_NAME` | `staging` |
| `DELIVERY_FOLDER_NAME` | `delivery` |
| `BACKUP_FOLDER_NAME` | `backup` |
| `REPORTS_FOLDER_NAME` | `reports` |

## Adding your own settings

`S.TRAINING_EPOCHS`, `S.MY_API_KEY`, or anything else your own code needs
that isn't a folder and isn't one of pangolin's built-in fields above: add
it to the `SETTINGS` class in **`custom/settings.py`** (already scaffolded,
starts empty):

```python
class SETTINGS(_BaseSettings):
    TRAINING_EPOCHS: int = 10
```

`get_settings()` picks it up automatically — `S.TRAINING_EPOCHS` then works
anywhere in the project, read from `.env`/the environment exactly like the
built-in fields, with the same Pydantic validation. No changes to the
pangolin library needed. Delete `custom/settings.py` if you never need this.

## Stock forecast example (Yahoo Finance + Chronos)

A second, self-contained pipeline lives alongside the sales example above:
`pipelines/stock_forecast_pipeline.py`. It downloads OHLCV history for the
tickers in `S.STOCK_TICKERS` (`.env`) from Yahoo Finance, validates it,
accumulates it into a persistent history, and forecasts
`S.STOCK_FORECAST_HORIZON` trading days ahead with Amazon's Chronos model.

1. `pip install -r requirements.txt` (also installs `pangolin` itself from
   `../pangolin`, in editable mode — adjust the path in `requirements.txt`
   if your checkout is laid out differently).
2. `pangolin run stock_forecast_pipeline`
3. Open `notebooks/stock_forecast_review.ipynb` to review the result: a
   single table for every ticker at
   `data/stocks/3_forecast/stock_prices_forecast.csv` (see the `ticker`
   column), holding **both** the historical prices and the forecast,
   distinguished by the `record_type` column (`"history"` / `"forecast"`) —
   no database, no separate forecast file.

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

It's also a worked example of the two processor styles pangolin supports:

| Step | Processor | Style |
| --- | --- | --- |
| 1. Download | `custom/processors/yahoo_downloader.py` | ad-hoc (no registry) |
| 2. Backup | pangolin's built-in `BackupRestore` | ad-hoc (no registry) |
| 3. Validate | pangolin's built-in `Validator` | registry pattern-match, `config/registries/stock_prices_validator.yaml` (`"*_STOCK_PRICES.csv"`) |
| 4. Historicize | `custom/processors/history_consolidator.py` | ad-hoc (no registry) |
| 5. Forecast | `custom/processors/chronos_forecaster.py` | ad-hoc (no registry) |

## Running it in Docker

Not scaffolded for this project. Re-run `pangolin init --dockerization` (or `-d`) in this same folder to add it — existing files are left untouched unless you also pass `--force`.
