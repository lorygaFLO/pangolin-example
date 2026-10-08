# Pangolin framework guide (for AI agents)

Scaffolded once by `pangolin init` — never overwritten again unless you
pass `--force`, so it's safe to extend. It describes the **pangolin
framework itself**, not this project's business logic.

For this project's actual settings/folders and any team-written notes, see
this project's own `README.md` (generated alongside this file, with the
exact settings and folder tables for this project) and any
project-specific `AGENTS.md` / `CLAUDE.md` the team keeps at the repo
root — this file lives here specifically so it never collides with those.

## Mental model

Data flows through four layers, top to bottom:

1. **`config/data_structure.yaml`** — declares the project's folder
   layout. Every top-level node with a `_settings_key` becomes an
   overridable `.env` setting (`S.<KEY>`); a node with
   `_pattern_matching: true` + `_registry: <path>` links it to one of the
   registries below.
2. **`config/registries/*.yaml`** — one file per pattern-match step: which
   validators/transformers run, matched by filename glob pattern.
3. **`custom/`** — validators / transformers / processors / settings /
   run_context specific to *this* project. Built-ins ship with pangolin
   itself (`pangolin.utils.validators` / `pangolin.utils.transformers`) —
   don't duplicate them here, only add what's genuinely custom.
4. **`pipelines/`** — one file per pipeline; each exposes a module-level
   `PIPELINE = <flow>` (a Prefect flow). Auto-discovered by every CLI
   command below — no registration step needed beyond that.

## Where to make a change

| Task | Touch this |
|---|---|
| New pipeline | new file in `pipelines/`, module-level `PIPELINE = <flow>` |
| New folder, or make a path overridable via `.env` | add a `_settings_key` node in `config/data_structure.yaml` |
| New validation/transform rule for a file pattern | `config/registries/*.yaml` |
| Validation/transform logic that isn't built-in | `custom/validators.py` / `custom/transformers.py` |
| Processing step beyond validate/transform | `custom/processors/` (create it — subclass `BaseProcessor`) |
| A project-only setting (`S.MY_FLAG`) | `custom/settings.py`, subclass `SETTINGS` |
| Extra fields on the per-run context | `custom/run_context.py` |

## CLI

```bash
pangolin list                # discovered pipelines + their debuggable steps
pangolin run [pipeline]      # run once, foreground
pangolin step <p> <step>     # run one step in isolation (needs DEBUG=True)
pangolin restore <run_id>    # restore input from a previous backup
pangolin prefect-server      # persistent local Prefect server, isolated per project
pangolin deploy              # serve every pipeline as a Prefect deployment
pangolin bootstrap           # apply docker/prefect_manifest.yaml to a Prefect server
```

## Gotchas worth knowing before editing

- **`DEBUG` / `DEBUG_RUN_ID`** — with `DEBUG=True` in `.env`, `RUN_ID` is
  pinned to `DEBUG_RUN_ID` instead of a fresh timestamp, so `pangolin step`
  can find staging left by a previous run. Set it *before* `pangolin
  restore` too when chaining restore → step, so both agree on the same run
  folder.
- **`PROJECT_NAME` / local Prefect isolation** — every pangolin project on
  the same machine gets its own local Prefect state at
  `.prefect/<PROJECT_NAME>`, derived automatically by `pangolin run` /
  `deploy` / `bootstrap` / `prefect-server`. Never reuse another project's
  `PROJECT_NAME` on the same machine, or their deployments/runs/logs mix
  with this project's in the same dashboard. Always start the dashboard
  with `pangolin prefect-server`, never the bare `prefect server start` —
  the bare command doesn't get this isolation and silently falls back to
  Prefect's shared global state.
- **Settings: fixed vs. yours** — `pangolin.config.settings.SETTINGS` (core
  fields like `BASEPATH`, `FS_PROTOCOL`, ...) lives in the installed
  library, not this project — don't edit it. A project-specific setting
  goes in `custom/settings.py` instead; `get_settings()` picks up that
  subclass automatically when it exists.
- **Container mode** — only relevant if `docker/` exists in this project
  (re-run `pangolin init --containerization` to add it; `--engine podman`
  targets Podman instead of Docker). Isolation between projects on the same
  host uses the same `PROJECT_NAME`, this time read from `docker/.env.docker`
  (namespaces containers/volumes instead of a local folder).

## Full reference

This project's own `README.md` has the complete settings table (generated
from the installed pangolin version) and the folder-settings table
(generated from this project's own `config/data_structure.yaml`) — both
project-specific, so not duplicated here.
