# Container deployment (engine: podman)

Four services (see `docker-compose.yml`). `docker-compose.yml`/`Dockerfile` are
plain OCI/Compose-spec files — Docker and Podman both read them unchanged;
only the `make`/`make.ps1` invocation below is pinned to the engine chosen at
`pangolin init --engine`. For Podman, make sure a compose provider is
installed (`podman-compose`, or Podman >= 4.7's built-in `podman compose`).

| Service | What it does |
| --- | --- |
| `prefect-server` | Prefect 3 server + UI, persistent SQLite on a volume |
| `bootstrap` | One-shot, idempotent — runs `pangolin bootstrap` to create/update Prefect Variables and Blocks from `docker/prefect_manifest.yaml`. Safe to re-run. |
| `worker` | Runs `pangolin deploy` — serves every pipeline in `pipelines/` as a Prefect deployment. |
| `caddy` | Reverse proxy in front of the Prefect UI/API. |

## Setup

1. Copy `docker/.env.docker.example` to `docker/.env.docker` and fill it in
   (do **not** commit `.env.docker` — it may hold real secrets).
2. Fill `docker/prefect_manifest.yaml` — Variables and Blocks to seed the
   Prefect server with on bootstrap. Anything written as `${ENV_VAR}` is
   resolved from `docker/.env.docker` at bootstrap time; `null` creates an
   empty placeholder you fill in later from the Prefect UI.
3. `make build && make up`

## Reaching the UI

The Prefect UI is **not** exposed directly on the `prefect-server` container
(no host port mapping) — it's reachable through the Caddy reverse proxy:

- `http://localhost:${PROXY_PORT:-8080}`
- `http://${PROJECT_NAME:-pangolin}.localhost:${PROXY_PORT:-8080}`

Both variables come from `docker/.env.docker`. For a public deployment, set
`PUBLIC_HOSTNAME` there instead (see the comments in `docker/Caddyfile`).

## Other Makefile targets

```bash
make logs        # tail logs from all services
make ps           # service status
make restart      # restart the worker only
make bootstrap    # re-run the one-shot bootstrap (idempotent)
make shell        # shell into the worker container
make down         # stop the stack (keeps volumes)
make clean        # stop the stack AND drop volumes (wipes the Prefect DB)
```

## Updating the pangolin version baked into the image

`docker/Dockerfile` installs the `pangolin` library itself via
`PANGOLIN_INSTALL_SPEC` (defaults to git, pinned to the `PANGOLIN_REF=v0.2.0`
tag, since pangolin isn't published to PyPI/a private index yet). Override at
build time, e.g. to try an unreleased branch:

```bash
podman compose build --build-arg PANGOLIN_REF=develop
```

Once pangolin is published, switch `PANGOLIN_INSTALL_SPEC` in the Dockerfile
to a plain version pin instead.
