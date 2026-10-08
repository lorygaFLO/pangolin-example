# syntax=docker/dockerfile:1.6
FROM python:3.11-slim AS base

# Build-time identity (filled by `make build` or `docker build --build-arg ...`)
ARG GIT_BRANCH=unknown
ARG GIT_SHA=unknown

# pangolin isn't on PyPI yet, so by default we install straight from git.
# Once it's published (PyPI or a private index), swap PANGOLIN_INSTALL_SPEC
# for a plain version pin, e.g. --build-arg PANGOLIN_INSTALL_SPEC=pangolin==0.2.0
# Pinned to the release tag (not a branch): v0.2.0 is the first one that ships
# pangolin.engine.DataCatalog, which this project's custom processors import.
ARG PANGOLIN_REF=v0.2.0
ARG PANGOLIN_INSTALL_SPEC=git+https://github.com/lorygaFLO/pangolin.git@${PANGOLIN_REF}

# Surface them at runtime so they show up in worker logs and Prefect Variables
ENV GIT_BRANCH=${GIT_BRANCH} \
    GIT_SHA=${GIT_SHA} \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# OS deps (curl is used by compose healthchecks; build-essential covers wheels
# that don't ship a manylinux build for python:3.11-slim)
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install the pangolin library + CLI first to leverage layer cache
RUN pip install --upgrade pip \
    && pip install "pangolin @ ${PANGOLIN_INSTALL_SPEC}"

# Project source: config/, pipelines/, custom/ (this repo, NOT the pangolin
# library — that came from PANGOLIN_INSTALL_SPEC above)
COPY . /app

# Default workdir contents
RUN mkdir -p /app/data

LABEL org.opencontainers.image.title="pangolin-project" \
      pangolin.git.branch="${GIT_BRANCH}" \
      pangolin.git.sha="${GIT_SHA}"

# Default command is overridden per-service in docker-compose.yml
CMD ["pangolin", "deploy"]
