ARG PYTHON_IMAGE=python:3.8.15-slim@sha256:fc2f284772a4443ce7238930ba9a8d5e3c720926616fca074b99213484a3820f

## ---------------------------base stage ----------------------------- ##

# Base image to use both for builder and base. Mainly used to cache additional dependency installations.
# TODO: as soon as there's a stable distroless image, replace it with distroless.
# hadolint ignore=DL3006
FROM ${PYTHON_IMAGE} as base

## standardise on locale
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

## Don't generate .pyc, enable tracebacks on seg faults, use random seed for hashing strings and unbuffer stdout/stderr.
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1
ENV PYTHONHASHSEED random
ENV PYTHONUNBUFFERED 1

## ---------------------------builder stage ----------------------------- ##

FROM base as builder

# Poetry version
ARG POETRY_VERSION=1.2.2

# Install packages needed to build pandas/numpy etc
RUN apt-get update && apt-get install -y --no-install-recommends \
     make=4.3-4.1 \
     g++=4:10.2.1-1 && \
     rm -rf /var/lib/apt/lists/*

# Sane python defaults
## pip timeout, don't use cache, don't check for newer version and settings
ENV PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# Install poetry for managing dependencies and make sure dependencies are installed under /venv
RUN pip install "poetry==${POETRY_VERSION}" && \
    poetry config virtualenvs.create false && \
    python -m venv /venv

# Copy dependency definitions
WORKDIR /src
COPY Makefile pyproject.toml poetry.lock /src/

# Activate virtualenv in a shellcheck friendly way
ENV VIRTUAL_ENV=/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install dependencies excluding dev and current package
ENV POETRY_OPTS "--only main --no-root"
RUN make python-update-dependencies

# Build package from source code
COPY . /src/
RUN make python-build

## ---------------------------final stage ----------------------------- ##

FROM base as final

## Copy dependencies from builder
COPY --from=builder /venv /venv
## Copy package from builder
COPY --from=builder /src/dist/*.whl /app/

WORKDIR /app

# Activate virtualenv so that this python is used (see https://pythonspeed.com/articles/activate-virtualenv-dockerfile/)
ENV VIRTUAL_ENV=/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

## Install package
RUN pip install --no-cache-dir ./*.whl

# We should refrain from running as privileged user
# Run as UID for nobody
USER 65534

ENTRYPOINT ["python", "-m"]
