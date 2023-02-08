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

# Sane python defaults
## pip timeout, don't use cache, don't check for newer version and settings
ENV PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# Activate virtualenv in a shellcheck friendly way
ENV VIRTUAL_ENV=/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install dependencies excluding dev and current package
RUN pip install "permifrost==0.1.0"

## ---------------------------final stage ----------------------------- ##

FROM base as final

## Copy dependencies from builder
COPY --from=builder /venv /venv

WORKDIR /app

# Activate virtualenv so that this python is used (see https://pythonspeed.com/articles/activate-virtualenv-dockerfile/)
ENV VIRTUAL_ENV=/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Copy files required for transformations
COPY load/snowflake /app/load/snowflake

# We should refrain from running as privileged user
# Run as UID for nobody
USER 65534
