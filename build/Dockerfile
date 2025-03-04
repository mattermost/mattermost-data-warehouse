# Use a multi-arch base image by omitting the digest.
ARG PYTHON_IMAGE=python:3.10.15-slim

## --------------------------- Base Stage ----------------------------- ##
# Base image to use both for builder and final image.
# hadolint ignore=DL3006
FROM ${PYTHON_IMAGE} as base

# Standardize on locale.
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

# Python environment defaults.
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONFAULTHANDLER=1
ENV PYTHONHASHSEED=random
ENV PYTHONUNBUFFERED=1

## --------------------------- Builder Stage ----------------------------- ##
FROM base as builder

# Define Poetry version.
ARG POETRY_VERSION=1.7.1

# Install packages needed to build libraries such as pandas and numpy.
RUN apt-get update && apt-get install -y --no-install-recommends \
    make=4.3-4.1 \
    g++=4:12.2.0-3 \
    git=1:2.39.5-0+deb12u1 && \
    rm -rf /var/lib/apt/lists/*

# Sane pip defaults.
ENV PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# Install Poetry and set up a virtual environment under /venv.
RUN pip install "poetry==${POETRY_VERSION}" && \
    poetry config virtualenvs.create false && \
    python -m venv /venv

# Set working directory and copy dependency definitions.
WORKDIR /src
COPY .git Makefile pyproject.toml poetry.lock /src/

# Activate virtualenv in a shellcheck-friendly way.
ENV VIRTUAL_ENV=/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install project dependencies (excluding dev and the current package).
ENV POETRY_OPTS="--only main --no-root"
RUN make python-update-dependencies

# Copy the rest of the source code and build the package.
COPY . /src/
RUN make python-build

## --------------------------- Final Stage ----------------------------- ##
FROM base as final

# Copy the virtual environment and built package from the builder stage.
COPY --from=builder /venv /venv
COPY --from=builder /src/dist/*.whl /app/

WORKDIR /app

# Activate virtualenv so that this Python is used.
ENV VIRTUAL_ENV=/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install the built package.
RUN pip install --no-cache-dir ./*.whl

# Copy additional files required for transformations.
COPY transform/sql /app/transform/sql

# Run as non-privileged user.
USER 65534

ENTRYPOINT ["python", "-m"]
