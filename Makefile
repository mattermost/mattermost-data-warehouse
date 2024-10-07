# ====================================================================================
# Variables

## General Variables
# Branch Variables
PROTECTED_BRANCH := master
CURRENT_BRANCH   := $(shell git rev-parse --abbrev-ref HEAD)
# Use repository name as application name
APP_NAME    := $(shell basename -s .git `git config --get remote.origin.url`)
PERMIFROST_NAME := mattermost-permifrost
# Get current commit
APP_COMMIT  := $(shell git log --pretty=format:'%h' -n 1)
# Check if we are in protected branch, if yes use `protected_branch_name-sha` as app version.
# Else check if we are in a release tag, if yes use the tag as app version, else use `dev-sha` as app version.
APP_VERSION ?= $(shell if [ $(PROTECTED_BRANCH) = $(CURRENT_BRANCH) ]; then echo $(PROTECTED_BRANCH); else (git describe --abbrev=0 --exact-match --tags 2>/dev/null || echo dev-$(APP_COMMIT)) ; fi)

# Get current date and format like: 2022-04-27 11:32
BUILD_DATE  := $(shell date +%Y-%m-%d\ %H:%M)

## General Configuration Variables
# We don't need make's built-in rules.
MAKEFLAGS     += --no-builtin-rules
# Be pedantic about undefined variables.
MAKEFLAGS     += --warn-undefined-variables
# Set help as default target
.DEFAULT_GOAL := help

## Docker Variables

# Docker executable
DOCKER                  := $(shell which docker)
# Dockerfile's location
DOCKER_FILE             += ./build/Dockerfile
PERMIFROST_DOCKER_FILE  += ./build/permifrost.Dockerfile
# Docker compose DBT file
DOCKER_COMPOSE_DBT_FILE += ./build/docker-compose.dbt.yml

# Docker options to inherit for all docker run commands
DOCKER_OPTS             += --rm -u $$(id -u):$$(id -g) --platform "linux/amd64"
# Registry to upload images
DOCKER_REGISTRY                    ?= docker.io
DOCKER_REGISTRY_REPO               ?= mattermost/${APP_NAME}
DOCKER_REGISTRY_PERMIFROST_REPO    ?= mattermost/${PERMIFROST_NAME}
# Registry credentials
DOCKER_USER             ?= user
DOCKER_PASSWORD         ?= password
## Docker Images
DOCKER_IMAGE_PYTHON     += "python:3.10.15-slim@sha256:1eb5d76bf3e9e612176ebf5eadf8f27ec300b7b4b9a99f5856f8232fd33aa16e"
DOCKER_IMAGE_DOCKERLINT += "hadolint/hadolint:v2.9.2@sha256:d355bd7df747a0f124f3b5e7b21e9dafd0cb19732a276f901f0fdee243ec1f3b"
DOCKER_IMAGE_COSIGN     += "bitnami/cosign:1.8.0@sha256:8c2c61c546258fffff18b47bb82a65af6142007306b737129a7bd5429d53629a"

## Cosign Variables
# The public key
COSIGN_PUBLIC_KEY       ?= akey
# The private key
COSIGN_KEY              ?= akey
# The passphrase used to decrypt the private key
COSIGN_PASSWORD         ?= password

## Python Variables
# Python executable
PYTHON                       := $(shell which python)
# Poetry executable
POETRY                       := $(shell which poetry)
# Poetry options
POETRY_OPTS                  ?=
# Virtualenv executable and config
VIRTUALENV				     := $(shell which virtualenv)
VIRTUALENV_AIRFLOW			 := ./.venv-airflow
# Extract Python Version
PYTHON_VERSION               ?= $(shell ${PYTHON} --version | cut -d ' ' -f 2 )
# Temporary folder to output generated artifacts
PYTHON_OUT_BIN_DIR           := ./dist

# Docker compose specific environment configuration file
DOCKER_COMPOSE_ENV_FILE      := .dbt.env

# ====================================================================================
# Colors

BLUE   := $(shell printf "\033[34m")
YELLOW := $(shell printf "\033[33m")
RED    := $(shell printf "\033[31m")
GREEN  := $(shell printf "\033[32m")
CYAN   := $(shell printf "\033[36m")
CNone  := $(shell printf "\033[0m")

# ====================================================================================
# Logger

TIME_LONG	= `date +%Y-%m-%d' '%H:%M:%S`
TIME_SHORT	= `date +%H:%M:%S`
TIME		= $(TIME_SHORT)

INFO = echo ${TIME} ${BLUE}[ .. ]${CNone}
WARN = echo ${TIME} ${YELLOW}[WARN]${CNone}
ERR  = echo ${TIME} ${RED}[FAIL]${CNone}
OK   = echo ${TIME} ${GREEN}[ OK ]${CNone}
FAIL = (echo ${TIME} ${RED}[FAIL]${CNone} && false)

# ====================================================================================
# Verbosity control hack

VERBOSE ?= 0
AT_0 := @
AT_1 :=
AT = $(AT_$(VERBOSE))

# ====================================================================================
# Targets

help: ## to get help
	$(AT)echo "Welcome to ${APP_NAME}:${APP_VERSION}"
	$(AT)echo "Usage:"
	$(AT)grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) |\
	awk 'BEGIN {FS = ":.*?## "}; {printf "make ${CYAN}%-30s${CNone} %s\n", $$1, $$2}'

.PHONY: python-build
python-build: ## to build python package
	$(AT)$(INFO) python build $*...
	$(AT)$(POETRY) build || ${FAIL}
	$(AT)$(OK) python build $*

.PHONY: python-test
python-test: ## to run python tests
	$(AT)$(INFO) testing python...
	$(AT)$(POETRY) run pytest --cov || ${FAIL}
	$(AT)$(OK) testing python

.PHONY: python-update-dependencies
python-update-dependencies: ## to update python dependencies
	$(AT)$(INFO) updating python dependencies...
	$(AT)$(POETRY) install ${POETRY_OPTS} || ${FAIL}
	$(AT)$(OK) updating python dependencies

.PHONY: python-lint
python-lint: ## to run linter on python code
	$(AT)$(INFO) python lint...
	$(AT)$(POETRY) run isort . --check || ${FAIL}
	$(AT)$(POETRY) run black . --check || ${FAIL}
	$(AT)$(POETRY) run flake8 --config build/.flake8 . || ${FAIL}
	$(AT)$(OK) python lint

.PHONY: airflow-create-virtualenv
airflow-create-virtualenv: 	## to create airflow virtualenv
	$(AT)$(INFO) creating airflow virtualenv...
	$(AT)$(VIRTUALENV) ${VIRTUALENV_AIRFLOW} || ${FAIL}
	$(AT)$(OK) created airflow virtualenv

.PHONY: airflow-update-dependencies
airflow-update-dependencies: 	## to update airflow dev dependencies
	$(AT)$(INFO) updating python dependencies...
	$(AT)$(VIRTUALENV_AIRFLOW)/bin/pip install -r build/requirements-airflow-dev.txt || ${FAIL}
	$(AT)$(OK) updating python dependencies

.PHONY: airflow-test
airflow-test: $(VIRTUALENV_AIRFLOW)  ## to run airflow tests
	@$(INFO) testing airflow...
	$(AT)$(VIRTUALENV_AIRFLOW)/bin/pytest dags plugins || ${FAIL}
	@$(OK) testing airflow

.PHONY: docker-build
docker-build: ## to build the docker image
	@$(INFO) Performing Docker build ${APP_NAME}:${APP_VERSION}
	$(AT)$(DOCKER) build \
	--build-arg PYTHON_IMAGE=${DOCKER_IMAGE_PYTHON} \
	-f ${DOCKER_FILE} . \
	-t ${APP_NAME}:${APP_VERSION} || ${FAIL}
	@$(OK) Performing Docker build ${APP_NAME}:${APP_VERSION}

.PHONY: docker-push
docker-push: ## to push the docker image
	@$(INFO) Pushing to registry...
	$(AT)$(DOCKER) tag ${APP_NAME}:${APP_VERSION} $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_REPO}:${APP_VERSION} || ${FAIL}
	$(AT)$(DOCKER) push $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_REPO}:${APP_VERSION} || ${FAIL}
# if we are on a latest semver APP_VERSION tag, also push latest
ifneq ($(shell echo $(APP_VERSION) | egrep '^v([0-9]+\.){0,2}(\*|[0-9]+)'),)
  ifeq ($(shell git tag -l --sort=v:refname | tail -n1),$(APP_VERSION))
	$(AT)$(DOCKER) tag ${APP_NAME}:${APP_VERSION} $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_REPO}:latest || ${FAIL}
	$(AT)$(DOCKER) push $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_REPO}:latest || ${FAIL}
  endif
endif
	@$(OK) Pushing to registry $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_REPO}:${APP_VERSION}

.PHONY: docker-sign
docker-sign: ## to sign the docker image
	@$(INFO) Signing the docker image...
	$(AT)echo "$${COSIGN_KEY}" > cosign.key && \
	$(DOCKER) run ${DOCKER_OPTS} \
	--entrypoint '/bin/sh' \
        -v $(PWD):/app -w /app \
	-e COSIGN_PASSWORD=${COSIGN_PASSWORD} \
	-e HOME="/tmp" \
    ${DOCKER_IMAGE_COSIGN} \
	-c \
	"echo Signing... && \
	cosign login $(DOCKER_REGISTRY) -u ${DOCKER_USER} -p ${DOCKER_PASSWORD} && \
	cosign sign --key cosign.key $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_REPO}:${APP_VERSION}" || ${FAIL}
# if we are on a latest semver APP_VERSION tag, also sign latest tag
ifneq ($(shell echo $(APP_VERSION) | egrep '^v([0-9]+\.){0,2}(\*|[0-9]+)'),)
  ifeq ($(shell git tag -l --sort=v:refname | tail -n1),$(APP_VERSION))
	$(DOCKER) run ${DOCKER_OPTS} \
	--entrypoint '/bin/sh' \
        -v $(PWD):/app -w /app \
	-e COSIGN_PASSWORD=${COSIGN_PASSWORD} \
	-e HOME="/tmp" \
	${DOCKER_IMAGE_COSIGN} \
	-c \
	"echo Signing... && \
	cosign login $(DOCKER_REGISTRY) -u ${DOCKER_USER} -p ${DOCKER_PASSWORD} && \
	cosign sign --key cosign.key $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_REPO}:latest" || ${FAIL}
  endif
endif
	$(AT)rm -f cosign.key || ${FAIL}
	@$(OK) Signing the docker image: $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_REPO}:${APP_VERSION}

.PHONY: docker-verify
docker-verify: ## to verify the docker image
	@$(INFO) Verifying the published docker image...
	$(AT)echo "$${COSIGN_PUBLIC_KEY}" > cosign_public.key && \
	$(DOCKER) run ${DOCKER_OPTS} \
	--entrypoint '/bin/sh' \
	-v $(PWD):/app -w /app \
	${DOCKER_IMAGE_COSIGN} \
	-c \
	"echo Verifying... && \
	cosign verify --key cosign_public.key $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_REPO}:${APP_VERSION}" || ${FAIL}
# if we are on a latest semver APP_VERSION tag, also verify latest tag
ifneq ($(shell echo $(APP_VERSION) | egrep '^v([0-9]+\.){0,2}(\*|[0-9]+)'),)
  ifeq ($(shell git tag -l --sort=v:refname | tail -n1),$(APP_VERSION))
	$(DOCKER) run ${DOCKER_OPTS} \
	--entrypoint '/bin/sh' \
	-v $(PWD):/app -w /app \
	${DOCKER_IMAGE_COSIGN} \
	-c \
	"echo Verifying... && \
	cosign verify --key cosign_public.key $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_REPO}:latest" || ${FAIL}
  endif
endif
	$(AT)rm -f cosign_public.key || ${FAIL}
	@$(OK) Verifying the published docker image: $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_REPO}:${APP_VERSION}

.PHONY: docker-sbom
docker-sbom: ## to print a sbom report
	@$(INFO) Performing Docker sbom report...
	$(AT)$(DOCKER) sbom ${APP_NAME}:${APP_VERSION} || ${FAIL}
	@$(OK) Performing Docker sbom report

.PHONY: docker-scan
docker-scan: ## to print a vulnerability report
	@$(INFO) Performing Docker scan report...
	$(AT)$(DOCKER) scan ${APP_NAME}:${APP_VERSION} || ${FAIL}
	@$(OK) Performing Docker scan report

.PHONY: docker-lint
docker-lint: ## to lint the Dockerfile
	@$(INFO) Dockerfile linting...
	$(AT)$(DOCKER) run -i ${DOCKER_OPTS} \
	${DOCKER_IMAGE_DOCKERLINT} \
	< ${DOCKER_FILE} || ${FAIL}
	@$(OK) Dockerfile linting

.PHONY: docker-login
docker-login: ## to login to a container registry
	@$(INFO) Dockerd login to container registry ${DOCKER_REGISTRY}...
	$(AT) echo "${DOCKER_PASSWORD}" | $(DOCKER) login --password-stdin -u ${DOCKER_USER} $(DOCKER_REGISTRY) || ${FAIL}
	@$(OK) Dockerd login to container registry ${DOCKER_REGISTRY}...


.PHONY: permifrost-docker-build
permifrost-docker-build: ## to build the docker image
	@$(INFO) Performing Permifrost Docker build ${PERMIFROST_NAME}:${APP_VERSION}
	$(AT)$(DOCKER) build \
	--build-arg PYTHON_IMAGE=${DOCKER_IMAGE_PYTHON} \
	-f ${PERMIFROST_DOCKER_FILE} . \
	-t ${PERMIFROST_NAME}:${APP_VERSION} || ${FAIL}
	@$(OK) Performing Permifrost Docker build ${PERMIFROST_NAME}:${APP_VERSION}

.PHONY: permifrost-docker-push
permifrost-docker-push: ## to push the docker image
	@$(INFO) Pushing permifrost to registry...
	$(AT)$(DOCKER) tag ${PERMIFROST_NAME}:${APP_VERSION} $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_PERMIFROST_REPO}:${APP_VERSION} || ${FAIL}
	$(AT)$(DOCKER) push $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_PERMIFROST_REPO}:${APP_VERSION} || ${FAIL}
# if we are on a latest semver APP_VERSION tag, also push latest
ifneq ($(shell echo $(APP_VERSION) | egrep '^v([0-9]+\.){0,2}(\*|[0-9]+)'),)
  ifeq ($(shell git tag -l --sort=v:refname | tail -n1),$(APP_VERSION))
	$(AT)$(DOCKER) tag ${APP_NAME}:${APP_VERSION} $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_PERMIFROST_REPO}:latest || ${FAIL}
	$(AT)$(DOCKER) push $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_PERMIFROST_REPO}:latest || ${FAIL}
  endif
endif
	@$(OK) Pushing to registry $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_PERMIFROST_REPO}:${APP_VERSION}

.PHONY: permifrost-docker-sign
permifrost-docker-sign: ## to sign the permifrost docker image
	@$(INFO) Signing the Permifrost Docker image...
	$(AT)echo "$${COSIGN_KEY}" > cosign.key && \
	$(DOCKER) run ${DOCKER_OPTS} \
	--entrypoint '/bin/sh' \
        -v $(PWD):/app -w /app \
	-e COSIGN_PASSWORD=${COSIGN_PASSWORD} \
	-e HOME="/tmp" \
    ${DOCKER_IMAGE_COSIGN} \
	-c \
	"echo Signing... && \
	cosign login $(DOCKER_REGISTRY) -u ${DOCKER_USER} -p ${DOCKER_PASSWORD} && \
	cosign sign --key cosign.key $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_PERMIFROST_REPO}:${APP_VERSION}" || ${FAIL}
# if we are on a latest semver APP_VERSION tag, also sign latest tag
ifneq ($(shell echo $(APP_VERSION) | egrep '^v([0-9]+\.){0,2}(\*|[0-9]+)'),)
  ifeq ($(shell git tag -l --sort=v:refname | tail -n1),$(APP_VERSION))
	$(DOCKER) run ${DOCKER_OPTS} \
	--entrypoint '/bin/sh' \
        -v $(PWD):/app -w /app \
	-e COSIGN_PASSWORD=${COSIGN_PASSWORD} \
	-e HOME="/tmp" \
	${DOCKER_IMAGE_COSIGN} \
	-c \
	"echo Signing... && \
	cosign login $(DOCKER_REGISTRY) -u ${DOCKER_USER} -p ${DOCKER_PASSWORD} && \
	cosign sign --key cosign.key $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_PERMIFROST_REPO}:latest" || ${FAIL}
  endif
endif
	$(AT)rm -f cosign.key || ${FAIL}
	@$(OK) Signing the Permifrost Docker image: $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_PERMIFROST_REPO}:${APP_VERSION}

.PHONY: permifrost-docker-verify
permifrost-docker-verify: ## to verify the Permifrost Docker image
	@$(INFO) Verifying the published Permifrost Docker image...
	$(AT)echo "$${COSIGN_PUBLIC_KEY}" > cosign_public.key && \
	$(DOCKER) run ${DOCKER_OPTS} \
	--entrypoint '/bin/sh' \
	-v $(PWD):/app -w /app \
	${DOCKER_IMAGE_COSIGN} \
	-c \
	"echo Verifying... && \
	cosign verify --key cosign_public.key $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_PERMIFROST_REPO}:${APP_VERSION}" || ${FAIL}
# if we are on a latest semver APP_VERSION tag, also verify latest tag
ifneq ($(shell echo $(APP_VERSION) | egrep '^v([0-9]+\.){0,2}(\*|[0-9]+)'),)
  ifeq ($(shell git tag -l --sort=v:refname | tail -n1),$(APP_VERSION))
	$(DOCKER) run ${DOCKER_OPTS} \
	--entrypoint '/bin/sh' \
	-v $(PWD):/app -w /app \
	${DOCKER_IMAGE_COSIGN} \
	-c \
	"echo Verifying... && \
	cosign verify --key cosign_public.key $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_PERMIFROST_REPO}:latest" || ${FAIL}
  endif
endif
	$(AT)rm -f cosign_public.key || ${FAIL}
	@$(OK) Verifying the published docker image: $(DOCKER_REGISTRY)/${DOCKER_REGISTRY_PERMIFROST_REPO}:${APP_VERSION}

.PHONY: permifrost-docker-sbom
permifrost-docker-sbom: ## to print a sbom report for Permifrost Docker image
	@$(INFO) Performing Permifrost Docker sbom report...
	$(AT)$(DOCKER) sbom ${PERMIFROST_NAME}:${APP_VERSION} || ${FAIL}
	@$(OK) Performing Permifrost Docker sbom report


.PHONY: permifrost-docker-scan
permifrost-docker-scan: ## to print a vulnerability report  for Permifrost Docker image
	@$(INFO) Performing Permifrost Docker scan report...
	$(AT)$(DOCKER) scan ${PERMIFROST_NAME}:${APP_VERSION} || ${FAIL}
	@$(OK) Performing Docker scan report

.PHONY: permifrost-docker-lint
permifrost-docker-lint: ## to lint the Permifrost Dockerfile
	@$(INFO) Dockerfile linting...
	$(AT)$(DOCKER) run -i ${DOCKER_OPTS} \
	${DOCKER_IMAGE_DOCKERLINT} \
	< ${PERMIFROST_DOCKER_FILE} || ${FAIL}
	@$(OK) Dockerfile linting

.PHONY: dbt-docs
dbt-docs: ## to generate and serve dbt docs
	$(AT)$(INFO) Generating docs and spinning up the a webserver on port 8081...
	$(AT)$(DOCKER) compose -f ${DOCKER_COMPOSE_DBT_FILE} run -d -p "8081:8081" dbt_image bash -c "dbt deps && dbt docs generate -t prod && dbt docs serve --port 8081"  || ${FAIL}
	$(AT)$(OK) Server started, visit http://localhost:8081 to view the docs

.PHONY: dbt-generate-docs
dbt-generate-docs: ## to generate dbt docs
	$(AT)$(INFO) Generating docs...
	$(AT)$(DOCKER) compose -f ${DOCKER_COMPOSE_DBT_FILE} run dbt_image bash -c "dbt deps && dbt docs generate -t prod" } || ${FAIL}
	$(AT)$(OK) Generated docs

.PHONY: dbt-bash
dbt-bash: ## to start a bash shell with DBT for snowflake-dbt project
	$(AT)$(INFO) Running bash with dbt in ${CYAN}snowflake-dbt${CNone}...
	$(AT)$(DOCKER) compose -f ${DOCKER_COMPOSE_DBT_FILE} run dbt_image bash -c "dbt deps && /bin/bash" || ${FAIL}
	$(AT)$(OK) Exited dbt bash

.PHONY: dbt-mattermost-analytics
dbt-mattermost-analytics: ## to start a bash shell with DBT for mattermost analytics project
	$(AT)$(INFO) Running bash with dbt in ${CYAN}mattermost_analytics${CNone}...
	$(AT)$(DOCKER) compose -f ${DOCKER_COMPOSE_DBT_FILE} run -p "8081:8081" mattermost_analytics bash -c "dbt deps && /bin/bash" --force-recreate || ${FAIL}
	$(AT)$(OK) Exited dbt bash

.PHONY: data-image
data-image: ## to start a bash shell on the data image
	$(AT)$(INFO) Attaching to data-image and mounting repo...
	$(AT)$(DOCKER) compose -f ${DOCKER_COMPOSE_DBT_FILE} run data_image bash || ${FAIL}
	$(AT)$(OK) Exited data-image

.PHONY: permifrost-image
permifrost-image: ## to start a bash shell on the permifrost image
	$(AT)$(INFO) Attaching to permifrost-image and mounting repo...
	$(AT)$(DOCKER) compose -f ${DOCKER_COMPOSE_DBT_FILE} run permifrost bash || ${FAIL}
	$(AT)$(OK) Exited permifrost-image

.PHONY: clean
clean: ## to clean-up
	@$(INFO) cleaning...
	$(AT)rm -rf ${PYTHON_OUT_BIN_DIR}  \
		 rm -rf ${VIRTUALENV_AIRFLOW} || ${FAIL}
	$(AT)$(OK) cleaning completed
