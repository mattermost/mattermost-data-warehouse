# ====================================================================================
# Variables

## General Variables
# Branch Variables
PROTECTED_BRANCH := master
CURRENT_BRANCH   := $(shell git rev-parse --abbrev-ref HEAD)
# Use repository name as application name
APP_NAME    := $(shell basename -s .git `git config --get remote.origin.url`)
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
# Docker compose DBT file
DOCKER_COMPOSE_DBT_FILE += ./build/docker-compose.dbt.yml

## Python Variables
# Python executable
PYTHON                       := $(shell which python)
# Poetry executable
POETRY                       := $(shell which poetry)
# Extract Python Version
PYTHON_VERSION               ?= $(shell ${PYTHON} --version | cut -d ' ' -f 2 )
# Temporary folder to output generated artifacts
PYTHON_OUT_BIN_DIR           := ./dist

# Docker compose specific environment configuration file
DOCKER_COMPOSE_ENV_FILE      := .dbt.env

## Github Variables
# A github access token that provides access to upload artifacts under releases
GITHUB_TOKEN                 ?= a_token
# Github organization
GITHUB_ORG                   := mattermost
# Most probably the name of the repo
GITHUB_REPO                  := ${APP_NAME}

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
	$(AT)$(POETRY) run pytest || ${FAIL}
	$(AT)$(OK) testing python

.PHONY: python-update-dependencies
python-update-dependencies: ## to update python dependencies
	$(AT)$(INFO) updating python dependencies...
	$(AT)$(POETRY) install && \
	    $(POETRY) run pip install clearbit==0.1.7 || ${FAIL}
	$(AT)$(OK) updating python dependencies

.PHONY: dbt-docs
dbt-docs: ## to generate and serve dbt docs
	$(AT)$(INFO) Generating docs and spinning up the a webserver on port 8081...
	$(AT)export DBT_PROFILE_PATH=".";\
	$(DOCKER) compose run -p "8081:8081" dbt_image bash -c "dbt deps && dbt docs generate -t prod && dbt docs serve --port 8081"

.PHONY: dbt-generate-docs
dbt-generate-docs: ## to generate dbt docs
	$(AT)$(INFO) Generating docs
	$(AT)export DBT_PROFILE_PATH=".";\
	$(DOCKER) compose run dbt_image bash -c "dbt deps && dbt docs generate -t prod"

.PHONY: dbt-bash
dbt-bash: ## to start a bash shell with DBT
	$(AT)$(INFO) Running bash with dbt...
	$(AT)export DBT_PROFILE_PATH=".";\
	$(DOCKER) compose -f ${DOCKER_COMPOSE_DBT_FILE} run dbt_image bash -c "dbt deps && /bin/bash"

.PHONY: data-image
data-image: ## to start a bash shell on the data image
	$(AT)$(INFO) Attaching to data-image and mounting repo...
	$(AT)export DBT_PROFILE_PATH=".";\
	$(DOCKER) compose -f ${DOCKER_COMPOSE_DBT_FILE} run data_image bash

.PHONY: clean
clean: ## to clean-up
	@$(INFO) cleaning ${PYTHON_OUT_BIN_DIR} folder...
	$(AT)rm -rf ${PYTHON_OUT_BIN_DIR} || ${FAIL}
	@$(OK) cleaning ${PYTHON_OUT_BIN_DIR} folder