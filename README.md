# mattermost-data-warehouse

This repo contains the code for loading data to Mattermost's data warehouse, performing transformations and exporting 
data to other tools.

<!-- TOC -->
* [Repository structure](#repository-structure)
* [What does this repo contain?](#what-does-this-repo-contain)
  * [Extract](#extract)
  * [Transform](#transform)
  * [Load](#load)
  * [Billing](#billing)
  * [Utils](#utils)
  * [DAGs](#dags)
* [Running locally](#running-locally)
  * [Prerequisites](#prerequisites)
  * [Setup](#setup)
  * [Starting a bash shell with DBT](#starting-a-bash-shell-with-dbt)
  * [Building docs and serving them locally](#building-docs-and-serving-them-locally)
* [Developing](#developing)
  * [Requirements](#requirements)
  * [Install dependencies](#install-dependencies)
  * [Adding dependencies](#adding-dependencies)
* [Configuration](#configuration)
  * [Snowflake connections](#snowflake-connections)
<!-- TOC -->

## Repository structure

```
.
├── billing                 
├── dags                    <--- Airflow DAGs. DAGs mostly use KubernetesOperator to run a job.
├── extract                 <--- Python scripts that extract data from various locations.
│  ├── pg_import            <--- Generic utility that copies data from Snowflake to Postgres
│  └── s3_extract           <--- Various utilities for importing data from S3 to Snowflake
├── k8s                     <--- Pod definitions for Pipelinewise
├── load        
│  └── snowflake            <--- Snowflake role definitions
├── README.md               <--- This file
├── poetry.lock             <--- Pinned dependency versions
├── pyproject.toml          <--- PEP-518 build system requirements
├── requirements.txt        <--- Dependencies (deprecated)
├── transform
│  ├── snowflake-dbt        <--- Snowflake DBT models
│  └── sql                  <--- SQL scripts that get executed by DAGs
└── utils                   <--- Various Python scripts
```

## What does this repo contain?

### Extract

Tools for extracting data 
- from S3 to Snowflake and
- from Snowflake to Postgres.

All extractions are executed using Airflow on a Kubernetes cluster.

### Transform

- DBT project for running transformations on Snowflake. The DBT project runs on DBT cloud.
- SQL scripts for NPS feedback. The scripts are executed by an Airflow DAG.

### Load

Snowflake role definitions. An Airflow DAG runs the update.

### Billing

Trigger building invoice for subscriptions. Triggered via an Airflow DAG.

### Utils

A set of Python scripts performing custom ETLs. The utilities run as part of Airflow DAGs.

### DAGs

Airflow DAGs that orchestrate ETL pipelines.

## Running locally

### Prerequisites

- Docker & docker compose
- Snowflake access with proper roles (ask the team which role to use)

### Setup

Copy `.dbt.env.example` to `.dbt.env`. Edit the file and replace placeholder with appropriate values.

### Starting a bash shell with DBT

At the home of the repo run:

```bash
DBT_PROFILE_PATH=./transform/snowflake-dbt/profile make dbt-bash
```

This command creates a container with `dbt` pre-installed and connects you to the bash shell.

To test that everything is working as expected, try to generate dbt docs:

```bash
dbt docs generate -t prod
```

If the output looks similar to:
```
...

07:28:30  Found 289 models, 10 tests, 0 snapshots, 0 analyses, 407 macros, 0 operations, 15 seed files, 263 sources, 0 exposures, 0 metrics
07:28:30  
07:29:00  Concurrency: 8 threads (target='prod')
07:29:00  
07:32:04  Done.
07:32:04  Building catalog
07:44:19  Catalog written to /usr/app/target/catalog.json
```

then the setup is working.


### Building docs and serving them locally

At the home of the repo run:

```bash
DBT_PROFILE_PATH=./transform/snowflake-dbt/profile make dbt-docs
```

This command will generate the docs and serve them at [http://localhost:8081](http://localhost:8081).

## Developing

### Requirements

- [Poetry](https://python-poetry.org/docs/#installation)

### Install dependencies

Run the following commands:
```bash
poetry install

# Clearbit cannot be installed as part of poetry dependencies, as it's a really old dependency.
poetry run pip install clearbit==0.1.7
```

### Adding dependencies

Additional dependencies can be specified at `pyproject.toml`. [Poetry's documentation](https://python-poetry.org/docs/basic-usage/#specifying-dependencies) 
provides examples. Please prefer using `poetry` CLI, as it also updates `poetry.lock` file and "pins" any new dependencies.

> Note that currently there's a `requirements.txt` file. This file will be deprecated.


## Configuration

### Snowflake connections

Snowflake connection details can be configured by adding the proper environment variables. Each role requires a
different set of environment variables. The following table describes the required environment variables for each role:

<table>
  <thead>
   <tr>
    <th>Role</th>
    <th>Environment variable</th>
    <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <!-- SYSADMIN -->
    <tr>
        <td rowspan=5>SYSADMIN</td>
        <td>SNOWFLAKE_USER</td>
        <td>Username</td>
    </tr>
    <tr>
        <td>SNOWFLAKE_PASSWORD</td>
        <td>Password</td>
    </tr>
    <tr>
        <td>SNOWFLAKE_ACCOUNT</td>
        <td>Snowflake account to connect to</td>
    </tr>
    <tr>
        <td>SNOWFLAKE_LOAD_DATABASE</td>
        <td>Database to load data to</td>
    </tr>
    <tr>
        <td>SNOWFLAKE_LOAD_WAREHOUSE</td>
        <td>Warehouse to load data to</td>
    </tr>
    <!-- ANALYTICS_LOADER -->
    <tr>
        <td rowspan=5>ANALYTICS_LOADER</td>
        <td>SNOWFLAKE_LOAD_USER</td>
        <td>Username</td>
    </tr>
    <tr>
        <td>SNOWFLAKE_LOAD_PASSWORD</td>
        <td>Password</td>
    </tr>
    <tr>
        <td>SNOWFLAKE_ACCOUNT</td>
        <td>Snowflake account to connect to</td>
    </tr>
    <tr>
        <td>SNOWFLAKE_TRANSFORM_DATABASE</td>
        <td>Database to load data to</td>
    </tr>
    <tr>
        <td>SNOWFLAKE_LOAD_WAREHOUSE</td>
        <td>Warehouse to load data to</td>
    </tr>
    <!-- LOADER -->
    <tr>
        <td rowspan=5>LOADER</td>
        <td>SNOWFLAKE_LOAD_USER</td>
        <td>Username</td>
    </tr>
    <tr>
        <td>SNOWFLAKE_LOAD_PASSWORD</td>
        <td>Password</td>
    </tr>
    <tr>
        <td>SNOWFLAKE_ACCOUNT</td>
        <td>Snowflake account to connect to</td>
    </tr>
    <tr>
        <td>SNOWFLAKE_LOAD_DATABASE</td>
        <td>Database to load data to</td>
    </tr>
    <tr>
        <td>SNOWFLAKE_LOAD_WAREHOUSE</td>
        <td>Warehouse to load data to</td>
    </tr>
    <!-- TRANSFORMER -->
    <tr>
        <td rowspan=5>TRANSFORMER</td>
        <td>SNOWFLAKE_USER</td>
        <td>Username</td>
    </tr>
    <tr>
        <td>SNOWFLAKE_PASSWORD</td>
        <td>Password</td>
    </tr>
    <tr>
        <td>SNOWFLAKE_ACCOUNT</td>
        <td>Snowflake account to connect to</td>
    </tr>
    <tr>
        <td>SNOWFLAKE_TRANSFORM_DATABASE</td>
        <td>Database to use for transforming data</td>
    </tr>
    <tr>
        <td>SNOWFLAKE_TRANSFORM_WAREHOUSE</td>
        <td>Warehouse to store transformed data to</td>
    </tr>
    <!-- PERMISSIONS -->
    <tr>
        <td rowspan=5>PERMISSIONS</td>
        <td>PERMISSION_BOT_USER</td>
        <td>Username</td>
    </tr>
    <tr>
        <td>PERMISSION_BOT_PASSWORD</td>
        <td>Password</td>
    </tr>
    <tr>
        <td>PERMISSION_BOT_ACCOUNT</td>
        <td>Snowflake account to connect to</td>
    </tr>
    <tr>
        <td>PERMISSION_BOT_DATABASE</td>
        <td>Database to use for transforming data</td>
    </tr>
    <tr>
        <td>PERMISSION_BOT_WAREHOUSE</td>
        <td>Warehouse to store transformed data to</td>
    </tr>
  </tbody>
</table>