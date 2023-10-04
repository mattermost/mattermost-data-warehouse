# mattermost-data-warehouse

This repo contains the code for loading data to Mattermost's data warehouse, performing transformations and exporting 
data to other tools.

<!-- TOC -->
* [mattermost-data-warehouse](#mattermost-data-warehouse)
  * [Repository structure](#repository-structure)
  * [What does this repo contain?](#what-does-this-repo-contain)
    * [Extract](#extract)
    * [Transform](#transform)
    * [Load](#load)
    * [Billing](#billing)
    * [Utils](#utils)
    * [DAGs](#dags)
  * [DBT setuo & development](#dbt-setuo--development)
  * [Python Development](#python-development)
    * [Requirements](#requirements)
    * [Install dependencies](#install-dependencies)
    * [Adding dependencies](#adding-dependencies)
  * [Configuration](#configuration)
    * [Snowflake connections](#snowflake-connections)
  * [Airflow](#airflow)
<!-- TOC -->

## Repository structure

```
.
├── dags                    <--- Airflow DAGs. DAGs mostly use KubernetesOperator to run a job.
├── extract                 <--- Python scripts that extract data from various locations.
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
│  ├── mattermost-analytics <--- New DBT project for Mattermost analytics.
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

## DBT setuo & development

Please see [DBT setup instructions](docs/DBT.md) for setting up DBT and for performing common operations.

[DBT development guidelines](docs/DBT-dev.md) contains instructions about the DBT development environment, as well as for
common development operations.

## Python Development

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
    <tr>
      <td>RELEASE_LOCATION</td>
      <td>Location to load release data from</td>
    </tr>
  </tbody>
</table>

## Airflow

Please see the [separate documentation file](docs/Airflow.md).