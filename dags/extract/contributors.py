from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from dags.airflow_utils import MATTERMOST_DATAWAREHOUSE_IMAGE, pod_defaults, pod_env_vars, send_alert
from dags.kube_secrets import (
    SNOWFLAKE_USER,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_LOAD_ROLE,
    GITHUB_FINEGRAIN_TOKEN,
)

# Default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "owner": "airflow",
    "on_failure_callback": send_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=8),
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
}

doc_md = """
### Github contributors DAG

#### Purpose

This DAG extract github contributors information from github and stores it to Snowflake.

Configuration variables:

- `contributors_table` - the table to upload contributors to.
- `contributors_schema` - the schema where the table defined in `contributors_table` is.

"""

# Create the DAG
with DAG(
        "contributors_nightly",
        default_args=default_args,
        schedule_interval="0 7 * * *",
        catchup=False,
        max_active_runs=1,  # Don't allow multiple concurrent dag executions
        doc_md=doc_md,
    ) as dag:
    extract_github_contributors = KubernetesPodOperator(
        **pod_defaults,
        image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
        task_id="extract-github-contributors",
        name="extract-github-contributors",
        secrets=[
            SNOWFLAKE_USER,
            SNOWFLAKE_PASSWORD,
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_DATABASE,
            SNOWFLAKE_LOAD_WAREHOUSE,
            SNOWFLAKE_LOAD_ROLE,
            GITHUB_FINEGRAIN_TOKEN,
        ],
        env_vars=pod_env_vars,
        arguments=[
            "contributors mattermost {{ var.value.contributors_table }} "
            " -s {{ var.value.contributors_schema }}"
            " -d ${SNOWFLAKE_LOAD_DATABASE}"
            " -w ${SNOWFLAKE_LOAD_WAREHOUSE}"
            " -r ${SNOWFLAKE_LOAD_ROLE}"
            " --token ${GITHUB_FINEGRAIN_TOKEN}"
        ]
    )

extract_github_contributors
