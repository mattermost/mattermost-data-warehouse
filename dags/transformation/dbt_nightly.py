from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.trigger_rule import TriggerRule

from dags.airflow_utils import MATTERMOST_DATAWAREHOUSE_IMAGE, pod_defaults, pod_env_vars, send_alert
from dags.kube_secrets import (
    DBT_CLOUD_API_ACCOUNT_ID,
    DBT_CLOUD_API_KEY,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
    SSH_KEY,
)

# Load the env vars into a dict and set Secrets
env_vars = {**pod_env_vars, **{}}

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
### Nightly DBT dag

#### Purpose
This DAG triggers nightly tasks:
- Trigger DBT models with `nightly` tag.
- Update github contributors.

#### Notes
- DAG was previously running dbt seed. This option has been removed.
"""

# Create the DAG
dag = DAG(
    "dbt_nightly",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    catchup=False,
    max_active_runs=1,  # Don't allow multiple concurrent dag executions
    doc_md=doc_md,
)

dbt_run_cloud_nightly = KubernetesPodOperator(
    **pod_defaults,
    image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
    task_id="dbt-cloud-run-nightly",
    name="dbt-cloud-run-nightly",
    secrets=[
        DBT_CLOUD_API_ACCOUNT_ID,
        DBT_CLOUD_API_KEY,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
        SSH_KEY,
    ],
    env_vars={**env_vars, "DBT_JOB_TIMEOUT": "3600"},
    arguments=["python -m utils.run_dbt_cloud_job 19427 \"Airflow dbt nightly\""],
    dag=dag,
)

dbt_run_cloud_mattermost_analytics_nightly = KubernetesPodOperator(
    **pod_defaults,
    image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
    task_id="dbt-cloud-mattermost-analytics-nightly",
    name="dbt-cloud-mattermost-analytics-nightly",
    trigger_rule=TriggerRule.ALL_DONE,
    secrets=[
        DBT_CLOUD_API_ACCOUNT_ID,
        DBT_CLOUD_API_KEY,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
        SSH_KEY,
    ],
    # Set timeout to 2.5 hours
    env_vars={**env_vars, "DBT_JOB_TIMEOUT": "9000"},
    arguments=["python -m  utils.run_dbt_cloud_job 254981 \"Mattermost Analytics DBT nightly\""],
    dag=dag,
)

dbt_run_cloud_nightly >> dbt_run_cloud_mattermost_analytics_nightly
