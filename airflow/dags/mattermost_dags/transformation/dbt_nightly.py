from datetime import datetime, timedelta

from mattermost_dags.airflow_utils import MATTERMOST_DATAWAREHOUSE_IMAGE, pod_defaults, pod_env_vars, send_alert
from mattermost_dags.kube_secrets import (
    DBT_CLOUD_API_ACCOUNT_ID,
    DBT_CLOUD_API_KEY,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_DATABASE,
    SNOWFLAKE_TRANSFORM_LARGE_WAREHOUSE,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
)

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.trigger_rule import TriggerRule

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
    schedule="0 7 * * *",
    catchup=False,
    max_active_runs=1,  # Don't allow multiple concurrent dag executions
    doc_md=doc_md,
)

# Previously hourly jobs for old project

# Parse user agents and update table
user_agent = KubernetesPodOperator(
    **pod_defaults,
    image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
    task_id="user-agent",
    name="user-agent",
    secrets=[
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
    ],
    env_vars=env_vars,
    arguments=["python -m utils.user_agent_parser"],
    dag=dag,
)

# Deferred merge helpers - merge event delta table into base table
deferred_merge = KubernetesPodOperator(
    **pod_defaults,
    image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
    task_id="deferred-merge",
    name="deferred-merge",
    trigger_rule=TriggerRule.ALL_DONE,
    secrets=[
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_LARGE_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_DATABASE,
    ],
    env_vars=env_vars,
    arguments=[
        "snowflake "
        " -a ${SNOWFLAKE_ACCOUNT}"
        " -u ${SNOWFLAKE_USER}"
        " -p ${SNOWFLAKE_PASSWORD}"
        " -d ${SNOWFLAKE_TRANSFORM_DATABASE}"
        " -s {{ var.value.rudderstack_support_schema }}"
        " -w ${SNOWFLAKE_TRANSFORM_LARGE_WAREHOUSE}"
        " -r ${SNOWFLAKE_TRANSFORM_ROLE}"
        " merge {{ var.value.base_events_table }} "
        " {{ var.value.base_events_delta_schema }} {{ var.value.base_events_delta_table }}"
    ],
    dag=dag,
)

# Old hourly job
dbt_run_cloud = KubernetesPodOperator(
    **pod_defaults,
    image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
    task_id="dbt-cloud-run",
    name="dbt-cloud-run",
    secrets=[
        DBT_CLOUD_API_ACCOUNT_ID,
        DBT_CLOUD_API_KEY,
    ],
    env_vars={**env_vars, "DBT_JOB_TIMEOUT": "4200"},
    arguments=["python -m  utils.run_dbt_cloud_job 19444 \"Airflow dbt hourly\""],
    dag=dag,
)


# Old project nightly
dbt_run_cloud_nightly = KubernetesPodOperator(
    **pod_defaults,
    image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
    task_id="dbt-cloud-run-nightly",
    name="dbt-cloud-run-nightly",
    secrets=[
        DBT_CLOUD_API_ACCOUNT_ID,
        DBT_CLOUD_API_KEY,
    ],
    env_vars={**env_vars, "DBT_JOB_TIMEOUT": "4800"},
    arguments=["python -m utils.run_dbt_cloud_job 19427 \"Airflow dbt nightly\""],
    dag=dag,
)

# New project nightly
dbt_run_cloud_mattermost_analytics_nightly = KubernetesPodOperator(
    **pod_defaults,
    image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
    task_id="dbt-cloud-mattermost-analytics-nightly",
    name="dbt-cloud-mattermost-analytics-nightly",
    trigger_rule=TriggerRule.ALL_DONE,
    secrets=[
        DBT_CLOUD_API_ACCOUNT_ID,
        DBT_CLOUD_API_KEY,
    ],
    # Set timeout to 2.5 hours
    env_vars={**env_vars, "DBT_JOB_TIMEOUT": "9000"},
    arguments=["python -m  utils.run_dbt_cloud_job 254981 \"Mattermost Analytics DBT nightly\""],
    dag=dag,
)

user_agent >> dbt_run_cloud >> dbt_run_cloud_nightly >> deferred_merge >> dbt_run_cloud_mattermost_analytics_nightly
