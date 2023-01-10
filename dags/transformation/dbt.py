from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from dags.airflow_utils import MATTERMOST_DATAWAREHOUSE_IMAGE, pod_defaults, pod_env_vars, send_alert
from dags.kube_secrets import (
    CLEARBIT_KEY,
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

# Create the DAG
dag = DAG(
    "dbt",
    default_args=default_args,
    schedule_interval="5 * * * *",
    catchup=False,
    max_active_runs=1,  # Don't allow multiple concurrent dag executions
)

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

dbt_run_cloud = KubernetesPodOperator(
    **pod_defaults,
    image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
    task_id="dbt-cloud-run",
    name="dbt-cloud-run",
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
    env_vars=env_vars,
    arguments=["python -m  utils.run_dbt_cloud_job 19444 \"Airflow dbt hourly\""],
    dag=dag,
)


update_clearbit = KubernetesPodOperator(
    **pod_defaults,
    image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
    task_id="update-clearbit",
    name="update-clearbit",
    secrets=[
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        CLEARBIT_KEY,
    ],
    env_vars=env_vars,
    arguments=["python -m utils.cloud_clearbit"],
    dag=dag,
)

update_onprem_clearbit = KubernetesPodOperator(
    **pod_defaults,
    image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
    task_id="update-onprem-clearbit",
    name="update-onprem-clearbit",
    secrets=[
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        CLEARBIT_KEY,
    ],
    env_vars=env_vars,
    arguments=["python -m utils.onprem_clearbit"],
    dag=dag,
)

user_agent >> dbt_run_cloud >> update_clearbit >> update_onprem_clearbit
