from datetime import datetime, timedelta

from mattermost_dags.airflow_utils import MATTERMOST_DATAWAREHOUSE_IMAGE, pod_defaults, pod_env_vars, send_alert
from mattermost_dags.kube_secrets import DBT_CLOUD_API_ACCOUNT_ID, DBT_CLOUD_API_KEY

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

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
    "default_timeout": timedelta(hours=1),
}

# Create the DAG
dag = DAG(
    "dbt",
    default_args=default_args,
    schedule="5 * * * *",
    catchup=False,
    max_active_runs=1,  # Don't allow multiple concurrent dag executions
)


dbt_run_cloud_mattermost_analytics_hourly = KubernetesPodOperator(
    **pod_defaults,
    image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
    task_id="dbt-cloud-mattermost-analytics-hourly",
    name="dbt-cloud-mattermost-analytics-hourly",
    secrets=[
        DBT_CLOUD_API_ACCOUNT_ID,
        DBT_CLOUD_API_KEY,
    ],
    env_vars=env_vars,
    arguments=["python -m  utils.run_dbt_cloud_job 215330 \"Mattermost Analytics DBT hourly\""],
    dag=dag,
)

dbt_run_cloud_mattermost_analytics_hourly
