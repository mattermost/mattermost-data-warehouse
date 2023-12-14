from datetime import datetime, timedelta

from mattermost_dags.airflow_utils import MATTERMOST_DATAWAREHOUSE_IMAGE, pod_defaults, send_alert
from mattermost_dags.kube_secrets import (
    DIAGNOSTIC_LOCATION_ONE,
    DIAGNOSTIC_LOCATION_TWO,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# Default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": send_alert,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2019, 1, 1),
}

# Create the DAG
dag = DAG(
    "diagnostics",
    default_args=default_args,
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,  # Don't allow multiple concurrent dag executions
)

KubernetesPodOperator(
    **pod_defaults,
    image=MATTERMOST_DATAWAREHOUSE_IMAGE,
    task_id="diagnostics",
    name="diagnostics",
    secrets=[
        DIAGNOSTIC_LOCATION_ONE,
        DIAGNOSTIC_LOCATION_TWO,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_WAREHOUSE,
    ],
    arguments=["python -m extract.s3_extract.diagnostics_job {{ ds }}"],
    dag=dag,
)
