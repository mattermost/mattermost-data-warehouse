import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from dags.airflow_utils import DATA_IMAGE, clone_and_setup_extraction_cmd, pod_defaults, mm_failed_task
from dags.kube_secrets import (
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_WAREHOUSE,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": mm_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2019, 1, 1),
}

container_cmd = f"""
    {clone_and_setup_extraction_cmd} &&
    python extract/s3_extract/licenses_job.py {{{{ next_ds }}}}
"""

# Create the DAG
dag = DAG(
    "licenses", default_args=default_args, schedule_interval="0 3 * * *"
)

KubernetesPodOperator(
    **pod_defaults,
    image=DATA_IMAGE,
    task_id="licenses",
    name="license-import",
    secrets=[
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_WAREHOUSE,
    ],
    env_vars={},
    arguments=[container_cmd],
    dag=dag,
)
