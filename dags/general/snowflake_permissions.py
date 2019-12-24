import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from dags.airflow_utils import DATA_IMAGE, clone_repo_cmd, pod_defaults, mm_failed_task
from dags.kube_secrets import (
    PERMISSION_BOT_ACCOUNT,
    PERMISSION_BOT_DATABASE,
    PERMISSION_BOT_PASSWORD,
    PERMISSION_BOT_ROLE,
    PERMISSION_BOT_USER,
    PERMISSION_BOT_WAREHOUSE,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
pod_env_vars = {"DRY": "--dry"}

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

# Set the command for the container
container_cmd = f"""
    {clone_repo_cmd} &&
    meltano init airflow_job &&
    cp dbt/load/snowflake/roles.yaml airflow_job/load/roles.yml &&
    cd airflow_job/ &&
    meltano permissions grant load/roles.yml --db snowflake $DRY
"""

# Create the DAG
dag = DAG(
    "snowflake_permissions", default_args=default_args, schedule_interval="0 0 */1 * *"
)

# Task 1
snowflake_load = KubernetesPodOperator(
    **pod_defaults,
    image=DATA_IMAGE,
    task_id="snowflake-permissions",
    name="snowflake-permissions",
    secrets=[
        PERMISSION_BOT_USER,
        PERMISSION_BOT_PASSWORD,
        PERMISSION_BOT_ACCOUNT,
        PERMISSION_BOT_ROLE,
        PERMISSION_BOT_DATABASE,
        PERMISSION_BOT_WAREHOUSE,
    ],
    env_vars=pod_env_vars,
    arguments=[container_cmd],
    dag=dag,
)
