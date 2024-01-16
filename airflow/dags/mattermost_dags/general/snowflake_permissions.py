from datetime import datetime, timedelta

from mattermost_dags.airflow_utils import PERMIFROST_IMAGE, pod_defaults, send_alert
from mattermost_dags.kube_secrets import (
    PERMISSION_BOT_ACCOUNT,
    PERMISSION_BOT_DATABASE,
    PERMISSION_BOT_PASSWORD,
    PERMISSION_BOT_ROLE,
    PERMISSION_BOT_USER,
    PERMISSION_BOT_WAREHOUSE,
)

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": send_alert,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2019, 1, 1),
}

# Set the command for the container
container_cmd = """
    permifrost run load/snowflake/roles.yaml
"""

# Create the DAG
dag = DAG("snowflake_permissions", default_args=default_args, schedule="0 0 * * *")

# Task 1
snowflake_load = KubernetesPodOperator(
    **pod_defaults,
    image=PERMIFROST_IMAGE,
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
    arguments=[container_cmd],
    dag=dag,
)
