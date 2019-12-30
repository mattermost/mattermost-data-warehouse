import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from dags.airflow_utils import DATA_IMAGE, clone_repo_cmd, pod_defaults, mm_failed_task
from dags.kube_secrets import (
    AWS_ACCOUNT_ID,
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

# Set the command for the container
# Note the {{{{ }}}} is because we format this string but want the resulting string to just have {{ execution_date... }}
container_cmd = """
    {} &&
    cd dbt &&
    python extract/s3_extract/push_proxy_job.py --log-type {} --date {{{{ execution_date.strftime("%Y/%m/%d") }}}}
"""

# Create the DAG
dag = DAG(
    "push_proxy", default_args=default_args, schedule_interval="0 4 * * *"
)


def get_push_proxy_job(log_type, cmd):
    return KubernetesPodOperator(
        **pod_defaults,
        image=DATA_IMAGE,
        task_id=f"push-proxy-{log_type}",
        name=f"push-proxy-{log_type}",
        secrets=[
            AWS_ACCOUNT_ID,
            SNOWFLAKE_LOAD_USER,
            SNOWFLAKE_LOAD_PASSWORD,
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_DATABASE,
            SNOWFLAKE_LOAD_WAREHOUSE,
        ],
        env_vars={},
        arguments=[cmd],
        dag=dag,
    )


job = None
for log_type in ["US", "TEST", "EU"]:
    new_job = get_push_proxy_job(log_type, container_cmd.format(clone_repo_cmd, log_type))

    if job is not None:
        job << new_job
        job = new_job

