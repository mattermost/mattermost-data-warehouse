import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from dags.airflow_utils import DATA_IMAGE, clone_repo_cmd, pod_defaults, mm_failed_task
from dags.kube_secrets import (
    HEROKU_POSTGRESQL_URL,
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

def get_container_operator(task_name, job_name):
    cmd = f"""
        {clone_repo_cmd} &&
        export PYTHONPATH="/opt/bitnami/airflow/dags/git/mattermost-data-warehouse/:$PYTHONPATH" &&
        python utils/run_sql.py {job_name}
    """
    pod_operator = KubernetesPodOperator(
        **pod_defaults,
        image=DATA_IMAGE,
        task_id=f"pg-{task_name}",
        name=task_name,
        secrets=[
            HEROKU_POSTGRESQL_URL,
        ],
        arguments=[container_cmd],
        dag=dag,
    )


# Create the DAG
dag = DAG(
    "pg_job", default_args=default_args, schedule_interval="@hourly"
)

test = get_container_operator('test', 'test')