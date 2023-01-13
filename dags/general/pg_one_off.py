from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from dags.airflow_utils import PSQL_IMAGE, clone_repo_cmd, pod_defaults, send_alert
from dags.kube_secrets import HEROKU_POSTGRESQL_URL

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


def get_container_operator(task_name, job_name):
    cmd = f"""
        {clone_repo_cmd} &&
        cd mattermost-data-warehouse &&
        export PYTHONPATH="/opt/bitnami/airflow/dags/git/mattermost-data-warehouse/:$PYTHONPATH" &&
        python utils/run_sql.py {job_name}
    """
    return KubernetesPodOperator(
        **pod_defaults,
        image=PSQL_IMAGE,
        task_id=f"pg-{task_name}",
        name=task_name,
        secrets=[
            HEROKU_POSTGRESQL_URL,
        ],
        arguments=[cmd],
        dag=dag,
    )


# Create the DAG
dag = DAG("pg_one_off", default_args=default_args, schedule_interval="@daily")
