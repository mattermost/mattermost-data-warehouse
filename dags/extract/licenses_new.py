import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from dags.airflow_utils import send_alert
from dags.kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)

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


# Create the DAG
dag = DAG("licenses-new", default_args=default_args, schedule_interval="0 3 * * *")


KubernetesPodOperator(
    # Sensible pod defaults
    get_logs=True,
    image_pull_policy="IfNotPresent",
    in_cluster=True,
    is_delete_operator_pod=True,
    namespace=os.environ["NAMESPACE"],
    image="mattermost/mattermost-data-warehouse:master",  # Uses latest build from master
    # Task specific information
    task_id="licenses_new",
    name="license-import-new",
    secrets=[
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_WAREHOUSE,
    ],
    arguments=["extract.s3_extract.licenses_job {{ next_ds }}"],
    dag=dag,
)
