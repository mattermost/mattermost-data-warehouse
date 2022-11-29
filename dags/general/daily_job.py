from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from dags.airflow_utils import pod_defaults, send_alert
from dags.kube_secrets import (
    NPS_WEBHOOK_URL,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_DATABASE,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
)

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "owner": "airflow",
    "on_failure_callback": send_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=8),
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
}

# Create the DAG
dag = DAG("daily_job", default_args=default_args, schedule_interval="0 12 * * *")

post_nps_feedback = KubernetesPodOperator(
    **pod_defaults,
    image="mattermost/mattermost-data-warehouse:master",  # Uses latest build from master
    task_id="post-nps-feedback",
    name="post-nps-feedback",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
        SNOWFLAKE_TRANSFORM_DATABASE,
        NPS_WEBHOOK_URL,
    ],
    arguments=["python -m utils.post_nps_data.py"],
    dag=dag,
)

post_nps_feedback
