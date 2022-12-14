from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from dags.airflow_utils import MATTERMOST_DATAWAREHOUSE_IMAGE, pod_defaults, send_alert
from dags.kube_secrets import (
    DOCS_WEBHOOK_URL,
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
    "depends_on_past": False,
    "owner": "airflow",
    "on_failure_callback": send_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=8),
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
}

# Create the DAG
dag = DAG(
    "weekly_job",
    default_args=default_args,
    schedule_interval="0 12 * * 1",
    catchup=False,
    max_active_runs=1,  # Don't allow multiple concurrent dag executions
)

post_docs_feedback = KubernetesPodOperator(
    **pod_defaults,
    image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
    task_id="post-docs-feedback",
    name="post-docs-feedback",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
        SNOWFLAKE_TRANSFORM_DATABASE,
        DOCS_WEBHOOK_URL,
    ],
    arguments=["python -m utils.post_docs_data"],
    dag=dag,
)

post_docs_feedback
