from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from dags.airflow_utils import MATTERMOST_DATAWAREHOUSE_IMAGE, pod_defaults, send_alert
from dags.kube_secrets import (
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
    "snowflake_job",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,  # Don't allow multiple concurrent dag executions
)


def get_container_operator(task_name, job_name, schema):
    return KubernetesPodOperator(
        **pod_defaults,
        image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
        task_id=f"snowflake-{task_name}",
        name=task_name,
        secrets=[
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_USER,
            SNOWFLAKE_PASSWORD,
            SNOWFLAKE_TRANSFORM_ROLE,
            SNOWFLAKE_TRANSFORM_WAREHOUSE,
            SNOWFLAKE_TRANSFORM_SCHEMA,
            SNOWFLAKE_TRANSFORM_DATABASE,
        ],
        arguments=[f"python -m utils.run_snowflake_queries {job_name} TRANSFORMER {schema}"],
        dag=dag,
    )


automated_nps_feedback_category_update = get_container_operator(
    "automated-nps-feedback-category-update", "automated_nps_feedback_category_update", "mattermost"
)

nps_category_updates = get_container_operator(
    "nps-category-updates", "data_action_nps_feedback_category_update", "mattermost"
)

nps_subcategory_updates = get_container_operator(
    "nps-subcategory-updates", "data_action_nps_feedback_subcategory_update", "mattermost"
)

automated_nps_feedback_category_update >> nps_subcategory_updates >> nps_category_updates
