from datetime import datetime, timedelta

from mattermost_dags.airflow_utils import MATTERMOST_DATAWAREHOUSE_IMAGE, pod_defaults, send_alert
from mattermost_dags.kube_secrets import (
    NPS_WEBHOOK_URL,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_DATABASE,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
)

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

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
    "nps_daily_feedback",
    default_args=default_args,
    schedule="0 12 * * *",
    catchup=False,
    max_active_runs=1,  # Don't allow multiple concurrent dag executions
    doc_md="""
### Daily NPS Feedback

#### Purpose

This DAG updates NPS feedback and sends it to the appropriate channel once per day. It runs the following tasks:
- Calculate categories of feedback.
- Post NPS feedback to Mattermost channel.

#### Notes
- DAG was previously split in two parts, that resulted in data not being available.
""",
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


post_nps_feedback = KubernetesPodOperator(
    **pod_defaults,
    image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
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
    arguments=["python -m utils.post_nps_data"],
    dag=dag,
)

automated_nps_feedback_category_update >> nps_subcategory_updates >> nps_category_updates >> post_nps_feedback
