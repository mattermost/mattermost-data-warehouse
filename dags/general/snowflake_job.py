import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from dags.airflow_utils import (
    DATA_IMAGE,
    clone_repo_cmd,
    mm_failed_task,
    pod_defaults,
    pod_env_vars,
    xs_warehouse,
)
from dags.kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_TRANSFORM_DATABASE,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
env_vars = {**pod_env_vars, **{}}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "owner": "airflow",
    "on_failure_callback": mm_failed_task,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=8),
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
}

# Create the DAG
dag = DAG("snowflake_job", default_args=default_args, schedule_interval="*/5 * * * *")


def get_container_operator(task_name, job_name, schema):
    cmd = f"""
        {clone_repo_cmd} &&
        cd mattermost-data-warehouse &&
        export PYTHONPATH="/opt/bitnami/airflow/dags/git/mattermost-data-warehouse/:$PYTHONPATH" &&
        python utils/run_snowflake_queries.py {job_name} TRANSFORMER {schema}
    """
    return KubernetesPodOperator(
        **pod_defaults,
        image=DATA_IMAGE,
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
        arguments=[cmd],
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
