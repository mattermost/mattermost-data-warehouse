from datetime import datetime, timedelta

from mattermost_dags.airflow_utils import MATTERMOST_DATAWAREHOUSE_IMAGE, pod_defaults, pod_env_vars, send_alert
from mattermost_dags.kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_DATABASE,
    SNOWFLAKE_TRANSFORM_LARGE_WAREHOUSE,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_USER,
)

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# Load the env vars into a dict and set Secrets
env_vars = {**pod_env_vars, **{}}

# Default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "owner": "airflow",
    "on_failure_callback": send_alert,
    "sla": timedelta(hours=8),
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
}

doc_md = """
### Deferred merge

#### Purpose
This DAG triggers deferred merge. It merges the event delta table into the base table.

This DAG does not have a schedule. It can be triggered manually in Airflow UI.
"""


with DAG(
    "manual_deferred_merge",
    default_args=default_args,
    catchup=False,
    schedule=None,  # Don't schedule this DAG, trigger manually
    max_active_runs=1,  # Don't allow multiple concurrent dag executions
    doc_md=doc_md,
) as dag:
    manual_deferred_merge = KubernetesPodOperator(
        **pod_defaults,
        image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
        task_id="deferred-merge",
        name="deferred-merge",
        secrets=[
            SNOWFLAKE_USER,
            SNOWFLAKE_PASSWORD,
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_TRANSFORM_ROLE,
            SNOWFLAKE_TRANSFORM_LARGE_WAREHOUSE,
            SNOWFLAKE_TRANSFORM_DATABASE,
        ],
        env_vars=env_vars,
        arguments=[
            "snowflake "
            " -a ${SNOWFLAKE_ACCOUNT}"
            " -u ${SNOWFLAKE_USER}"
            " -p ${SNOWFLAKE_PASSWORD}"
            " -d ${SNOWFLAKE_TRANSFORM_DATABASE}"
            " -s {{ var.value.rudderstack_support_schema }}"
            " -w ${SNOWFLAKE_TRANSFORM_LARGE_WAREHOUSE}"
            " -r ${SNOWFLAKE_TRANSFORM_ROLE}"
            " merge {{ var.value.base_events_table }} "
            " {{ var.value.base_events_delta_schema }} {{ var.value.base_events_delta_table }}"
        ],
        dag=dag,
    )


manual_deferred_merge
