from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import Variable

from dags.airflow_utils import MATTERMOST_DATAWAREHOUSE_IMAGE, pod_defaults, send_alert
from dags.kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_USER,
)
from plugins.operators.mattermost_operator import MattermostOperator

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
    "event_table_monitoring",
    default_args=default_args,
    schedule_interval="0 12 * * *",
    catchup=False,
    max_active_runs=1,  # Don't allow multiple concurrent dag executions
)


def get_pod_operators(dag):
    result = []
    schemas = Variable.get("rudder_schemas", deserialize_json=True)

    for schema in schemas:
        op = KubernetesPodOperator(
            **pod_defaults,
            image=MATTERMOST_DATAWAREHOUSE_IMAGE,
            task_id=f"check-new-tables-{schema}",
            name=f"check-new-tables-{schema}",
            secrets=[
                SNOWFLAKE_ACCOUNT,
                SNOWFLAKE_USER,
                SNOWFLAKE_PASSWORD,
                SNOWFLAKE_LOAD_ROLE,
                SNOWFLAKE_LOAD_WAREHOUSE,
                SNOWFLAKE_LOAD_DATABASE,
            ],
            arguments=[
                "rudder list ${SNOWFLAKE_LOAD_DATABASE} "
                + schema
                + " -w ${SNOWFLAKE_LOAD_WAREHOUSE} -r ${SNOWFLAKE_LOAD_ROLE} --max-age {{ var.value.rudder_max_age }}"
            ],
            dag=dag,
        )
        failure_op = MattermostOperator(
            mattermost_conn_id='mattermost',
            text=f'New event tables detected in schema {schema}',
            username='Airflow',
            task_id=f"check-new-tables-{schema}-handle-failure",
            dag=dag,
        )
        op >> failure_op
        result.append(op)

    return result


pod_operators = get_pod_operators(dag)
pod_operators
