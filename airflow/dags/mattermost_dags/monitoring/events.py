from datetime import datetime, timedelta

from mattermost.operators.mattermost_operator import MattermostOperator
from mattermost_dags.airflow_utils import MATTERMOST_DATAWAREHOUSE_IMAGE, cleanup_xcom, pod_defaults, send_alert
from mattermost_dags.kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_USER,
)

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

doc_md = """
### Nightly DBT dag

#### Purpose

This DAG triggers nightly tasks that check for new tables created by Rudderstack.

The following options can be configured using Airflow variables:

- `rudder_schemas` (JSON array of strings) - list of schemas to check.
- `rudder_max_age` (integer) - include tables created in the past number of days defined by this variable.

"""


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
    schedule="0 12 * * *",
    catchup=False,
    max_active_runs=1,  # Don't allow multiple concurrent dag executions
    doc_md=doc_md,
)


def short_circuit_on_no_new_tables(task_id):
    def _do_short_circuit(**kwargs):
        """Short circuit if no new tables"""
        ti = kwargs['ti']
        xcom_result = ti.xcom_pull(task_ids=task_id)
        return bool(xcom_result and xcom_result.get('new_tables', []))

    return _do_short_circuit


def table_formatter(task_id):
    def format_tables(**kwargs):
        """Format result as table"""
        ti = kwargs['ti']
        result = ti.xcom_pull(task_ids=task_id).get('new_tables', [])
        return "\n".join([f' - {table}' for table in result])

    return format_tables


clean_xcom = PythonOperator(
    task_id="cleanup_xcom",
    dag=dag,
    python_callable=cleanup_xcom,
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
                + " --format-json > /airflow/xcom/return.json || true"
            ],
            do_xcom_push=True,
            dag=dag,
        )

        check_output = ShortCircuitOperator(
            task_id=f'short-circuit-{schema}',
            dag=dag,
            python_callable=short_circuit_on_no_new_tables(f"check-new-tables-{schema}"),
        )

        apply_format = PythonOperator(
            task_id=f'apply-format-{schema}',
            dag=dag,
            python_callable=table_formatter(f"check-new-tables-{schema}"),
        )

        alert_op = MattermostOperator(
            mattermost_conn_id='mattermost',
            attachments=[
                {
                    'title': '[Schema '
                    + schema
                    + '] New tables created in the past {{ var.value.rudder_max_age }} days',
                    'color': '#ffcc00',
                    'text': '{{ ti.xcom_pull(task_ids="apply-format-' + schema + '") }}',
                },
            ],
            icon_emoji=':warning:',
            username='Airflow',
            task_id=f"notify-new-tables-{schema}",
            dag=dag,
        )

        op >> check_output >> apply_format >> alert_op

        result.append(op)

    return result


clean_xcom >> get_pod_operators(dag)
