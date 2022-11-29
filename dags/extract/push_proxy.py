from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from dags.airflow_utils import pod_defaults, send_alert
from dags.kube_secrets import (
    AWS_ACCOUNT_ID,
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

# Set the command for the container
# Note the {{{{ }}}} is because we format this string but want the resulting string to just have {{ execution_date... }}
CMD_TEMPLATE = """
    python -m extract.s3_extract.push_proxy_job {} {{{{ execution_date.strftime("%Y/%m/%d") }}}}
"""

# Create the DAG
dag = DAG("push_proxy", default_args=default_args, schedule_interval="0 3 * * *")


def get_push_proxy_job(log_type, cmd):
    return KubernetesPodOperator(
        **pod_defaults,
        image="mattermost/mattermost-data-warehouse:master",  # Uses latest build from master
        task_id=f"push-proxy-{log_type}",
        name=f"push-proxy-{log_type}",
        secrets=[
            AWS_ACCOUNT_ID,
            SNOWFLAKE_LOAD_USER,
            SNOWFLAKE_LOAD_PASSWORD,
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_DATABASE,
            SNOWFLAKE_LOAD_WAREHOUSE,
        ],
        env_vars={},
        arguments=[cmd],
        dag=dag,
    )


job = None
for log_type in ["US", "TEST", "DE"]:
    new_job = get_push_proxy_job(log_type.lower(), CMD_TEMPLATE.format(log_type))

    if job is not None:
        job >> new_job

    job = new_job
