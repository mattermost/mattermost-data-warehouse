import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from dags.airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_and_seed_cmd,
    dbt_install_deps_cmd,
    pod_defaults,
    pod_env_vars,
    xs_warehouse,
)
from dags.kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
pod_env_vars = {**pod_env_vars, **{}}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=8),
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
}

# Create the DAG
dag = DAG("dbt", default_args=default_args, schedule_interval="0 */8 * * *")

# dbt-run
dbt_run_cmd = f"""
    {dbt_install_deps_cmd} &&
    dbt run --profiles-dir profile --target prod --models tag:nightly
"""
dbt_run = KubernetesPodOperator(
    **pod_defaults,
    image=DBT_IMAGE,
    task_id="dbt-run",
    name="dbt-run",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
    ],
    env_vars=pod_env_vars,
    arguments=[dbt_run_cmd],
    dag=dag,
)