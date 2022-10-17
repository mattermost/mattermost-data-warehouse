import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from dags.airflow_utils import (
    DATA_IMAGE,
    clone_and_setup_extraction_cmd,
    mm_failed_task,
    pod_defaults,
    pod_env_vars,
)
from dags.kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
    GITHUB_TOKEN,
    SSH_KEY,
    DBT_CLOUD_API_ACCOUNT_ID,
    DBT_CLOUD_API_KEY,
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

doc_md = """
### Nightly DBT dag

#### Purpose
This DAG triggers nightly tasks:
- Trigger DBT models with `nightly` tag.
- Update github contributors. 

#### Notes
- DAG was previously running dbt seed. This option has been removed.
"""

# Create the DAG
dag = DAG("dbt_nightly", default_args=default_args, schedule_interval="0 7 * * *", doc_md=doc_md)

update_github_contributors_cmd = f"""
    {clone_and_setup_extraction_cmd} &&
    python utils/github_contributors.py
"""

update_github_contributors = KubernetesPodOperator(
    **pod_defaults,
    image=DATA_IMAGE,
    task_id="github-contributors",
    name="github-contributors",
    secrets=[
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        GITHUB_TOKEN,
    ],
    env_vars=env_vars,
    arguments=[update_github_contributors_cmd],
    dag=dag,
)


dbt_run_cloud_nightly_cmd = f"""
    {clone_and_setup_extraction_cmd} &&
    python utils/run_dbt_cloud_job.py 19427 "Airflow dbt nightly"
"""

dbt_run_cloud_nightly = KubernetesPodOperator(
    **pod_defaults,
    image=DATA_IMAGE,
    task_id="dbt-cloud-run-nightly",
    name="dbt-cloud-run-nightly",
    secrets=[
        DBT_CLOUD_API_ACCOUNT_ID,
        DBT_CLOUD_API_KEY,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
        SSH_KEY,
    ],
    env_vars={
        **env_vars,
        "DBT_JOB_TIMEOUT": "3600"
    },
    arguments=[dbt_run_cloud_nightly_cmd],
    dag=dag,
)


dbt_run_cloud_nightly
