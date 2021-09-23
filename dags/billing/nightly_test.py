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
    xs_warehouse,
)
from dags.kube_secrets import BLAPI_TEST_DATABASE_URL, BLAPI_TEST_TOKEN, BLAPI_TEST_URL

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
    "start_date": datetime(2020, 10, 16, 0, 0, 0),
}

# We purposely don't run this job on the first day of the month, because stripe will
# create the draft invoices for the previous month and we handle that in blapi directly
dag = DAG(
    "test_env_nightly_billing", default_args=default_args, schedule_interval="0 1 2-31 * *"
)


run_nightly_billing_cmd = f"""
    {clone_and_setup_extraction_cmd} &&
    python billing/run_nightly_billing.py test
"""

run_nightly_billing = KubernetesPodOperator(
    **pod_defaults,
    image=DATA_IMAGE,
    task_id="test-env-run-nightly-billing",
    name="test-env-run-nightly-billing",
    secrets=[BLAPI_TEST_DATABASE_URL, BLAPI_TEST_TOKEN, BLAPI_TEST_URL],
    env_vars=env_vars,
    arguments=[run_nightly_billing_cmd],
    dag=dag,
    is_delete_operator_pod=False,
)