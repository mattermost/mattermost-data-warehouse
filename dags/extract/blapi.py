import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from dags.airflow_utils import (
    PIPELINEWISE_IMAGE,
    mm_failed_task,
    pod_defaults,
    pod_env_vars,
)
from dags.kube_secrets import PIPELINEWISE_SECRETS

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
dag = DAG("blapi", default_args=default_args, schedule_interval="0 * * * *")

volume_config = {"persistentVolumeClaim": {"claimName": "pipelinewise-pv"}}
volume = Volume(name="pipelinewise-volume", configs=volume_config)
volume_mount = VolumeMount(
    "pipelinewise-volume",
    mount_path="/app/.pipelinewise",
    sub_path=None,
    read_only=False,
)

del pod_defaults["cmds"]

blapi = KubernetesPodOperator(
    **pod_defaults,
    image=PIPELINEWISE_IMAGE,
    task_id="blapi-import",
    name="blapi-import",
    secrets=[PIPELINEWISE_SECRETS,],
    arguments=["run_tap", "--tap", "blapi", "--target", "snowflake"],
    volumes=[volume],
    volume_mounts=[volume_mount],
    startup_timeout_seconds=300,
    dag=dag,
)
