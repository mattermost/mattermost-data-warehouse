import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from dags.airflow_utils import MATTERMOST_DATAWAREHOUSE_IMAGE, send_alert

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

doc_md = """
### CI/CD DAG

#### Purpose

This DAG attempts to load the latest `master` version of the image on each run. 

> This is a workaround in order to reduce the load from pulling new images. The trick is that this is the only 
> DAG that will `Always` try to download the new image.
 """

# Create the DAG
dag = DAG(
    "cicd",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,  # Don't allow multiple concurrent dag executions
    doc_md=doc_md,
)

# Dummy task that just runs a random command.
dummy = KubernetesPodOperator(
    get_logs=True,
    image_pull_policy="Always",
    in_cluster=True,
    is_delete_operator_pod=True,
    namespace=os.environ["NAMESPACE"],
    cmds=["/bin/bash", "-c"],
    image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
    task_id="download-image",
    name="download-image",
    arguments=["date"],
    dag=dag,
)