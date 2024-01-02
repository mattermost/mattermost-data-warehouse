import os
from datetime import datetime, timedelta

from mattermost_dags.airflow_utils import MATTERMOST_DATAWAREHOUSE_IMAGE, PERMIFROST_IMAGE, send_alert

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

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
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,  # Don't allow multiple concurrent dag executions
    doc_md=doc_md,
)

# Dummy tasks that just runs a random command in order to download the images if needed.
download_warehouse_image = KubernetesPodOperator(
    get_logs=True,
    image_pull_policy="Always",
    in_cluster=True,
    on_finish_action="delete_pod",
    namespace=os.environ["NAMESPACE"],
    cmds=["/bin/bash", "-c"],
    image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
    task_id="download-image",
    name="download-image",
    arguments=["date"],
    dag=dag,
)

# Dummy tasks that just runs a random command in order to download the images if needed.
download_permifrost_image = KubernetesPodOperator(
    get_logs=True,
    image_pull_policy="Always",
    in_cluster=True,
    on_finish_action="delete_pod",
    namespace=os.environ["NAMESPACE"],
    cmds=["/bin/bash", "-c"],
    image=PERMIFROST_IMAGE,  # Uses latest build from master
    task_id="download-permifrost-image",
    name="download-permifrost-image",
    arguments=["date"],
    dag=dag,
)

download_warehouse_image >> download_permifrost_image
