""" This file contains common operators/functions to be used across multiple DAGs """
import os
import urllib.parse

from airflow.models import XCom
from airflow.utils.db import provide_session

from kubernetes.client import models as k8s
from operators.mattermost_operator import MattermostOperator

PERMIFROST_IMAGE = "mattermost/mattermost-permifrost:master"

MATTERMOST_DATAWAREHOUSE_IMAGE = "mattermost/mattermost-data-warehouse:master"

mm_webhook_url = os.getenv("MATTERMOST_WEBHOOK_URL")
DEFAULT_AIRFLOW_NAMESPACE = "airflow-dev"  # to prevent tests from failing
IS_DEV_MODE = os.getenv("ENVIRONMENT") == 'docker-compose'


# Default settings for all DAGs
pod_defaults = {
    "get_logs": True,
    "image_pull_policy": "IfNotPresent",
    "in_cluster": True,
    "is_delete_operator_pod": True,
    "namespace": os.environ.get('NAMESPACE', DEFAULT_AIRFLOW_NAMESPACE),
    "cmds": ["/bin/bash", "-c"],
    "container_resources": k8s.V1ResourceRequirements(
        requests={
            "cpu": "500m",
            "memory": "500Mi",
        },
        limits={
            "cpu": "1000m",
            "memory": "1000Mi",
        },
    )
}

if IS_DEV_MODE:
    # Override defaults for dev mode
    pod_defaults["in_cluster"] = False
    pod_defaults["config_file"] = "/opt/kube/airflow-kube.yaml"

# Default environment variables for worker pods
env = os.environ.copy()
pod_env_vars = {
    "CI_PROJECT_DIR": "/analytics",
    "EXECUTION_DATE": "{{ next_execution_date }}",
    "SNOWFLAKE_LOAD_DATABASE": "RAW",
    "SNOWFLAKE_TRANSFORM_DATABASE": "ANALYTICS",
}

# Warehouse variable declaration
xs_warehouse = "'{{warehouse_name: transforming_xs}}'"

clone_repo_cmd = "git clone -b master --single-branch --depth 1 https://github.com/mattermost/mattermost-data-warehouse"


def create_alert_body(context):
    """
    Creates post body to be sent to mattermost channel.
    """
    base_url = "https://airflow.dataeng.internal.mattermost.com"
    execution_date = context["ts"]
    dag_context = context["dag"]
    dag_name = dag_context.dag_id
    dag_id = context["dag"].dag_id
    task_name = context["task"].task_id
    task_id = context["task_instance"].task_id
    error_message = str(context["exception"])

    # Generate the link to the task
    task_params = urllib.parse.urlencode({"dag_id": dag_id, "task_id": task_id, "execution_date": execution_date})
    task_link = f"{base_url}/task?{task_params}"
    dag_link = f"{base_url}/tree?dag_id={dag_id}"

    # TODO create templates for other alerts
    body = f""":red_circle: {error_message}\n**Dag**: [{dag_name}]({dag_link})\n**Task**: [{task_name}]({task_link})"""
    return body


def send_alert(context):
    """
    Function to be used as a callable for on_failure_callback.
    Sends a post to mattermost channel using mattermost operator
    """
    task_id = str(context["task_instance"].task_id) + '_failed_alert'
    MattermostOperator(
        mattermost_conn_id='mattermost', text=create_alert_body(context), username='Airflow', task_id=task_id
    ).execute(context)


# To clean up Xcom after dag finished run.
@provide_session
def cleanup_xcom(**context):
    dag = context["dag"]
    dag_id = dag._dag_id
    session = context["session"]
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()
