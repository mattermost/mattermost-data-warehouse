""" This file contains common operators/functions to be used across multiple DAGs """
import os
import urllib.parse

from airflow.contrib.kubernetes.pod import Resources

from plugins.operators.mattermost_operator import MattermostOperator

PERMIFROST_IMAGE = "mattermost/mattermost-permifrost:master"
PIPELINEWISE_IMAGE = "docker.io/adovenmm/pipelinewise:latest"

MATTERMOST_DATAWAREHOUSE_IMAGE = "mattermost/mattermost-data-warehouse:master"

mm_webhook_url = os.getenv("MATTERMOST_WEBHOOK_URL")
DEFAULT_AIRFLOW_NAMESPACE = "airflow-dev"  # to prevent tests from failing

# Set the resources for the task pods
pod_resources = Resources(request_memory="500Mi", request_cpu="500m")

# Default settings for all DAGs
pod_defaults = {
    "get_logs": True,
    "image_pull_policy": "IfNotPresent",
    "in_cluster": True,
    "is_delete_operator_pod": True,
    "namespace": os.environ.get('NAMESPACE', DEFAULT_AIRFLOW_NAMESPACE),
    "cmds": ["/bin/bash", "-c"],
}

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
    base_url = "https://airflow.internal.mattermost.com"
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
