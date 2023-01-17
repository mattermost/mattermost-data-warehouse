""" This file contains common operators/functions to be used across multiple DAGs """
import json
import os
import urllib.parse
from datetime import date, timedelta
from typing import List

from airflow.contrib.kubernetes.pod import Resources

from plugins.operators.mattermost_operator import MattermostOperator

PERMIFROST_IMAGE = "docker.io/adovenmm/permifrost-image:latest"
PSQL_IMAGE = "docker.io/adovenmm/data-warehouse-psql:latest"
PIPELINEWISE_IMAGE = "docker.io/adovenmm/pipelinewise:latest"

MATTERMOST_DATAWAREHOUSE_IMAGE = "mattermost/mattermost-data-warehouse:master"

mm_webhook_url = os.getenv("MATTERMOST_WEBHOOK_URL")
DEFAULT_AIRFLOW_NAMESPACE = "airflow-dev"  # to prevent tests from failing


def mm_failed_task(context):
    """
    Function to be used as a callable for on_failure_callback.
    Send a Mattermost alert.
    """
    if mm_webhook_url is None or os.getenv("AIRFLOW_BASE_URL") != "https://airflow.internal.mattermost.com":
        return

    # Set all of the contextual vars
    base_url = "https://airflow.internal.mattermost.com/"
    execution_date = context["ts"]
    dag_context = context["dag"]
    dag_name = dag_context.dag_id
    dag_id = context["dag"].dag_id
    task_name = context["task"].task_id
    task_id = context["task_instance"].task_id
    execution_date_pretty = context["execution_date"].strftime("%a, %b %d, %Y at %-I:%M %p UTC")

    # Generate the link to the logs
    log_params = urllib.parse.urlencode({"dag_id": dag_id, "task_id": task_id, "execution_date": execution_date})
    log_link = f"{base_url}/log?{log_params}"
    log_link_markdown = f"[View Logs]({log_link})"

    body = f"""| DAG | Task | Logs | Timestamp |
        |-----|------|------|-----------|
        |{dag_name}|{task_name}|{log_link_markdown}|{execution_date_pretty}|"""

    payload = {"username": "Airflow", "text": body}

    os.system(
        f"""curl -i -X POST {mm_webhook_url} -H 'Content-Type: application/json' \
        --data-binary @- <<'EOF'
            {json.dumps(payload)}
        EOF
    """
    )


def split_date_parts(day: date, partition: str) -> List[dict]:

    if partition == "month":
        split_dict = {
            "year": day.strftime("%Y"),
            "month": day.strftime("%m"),
            "part": day.strftime("%Y_%m"),
        }

    return split_dict


def partitions(from_date: date, to_date: date, partition: str) -> List[dict]:
    """
    A list of partitions to build.
    """

    delta = to_date - from_date
    all_parts = [split_date_parts((from_date + timedelta(days=i)), partition) for i in range(delta.days + 1)]

    seen = set()
    parts = []
    # loops through every day and pulls out unique set of date parts
    for p in all_parts:
        if p["part"] not in seen:
            seen.add(p["part"])
            parts.append({k: v for k, v in p.items()})
    return parts


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
