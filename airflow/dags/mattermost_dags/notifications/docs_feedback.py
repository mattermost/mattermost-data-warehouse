from datetime import datetime

from mattermost_dags.airflow_utils import MATTERMOST_DATAWAREHOUSE_IMAGE, pod_defaults, send_alert
from mattermost_dags.kube_secrets import (
    DOCS_FEEDBACK_WEBHOOK_URL,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_DATABASE,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
)

from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


@dag(
    schedule="0 12 * * 1",
    catchup=False,
    max_active_runs=1,  # Don't allow multiple concurrent dag executions
    start_date=datetime(2019, 1, 1, 0, 0, 0),
    on_failure_callback=send_alert,
)
def docs_feedback():
    """
    ### Docs Feedback

    #### Purpose

    This DAG triggers a weekly job that posts to a Mattermost channel any feedback that was submitted via the
    documentation feedback form in the previous 7 days (not including today).

    The following options can be configured using Airflow variables:

    - `docs_feedback_channel` (string) - the channel used .
    """

    post_docs_feedback = KubernetesPodOperator(
        **pod_defaults,
        image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
        task_id="post-docs-feedback",
        name="post-docs-feedback",
        secrets=[
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_USER,
            SNOWFLAKE_PASSWORD,
            SNOWFLAKE_TRANSFORM_WAREHOUSE,
            SNOWFLAKE_TRANSFORM_DATABASE,
            SNOWFLAKE_TRANSFORM_SCHEMA,
            SNOWFLAKE_TRANSFORM_ROLE,
            DOCS_FEEDBACK_WEBHOOK_URL,
        ],
        arguments=[
            "snowflake "
            " -a ${SNOWFLAKE_ACCOUNT}"
            " -d ${SNOWFLAKE_TRANSFORM_DATABASE}"
            " -w ${SNOWFLAKE_TRANSFORM_WAREHOUSE}"
            " -s ${SNOWFLAKE_TRANSFORM_SCHEMA}"
            " -u ${SNOWFLAKE_USER}"
            " -p ${SNOWFLAKE_PASSWORD}"
            " -r ${SNOWFLAKE_TRANSFORM_ROLE}"
            " post-query feedback ${DOCS_FEEDBACK_WEBHOOK_URL} {{ var.value.docs_feedback_channel }}"
        ],
    )

    post_docs_feedback


dag = docs_feedback()
