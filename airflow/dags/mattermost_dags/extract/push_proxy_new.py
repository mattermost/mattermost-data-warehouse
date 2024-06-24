import pendulum
from mattermost_dags.airflow_utils import MATTERMOST_DATAWAREHOUSE_IMAGE, pod_defaults, send_alert
from mattermost_dags.kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)

from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


@dag(
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    on_failure_callback=send_alert,
)
def push_proxy_loader():
    """
    ### Push proxy DAG

    Loads ALB logs to Snowflake.
    """

    extract_hpns_us_logs = KubernetesPodOperator(
        **pod_defaults,
        image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
        task_id="task-push-proxy-us",
        name="push-proxy-us",
        secrets=[
            SNOWFLAKE_LOAD_USER,
            SNOWFLAKE_LOAD_PASSWORD,
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_DATABASE,
            SNOWFLAKE_LOAD_WAREHOUSE,
            SNOWFLAKE_LOAD_ROLE,
        ],
        env_vars={},
        arguments=[
            "push_proxy PUSH_PROXY_LOGS_US_NEW LOGS_US_NEW "
            " --prefix /AWSLogs/{{ var.value.push_proxy_aws_account_id }}/elasticloadbalancing/{{ var.value.push_proxy_aws_region }}"  # noqa: E501
            " -s {{ var.value.push_proxy_target_schema }}"
            " -a ${SNOWFLAKE_ACCOUNT}"
            " -d ${SNOWFLAKE_LOAD_DATABASE}"
            " -w ${SNOWFLAKE_LOAD_WAREHOUSE}"
            " -r ${SNOWFLAKE_LOAD_ROLE}"
            " -u ${SNOWFLAKE_LOAD_USER}"
            " -p ${SNOWFLAKE_LOAD_PASSWORD}"
        ],
    )

    extract_tpns_logs = KubernetesPodOperator(
        **pod_defaults,
        image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
        task_id="task-push-proxy-tests",
        name="push-proxy-test",
        secrets=[
            SNOWFLAKE_LOAD_USER,
            SNOWFLAKE_LOAD_PASSWORD,
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_DATABASE,
            SNOWFLAKE_LOAD_WAREHOUSE,
            SNOWFLAKE_LOAD_ROLE,
        ],
        env_vars={},
        arguments=[
            "push_proxy PUSH_PROXY_LOGS_TEST_NEW LOGS_TEST_NEW "
            " --prefix /AWSLogs/{{ var.value.push_proxy_aws_account_id }}/elasticloadbalancing/{{ var.value.push_proxy_aws_region }}"  # noqa: E501
            " -s {{ var.value.push_proxy_target_schema }}"
            " -a ${SNOWFLAKE_ACCOUNT}"
            " -d ${SNOWFLAKE_LOAD_DATABASE}"
            " -w ${SNOWFLAKE_LOAD_WAREHOUSE}"
            " -r ${SNOWFLAKE_LOAD_ROLE}"
            " -u ${SNOWFLAKE_LOAD_USER}"
            " -p ${SNOWFLAKE_LOAD_PASSWORD}"
        ],
    )

    extract_hpns_eu_logs = KubernetesPodOperator(
        **pod_defaults,
        image=MATTERMOST_DATAWAREHOUSE_IMAGE,  # Uses latest build from master
        task_id="task-push-proxy-eu",
        name="push-proxy-eu",
        secrets=[
            SNOWFLAKE_LOAD_USER,
            SNOWFLAKE_LOAD_PASSWORD,
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_DATABASE,
            SNOWFLAKE_LOAD_WAREHOUSE,
            SNOWFLAKE_LOAD_ROLE,
        ],
        env_vars={},
        arguments=[
            "push_proxy PUSH_PROXY_LOGS_EU_NEW LOGS_EU_NEW "
            " --prefix /AWSLogs/{{ var.value.push_proxy_aws_account_id }}/elasticloadbalancing/{{ var.value.push_proxy_aws_region_eu }}"  # noqa: E501
            " -s {{ var.value.push_proxy_target_schema }}"
            " -a ${SNOWFLAKE_ACCOUNT}"
            " -d ${SNOWFLAKE_LOAD_DATABASE}"
            " -w ${SNOWFLAKE_LOAD_WAREHOUSE}"
            " -r ${SNOWFLAKE_LOAD_ROLE}"
            " -u ${SNOWFLAKE_LOAD_USER}"
            " -p ${SNOWFLAKE_LOAD_PASSWORD}"
        ],
    )
    extract_hpns_us_logs >> extract_tpns_logs >> extract_hpns_eu_logs


dag = push_proxy_loader()
