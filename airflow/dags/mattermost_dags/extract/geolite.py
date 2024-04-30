import pendulum
from mattermost_dags.airflow_utils import MATTERMOST_DATAWAREHOUSE_IMAGE, pod_defaults, send_alert
from mattermost_dags.kube_secrets import (
    GEO_ACCOUNT_ID,
    GEO_LICENSE_ID,
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
    schedule="0 2 * * WED,SAT",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    on_failure_callback=send_alert,
)
def geolite_loader():
    """
    ### GeoLite2 Loader DAG.

    Refreshes GeoLite2 data in snowflake. Geolite2 database refreshes twice per week
    (see https://support.maxmind.com/hc/en-us/articles/4408216129947-Download-and-Update-Databases).
    """

    loader = KubernetesPodOperator(
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
            GEO_ACCOUNT_ID,
            GEO_LICENSE_ID,
        ],
        env_vars={},
        arguments=[
            "geolite "
            " -s {{ var.value.geolite_target_schema }}"
            " -a ${SNOWFLAKE_ACCOUNT}"
            " -d ${SNOWFLAKE_LOAD_DATABASE}"
            " -w ${SNOWFLAKE_LOAD_WAREHOUSE}"
            " -r ${SNOWFLAKE_LOAD_ROLE}"
            " -u ${SNOWFLAKE_LOAD_USER}"
            " -p ${SNOWFLAKE_LOAD_PASSWORD}"
            " --geo-account-id ${GEO_ACCOUNT_ID}"
            " --geo-license-id ${GEO_LICENSE_ID}"
        ],
    )

    loader


dag = geolite_loader()
