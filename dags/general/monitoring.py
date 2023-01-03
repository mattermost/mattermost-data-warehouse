import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.models.xcom import XCom
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.db import provide_session

from dags.airflow_utils import send_alert
from dags.general._helpers import resolve_hightouch, resolve_stitch

task_logger = logging.getLogger('airflow.task')


# To clean up Xcom after dag finished run.
@provide_session
def cleanup_xcom(**context):
    dag = context["dag"]
    dag_id = dag._dag_id
    session = context["session"]
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


default_args = {'on_failure_callback': send_alert}

with DAG(
    'monitoring',
    default_args=default_args,
    start_date=datetime(2017, 3, 20),
    schedule_interval='@hourly',
    catchup=False,
) as dag:
    check_stitch_extractions = SimpleHttpOperator(
        task_id="check_stitch_extractions",
        http_conn_id="stitch",
        method="GET",
        endpoint=Variable.get('stitch_extractions_endpoint'),
        headers={'Content-Type': 'application/json', 'Authorization': Variable.get('stitch_secret')},
        xcom_push=True,
    )
    check_stitch_loads = SimpleHttpOperator(
        task_id="check_stitch_loads",
        http_conn_id="stitch",
        method="GET",
        endpoint=Variable.get('stitch_loads_endpoint'),
        headers={'Content-Type': 'application/json', 'Authorization': Variable.get('stitch_secret')},
        xcom_push=True,
    )
    resolve_stitch_status = PythonOperator(
        task_id='resolve_stitch_status', provide_context=True, python_callable=resolve_stitch
    )
    hightouch_check_syncs = SimpleHttpOperator(
        task_id="check_hightouch_syncs",
        http_conn_id="hightouch",
        method="GET",
        endpoint=Variable.get('hightouch_syncs_endpoint'),
        headers={'Content-Type': 'application/json', 'Authorization': Variable.get('hightouch_secret')},
        xcom_push=True,
    )
    resolve_hightouch_status = PythonOperator(
        task_id='resolve_hightouch_status', provide_context=True, python_callable=resolve_hightouch
    )

    clean_xcom = PythonOperator(
        task_id="clean_xcom",
        python_callable=cleanup_xcom,
        provide_context=True,
    )

[check_stitch_extractions, check_stitch_loads] >> resolve_stitch_status
resolve_stitch_status >> clean_xcom
