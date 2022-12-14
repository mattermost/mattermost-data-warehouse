import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.models.xcom import XCom
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.db import provide_session
from dags.airflow_utils import send_alert
from dags.general._helpers import resolve_stitch

task_logger = logging.getLogger('airflow.task')


# creating an exception class for handling Stitch API response
class StitchApiException(Exception):
    pass


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
):
    check_stitch_extractions = SimpleHttpOperator(
        task_id="check_stitch_extractions",
        # TODO add to airflow variables
        http_conn_id="stitch",
        method="GET",
        endpoint=Variable.get('stitch_extractions_endpoint'),
        headers={'Content-Type': 'application/json', 'Authorization': Variable.get('stitch_secret')},
        xcom_push=True,
    )
    check_stitch_loads = SimpleHttpOperator(
        task_id="check_stitch_loads",
        # TODO add to airflow variables
        http_conn_id="stitch",
        method="GET",
        endpoint=Variable.get('stitch_loads_endpoint'),
        headers={'Content-Type': 'application/json', 'Authorization': Variable.get('stitch_secret')},
        xcom_push=True,
    )
    resolve_stitch_status = PythonOperator(
        task_id='resolve_stitch_status', provide_context=True, python_callable=resolve_stitch
    )

    clean_xcom = PythonOperator(
        task_id="clean_xcom",
        python_callable=cleanup_xcom,
        provide_context=True,
    )

[check_stitch_extractions, check_stitch_loads] >> resolve_stitch_status
resolve_stitch_status >> clean_xcom
