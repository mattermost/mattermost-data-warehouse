import logging
from datetime import datetime

from mattermost_dags.airflow_utils import cleanup_xcom, send_alert
from mattermost_dags.general._helpers import resolve_hightouch, resolve_stitch

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

task_logger = logging.getLogger('airflow.task')


default_args = {'on_failure_callback': send_alert}

with DAG(
    'monitoring',
    default_args=default_args,
    start_date=datetime(2017, 3, 20),
    schedule="0 10 * * *",
    catchup=False,
) as dag:
    check_stitch_extractions = SimpleHttpOperator(
        task_id="check_stitch_extractions",
        http_conn_id="stitch",
        method="GET",
        endpoint=Variable.get('stitch_extractions_endpoint'),
        headers={'Content-Type': 'application/json', 'Authorization': Variable.get('stitch_secret')},
        do_xcom_push=True,
    )
    check_stitch_loads = SimpleHttpOperator(
        task_id="check_stitch_loads",
        http_conn_id="stitch",
        method="GET",
        endpoint=Variable.get('stitch_loads_endpoint'),
        headers={'Content-Type': 'application/json', 'Authorization': Variable.get('stitch_secret')},
        do_xcom_push=True,
    )
    resolve_stitch_status = PythonOperator(task_id='resolve_stitch_status', python_callable=resolve_stitch)
    hightouch_check_syncs = SimpleHttpOperator(
        task_id="check_hightouch_syncs",
        http_conn_id="hightouch",
        method="GET",
        endpoint=Variable.get('hightouch_syncs_endpoint'),
        headers={'Content-Type': 'application/json', 'Authorization': f"Bearer {Variable.get('hightouch_secret')}"},
        do_xcom_push=True,
    )
    resolve_hightouch_status = PythonOperator(task_id='resolve_hightouch_status', python_callable=resolve_hightouch)

    clean_xcom = PythonOperator(
        task_id="clean_xcom",
        python_callable=cleanup_xcom,
    )

[check_stitch_extractions, check_stitch_loads] >> resolve_stitch_status
hightouch_check_syncs >> resolve_hightouch_status
[resolve_hightouch_status, resolve_stitch_status] >> clean_xcom
