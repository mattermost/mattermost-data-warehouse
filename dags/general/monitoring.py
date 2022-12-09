import json
import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.models.xcom import XCom
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.db import provide_session
from airflow_utils import send_alert

from plugins.operators.mattermost_operator import MattermostOperator

task_logger = logging.getLogger('airflow.task')


# creating an exception class for handling Stitch API response
class StitchApiException(Exception):
    pass


"""
This method returns list of extractions that are failing
tap_exit_status = 1, failing
tap_exit_status = 0, success
tap_exit_status = None, extraction in process
"""


def stitch_check_extractions(response):
    task_logger.info('Got extractions response, checking for errors')
    failed_extractions = {}
    try:
        extractions = json.loads(response)
        if ('data' not in extractions) or len(extractions) == 0:
            raise StitchApiException('Invalid response from extractions api')
        failed_extractions = {
            extraction['source_id']: extraction['tap_description']
            for extraction in extractions.get('data')
            if extraction['tap_exit_status'] == 1
        }
    except KeyError:
        task_logger.error('Error in check extractions ...', exc_info=True)
    except StitchApiException:
        task_logger.error('Error in ...', exc_info=True)
    return failed_extractions


"""
This method returns object of loads that are failing
error_state != None, failing
error_state = None, success
"""


def stitch_check_loads(response):
    task_logger.info('Got loads response, checking for errors')
    failed_loads = {}
    try:
        loads = json.loads(response)
        if ('data' not in loads) or len(loads) == 0:
            raise StitchApiException('Invalid response from loads api')
        failed_loads = {
            load['source_name']: load['error_state']['notification_data']['error']
            for load in loads.get('data')
            if load['error_state'] is not None
        }
    except KeyError:
        task_logger.error('Error in check loads ...', exc_info=True)
    except StitchApiException:
        task_logger.error('Error in ...', exc_info=True)
    return failed_loads


"""
This method fetches failed stitch extractions from xcom
triggers a mattermost alert in case of failure
"""


def resolve_stitch(ti=None, **kwargs):
    ti = kwargs['ti']
    extractions, loads = ti.xcom_pull(task_ids=['check_stitch_extractions', 'check_stitch_loads'])
    if not (extractions or loads):
        raise ValueError('No value found for stitch status in XCom')
    failed_loads = stitch_check_loads(loads)
    failed_extractions = stitch_check_extractions(extractions)

    if len(failed_loads) == 0 and len(failed_extractions) == 0:
        task_logger.info('There are no failed loads or extractions')
    else:
        status = ':red_circle:'
        message = f"**STITCH**: {status}\nFailed extractions: " "{failed_extractions}\nFailed loads: {failed_loads}"
        MattermostOperator(mattermost_conn_id='mattermost', text=message, task_id='resolve_stitch_message').execute(
            None
        )


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
