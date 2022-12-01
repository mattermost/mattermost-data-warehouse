from airflow import DAG
import json
from operators.mattermost_operator import MattermostOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime
from airflow_utils import send_alert
from airflow.models.xcom import XCom
from airflow.utils.db import provide_session
from airflow.models import Variable
import logging
task_logger = logging.getLogger('airflow.task')


"""
This method returns list of extractions that are failing
tap_exit_status = 1, failing
tap_exit_status = 0, success
tap_exit_status = None, extraction in process
"""
def stitch_check_extractions(response):
    task_logger.info('Got extractions response from stitch api, checking for errors')
    failed_extractions={}
    try:
        extractions = json.loads(response)['data']
        for extraction in extractions:
            if extraction['tap_exit_status'] is 1:
                failed_extractions[extraction['source_id']] = extraction['tap_description']
    except Exception as e:
        task_logger.error('error in getting extractions from stitch ', str(e))
        raise(e)
    return failed_extractions


"""
This method returns object of loads that are failing
error_state != None, failing
error_state = None, success
"""
def stitch_check_loads(response):
    task_logger.info('Got loads response from stitch api, checking for errors')
    failed_loads={}
    try:
        loads = json.loads(response)['data']
        for load in loads:
            if load['error_state'] is not None:
                failed_loads[load['source_name']] = load['error_state']['notification_data']['error']
    except Exception as e:
        task_logger.error('error in getting loads from stitch ', str(e))
        raise(e)
    return failed_loads

"""
This method fetches failed stitch extractions from xcom
triggers a mattermost alert in case of failure
""" 
def resolve_stitch(**kwargs):
    ti = kwargs['ti']
    extractions,loads = ti.xcom_pull(task_ids=['check_stitch_extractions','check_stitch_loads'])
    if not (extractions or loads):
        raise ValueError('No value found for stitch status in XCom')
    # task_logger.info('loads are', loads)
    failed_loads = stitch_check_loads(loads)
    failed_extractions = stitch_check_extractions(extractions)

    if len(failed_loads) == 0 and len(failed_extractions) == 0: 
        task_logger.info('There are no failed loads or extractions')
    else:
        status = ':red_circle:'
        message = f"**STITCH**: {status}\nFailed extractions: {failed_extractions}\nFailed loads: {failed_loads}"
        MattermostOperator(mattermost_conn_id='mattermost',text=message, task_id='resolve_stitch_message')

# To clean up Xcom after dag finished run.
@provide_session
def cleanup_xcom(**context):     
    dag = context["dag"]
    dag_id = dag._dag_id 
    session=context["session"]
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()

default_args={
    'on_failure_callback': send_alert
}

dag = DAG('monitoring', 
        default_args=default_args,
        start_date=datetime(2017, 3, 20), 
        catchup=False)

with dag:
    check_stitch_extractions = SimpleHttpOperator(
                task_id="check_stitch_extractions",
                #TODO add to airflow variables
                http_conn_id="stitch",
                method="GET",
                endpoint=Variable.get('stitch_extractions_endpoint'),
                headers = {
                        'Content-Type': 'application/json',
                        'Authorization': Variable.get('stitch_secret')
                        },
                response_check=lambda x: True,
                xcom_push=True,
                dag=dag,
            )
    check_stitch_loads = SimpleHttpOperator(
                task_id="check_stitch_loads",
                #TODO add to airflow variables
                http_conn_id="stitch",
                method="GET",
                endpoint=Variable.get('stitch_loads_endpoint'),
                headers = {
                        'Content-Type': 'application/json',
                        'Authorization': Variable.get('stitch_secret')
                        },
                response_check=lambda x: True,
                xcom_push=True,
                dag=dag,
            )
    resolve_stitch_status = PythonOperator(
                task_id='resolve_stitch_status', 
                provide_context=True ,
                python_callable=resolve_stitch)

    clean_xcom = PythonOperator(
                task_id="clean_xcom",
                python_callable = cleanup_xcom,
                provide_context=True, 
                dag=dag
)

check_stitch_extractions >> check_stitch_loads
check_stitch_loads >> resolve_stitch_status
resolve_stitch_status >> clean_xcom