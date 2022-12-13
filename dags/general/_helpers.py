import json
import logging
from datetime import datetime


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

def stitch_check_extractions(response):
    """
    This method returns list of extractions that are failing
    tap_exit_status = 1, failing
    tap_exit_status = 0, success
    tap_exit_status = None, extraction in process
    """
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
    except KeyError as e:
        task_logger.error('Error in check extractions ...', exc_info=True)
        raise e
    except StitchApiException as e:
        task_logger.error('Error in ...', exc_info=True)
        raise e
    return failed_extractions

def stitch_check_loads(response):
    """
    This method returns object of loads that are failing
    error_state != None, failing
    error_state = None, success
    """
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
    except KeyError as e:
        task_logger.error('Error in check loads ...', exc_info=True)
        raise e
    except StitchApiException as e:
        task_logger.error('Error in ...', exc_info=True)
        raise e
    return failed_loads


"""
This method fetches failed stitch extractions from xcom
triggers a mattermost alert in case of failure
"""


def resolve_stitch(ti=None, **kwargs):
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



