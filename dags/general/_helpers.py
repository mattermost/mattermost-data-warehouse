import json
import logging
import os
from datetime import datetime, timedelta

import looker_sdk
import tabulate

from plugins.operators.mattermost_operator import MattermostOperator

task_logger = logging.getLogger('airflow.task')


# creating an exception class for handling Stitch API response
class StitchApiException(Exception):
    pass


# creating an exception class for handling hightouch API response
class HightouchApiException(Exception):
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
            and time_filter(extraction['completion_time'], "%Y-%m-%dT%H:%M:%SZ", 1)
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
            if load['error_state'] is not None and time_filter(load['last_batch_loaded_at'], "%Y-%m-%dT%H:%M:%SZ", 1)
        }
    except KeyError as e:
        task_logger.error('Error in check loads ...', exc_info=True)
        raise e
    except StitchApiException as e:
        task_logger.error('Error in ...', exc_info=True)
        raise e
    return failed_loads


def resolve_stitch(ti=None, **kwargs):
    """
    This method fetches failed stitch extractions from xcom
    triggers a mattermost alert in case of failure
    """
    extractions, loads = ti.xcom_pull(task_ids=['check_stitch_extractions', 'check_stitch_loads'])
    if not (extractions or loads):
        raise ValueError('No value found for stitch status in XCom')
    failed_loads = stitch_check_loads(loads)
    failed_extractions = stitch_check_extractions(extractions)

    if len(failed_loads) == 0 and len(failed_extractions) == 0:
        task_logger.info('There are no failed loads or extractions')
    else:
        status = ':red_circle:'
        message = f"**STITCH**: {status}\nFailed extractions:{failed_extractions}\nFailed loads: {failed_loads}"
        MattermostOperator(mattermost_conn_id='mattermost', text=message, task_id='resolve_stitch_message').execute(
            None
        )


def hightouch_check_syncs(response):
    """
    This method returns object of sync that have status
    status = success or disabled, success
    status = fail or warning, fail
    """
    syncs = json.loads(''.join(response))
    failed_syncs = {}
    try:
        if ('data' not in syncs) or len(syncs) == 0:
            raise HightouchApiException('Invalid response from syncs api')
        failed_syncs = {
            sync['slug']: sync['status'] for sync in syncs.get('data') if sync['status'] not in ('success', 'disabled')
        }
    except KeyError as e:
        task_logger.error('Error in check syncs ...', exc_info=True)
        raise e
    except HightouchApiException as e:
        task_logger.error('Error in ...', exc_info=True)
        raise e
    return failed_syncs


def resolve_hightouch(ti=None, **kwargs):
    """
    This method fetches failed hightouch syncs from xcom
    triggers a mattermost alert in case of failure
    """
    syncs = ti.xcom_pull(task_ids=['check_hightouch_syncs'])
    if not syncs:
        raise ValueError('No value found for hightouch status in XCom')
    failed_syncs = hightouch_check_syncs(syncs)

    if len(failed_syncs) == 0:
        task_logger.info('There are no failed syncs')
    else:
        status = ':red_circle:'
        message = f"**HIGHTOUCH**: {status}\nFailed syncs: {failed_syncs}"
        MattermostOperator(mattermost_conn_id='mattermost', text=message, task_id='resolve_hightouch_message').execute(
            None
        )


def time_filter(target_time, format, delta_hours):
    """
    This method returns True if target_time + delta_hours >= current time.
    False otherwise.
    :param target_time: The UTC datetime string of target.
    :param format: Format of datetime string.
    :param delta_hours: Number of hours to be added to target_time for comparison.
    """
    return datetime.strptime(target_time, format) + timedelta(hours=delta_hours) >= datetime.utcnow()


def format_look_result(result: list) -> list:
    """
    Removes new line character, sql function `VARCHAR` to prevent cloudflare error
    Adds base looker url to dashboard link
    """
    return [
        {
            " ".join(key.replace("history", "error").split(".")): "[link]("+os.getenv("LOOKERSDK_BASE_URL")+"/dashboards-next/"
            + str(_dict[key])
            + ")"
            if key == "dashboard.link"
            else str(_dict[key]).replace("\n", " ").replace("VARCHAR","")
            for key in _dict.keys()
        }
        for _dict in result
    ]


def get_look_data(title: str) -> str:
    """
    This method gets look data from looker and returns string of result
    """
    title = title.lower()
    sdk = looker_sdk.init40()
    look = next(iter(sdk.search_looks(title=title)), None)
    if not look:
        raise Exception(f"Look '{title}' was not found")
    look_data = sdk.run_look(look.id, result_format='json')
    if not look_data:
        raise Exception(f"Unable to get look data for id '{str(look.id)}'")
    return look_data


def post_looker_results(look_data: str, connection_id: str) -> bool:
    """
    This method returns True if look data is sent to mattermost.
    False otherwise.
    :param look_data: Unformatted string of look data 
    :param connection_id: Connection id used by mattermost operator
    """
    message = tabulate(
        format_look_result(json.loads(look_data)),
        headers="keys",
        tablefmt='github'
        )
    MattermostOperator(mattermost_conn_id=connection_id, text=message, task_id='post_looker_results').execute(
        None
    )


def resolve_looker(look_title: str, connection_id: str, secrets: dict) -> bool:
    """
    This method returns if look data is successfully sent to mattermost channel, else False
    :param look_title: Name of look to be sent to channel
    :param connection_id: Connection id used by mattermost operator, defaults to `mattermost`
    :param secrets: Dictionary of looker secrets to be added to environment, required by looker sdk.
    """
    os.environ["LOOKERSDK_BASE_URL"] = secrets.get("looker_base_url")
    os.environ["LOOKERSDK_CLIENT_ID"] = secrets.get("looker_client_id")
    os.environ["LOOKERSDK_CLIENT_SECRET"] = secrets.get("looker_client_secret")
    os.environ["LOOKERSDK_VERIFY_SSL"] = "1"
    look_data = get_look_data(look_title)
    post_looker_results(look_data, connection_id)