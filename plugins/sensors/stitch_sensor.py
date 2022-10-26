from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from operators.mattermost_operator import MattermostWebhookOperator
import requests
import json
import os
os.environ["no_proxy"]="*" # to prevent error - Task exited with return code Negsignal.SIGSEGV

class StitchIntegrationSensor(BaseSensorOperator):
    def __init__(
        self,
        *,
        integration_name,
        **kwargs) -> None:
        self.integration_name = integration_name
        super().__init__(**kwargs)

    # This method fetches attributes from airflow variables and connections
    def initialize_sensor(self):
        self.token = Variable.get("stitch_api_token")
        self.headers = {'Authorization': self.token}
        self.extractions_url = Variable.get("stitch_extractions_url")
        self.loads_url = Variable.get("stitch_loads_url")
        self.mattermost_webhook_url = BaseHook.get_connection("mattermost").host

    # This method returns list of extractions that are failing
    # tap_exit_status = 1, failing
    # tap_exit_status = 0, success
    # tap_exit_status = None, extraction in process
    def get_extractions(self):
        failed_extractions = {}
        try:
            print("poking api -", self.extractions_url)
            response = requests.request("GET", self.extractions_url, headers=self.headers, data={})
            extractions = response.json()['data']
            for extraction in extractions:
                if extraction['tap_exit_status'] is 1:
                    failed_extractions[extraction['source_id']] = extraction['tap_description']
        except Exception as e:
            print('error in getting extractions from stitch ', str(e))
            raise(e)
        return failed_extractions

    # This method returns object of loads that are failing
    # error_state != None, failing
    # error_state = None, success
    def get_loads(self):
        failed_loads = {}
        try:
            print("poking api -", self.loads_url)
            response = requests.request("GET", self.loads_url, headers=self.headers, data={})
            loads = response.json()['data']
            for load in loads:
                if load['error_state'] is not None:
                    failed_loads[load['source_name']] = load['error_state']['notification_data']['error']
        except Exception as e:
            print('error in getting loads from stitch ', str(e))
            raise(e)
        return failed_loads

    # This method returns a prepared string to be sent to mattermost alert operator
    def prepare_message(self, url, extractions, loads):
        if len(extractions) == 0 and len(loads) == 0: status = ':large_green_circle:'
        else:   status = ':red_circle:'
        message = """
                Stitch Sensor: {stitch_status}
                Failed extractions: {failed_extractions}
                Failed loads: {failed_loads}
                *Log Url*: {log_url}
            """.format(
                failed_extractions=extractions,
                failed_loads=loads,
                stitch_status=status,
                log_url=url
            )
        print('prepared message :',message )
        return message
        
    def poke(self, context):
        try:
            print("poking stitch sensor ")
            self.initialize_sensor()
            extractions = self.get_extractions()
            loads = self.get_loads()
            message = self.prepare_message(context.get('task_instance').log_url, extractions, loads)
            alert = MattermostWebhookOperator(
                task_id='stitch_alert',
                http_conn_url=self.mattermost_webhook_url,
                message=message,
                channel='data-dev', # TODO move to airflow variables/function call
                username='stitch'
            )
            alert.printme(context=context)
            return True
        except Exception as e: 
            print('error in method poke of stitch sensor ', str(e))
            return False
    
    def printme(self, context):
        print("printme invoked in StitchIntegrationSensor")