from airflow.utils.decorators import apply_defaults
import requests

class MattermostWebhookOperator():

    @apply_defaults
    def __init__(self,
                 http_conn_url=None,
                 message=None,
                 channel=None,
                 username=None,
                 **kwargs) -> None:
        self.http_conn_url = http_conn_url
        self.message = message
        self.channel = channel
        self.username = username

    def execute(self, context): 
        print("executing post in mattermost operator- ", self.http_conn_url)
        payload='{"text": "%s", "channel": "%s"}' % (self.message, self.channel)
        response = requests.post(
            self.http_conn_url, data=payload.encode('utf-8'),
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code == 200: return True
        return False

    def printme(self, context):
        print("printme invoked with message :", self.message)

