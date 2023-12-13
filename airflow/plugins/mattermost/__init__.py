from airflow.plugins_manager import AirflowPlugin
from mattermost.hooks import MattermostWebhookHook
from mattermost.operators import MattermostOperator

class MattermostPlugin(AirflowPlugin):
    name = "mattermost"
    hooks = [MattermostWebhookHook]
    operators = [MattermostOperator]
