from mattermost.hooks import MattermostWebhookHook
from mattermost.operators import MattermostOperator

from airflow.plugins_manager import AirflowPlugin


class MattermostPlugin(AirflowPlugin):
    name = "mattermost"
    hooks = [MattermostWebhookHook]
    operators = [MattermostOperator]
