from airflow.plugins_manager import AirflowPlugin
from airflow.plugins.mattermost.hooks import MattermostWebhookHook
from airflow.plugins.mattermost.operators import MattermostOperator

class MattermostPlugin(AirflowPlugin):
    name = "mattermost"
    hooks = [MattermostWebhookHook]
    operators = [MattermostOperator]
