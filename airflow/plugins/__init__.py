from airflow.plugins_manager import AirflowPlugin
from hooks import MattermostWebhookHook
from operators import MattermostOperator

class MattermostPlugin(AirflowPlugin):
    name = "mattermost"
    hooks = [MattermostWebhookHook]
    operators = [MattermostOperator]
