import pytest

from airflow import AirflowException
from airflow.models import Connection

from plugins.hooks.mattermost_webhook_hook import MattermostWebhookHook

def test_execute_simple_message(responses, ok_response, mock_connection):
    hook = MattermostWebhookHook(mattermost_conn_id='some_conn_id', text='Test message')
    hook.execute()

    responses.assert_call_count("http://mattermost.example.com/hooks/myhookid", 1)


def test_missing_conn_id(mock_connection_without_token):
    hook = MattermostWebhookHook(text='Test message')

    with pytest.raises(AirflowException) as exc_info:
        hook.execute()
        assert str(exc_info.value) == "Failed to create Mattermost client. No http_conn_id provided"


def test_missing_token(mock_connection_without_token):
    hook = MattermostWebhookHook(mattermost_conn_id='some_conn_id', text='Test message')

    with pytest.raises(AirflowException) as exc_info:
        hook.execute()
        assert str(exc_info.value) == "Failed to create Mattermost client. No token provided"