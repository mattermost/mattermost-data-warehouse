from datetime import datetime

import pytest

from airflow import DAG

START_DATE = datetime(2017, 3, 20)


@pytest.fixture
def dag():
    dag = DAG('test_dag', description='Test DAG', start_date=START_DATE, catchup=False)
    return dag


@pytest.fixture
def full_config():
    return {
        'mattermost_conn_id': 'test-webhook-connection',
        "text": "Test message",
        "channel": "test-channel",
        "username": "joe",
        "type": "custom_test",
        "props": {"card": "Test Card **Works**"},
    }


@pytest.fixture
def min_config():
    return {'mattermost_conn_id': 'test-webhook-connection', "text": "Test message"}
