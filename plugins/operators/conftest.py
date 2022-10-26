from datetime import datetime
import pytest
from airflow import DAG

START_DATE = datetime(2022,1,1)

@pytest.fixture
def dag():
    dag = DAG('test_dag', description='Test DAG', start_date=START_DATE, catchup=False)
    return dag

@pytest.fixture
def full_config():
    return{
        'task_id': 'test task id',
        'http_conn_url': 'test-webhook-url',
        'message': 'test message',
        'channel': 'test channel',
        'username': 'test username',
    }

@pytest.fixture
def min_config():
    return{
        'task_id': 'test task id',
        'http_conn_url': 'test-webhook-url',
        'message': 'test message',
    }

