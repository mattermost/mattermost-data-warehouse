from datetime import datetime

import pendulum
import pytest
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.operators import BaseOperator


@pytest.fixture(autouse=True)
def config_utils(monkeypatch):

    # patching environment variables used in airflow DAGs
    monkeypatch.setenv("NAMESPACE", "test_namespace")
    monkeypatch.setenv("AIRFLOW_BASE_URL", "https://test.airflow.mattermost.com")
    monkeypatch.setenv("AIRFLOW_VAR_STITCH_SECRET", "test_secret_value")
    monkeypatch.setenv("AIRFLOW_VAR_STITCH_LOADS_ENDPOINT", "test_loads_endpoint")
    monkeypatch.setenv("AIRFLOW_VAR_STITCH_EXTRACTIONS_ENDPOINT", "test_extractions_endpoint")
    monkeypatch.setenv("AIRFLOW_VAR_HIGHTOUCH_SYNCS_ENDPOINT", "test_syncs_endpoint")
    monkeypatch.setenv("AIRFLOW_VAR_HIGHTOUCH_SECRET", "test_hightouch_secret")
    monkeypatch.setenv("AIRFLOW_VAR_RUDDER_SCHEMAS", '["schema1", "schema2"]')
    monkeypatch.setenv("AIRFLOW_VAR_RUDDER_MAX_AGE", '2')


@pytest.fixture
def config_alert_context(mocker):
    """
    Mocks airflow context passed as callback function parameter.
    Dict contains only required keys used in the function.

    'dag': instance of airflow DAG.
    'ts': execution timestamp
    'execution_date': execution date
    'task': instance of BaseOperator. A task defined in the above dag.
    'task_instance': instance of TaskInstance
    'exception': instance of AirflowException
    """
    execution_date = pendulum.datetime(2022, 11, 15, 0, 0, 0)
    dag = DAG("test_utils_dag", start_date=datetime(2022, 11, 15))
    task = BaseOperator(task_id='test_task', dag=dag)
    task_instance = TaskInstance(task, execution_date, state=None)
    context = {
        'dag': dag,
        'ts': execution_date,
        'execution_date': execution_date,
        'task': task,
        'task_instance': task_instance,
        'exception': AirflowException('Test Exception message'),
    }
    return context
