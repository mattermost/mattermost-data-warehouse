from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.exceptions import AirflowException
from airflow.operators import BaseOperator
import pytest
import pendulum
from datetime import datetime

@pytest.fixture(autouse=True)
def config_utils(monkeypatch):

        # patching environment variables used in airflow_utils
        monkeypatch.setenv("NAMESPACE", "test_namespace")

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
        task = BaseOperator(task_id='test_task', dag = dag)
        task_instance = TaskInstance(task, execution_date, state=None)
        context = {'dag': dag,
                'ts': execution_date,
                'execution_date': execution_date, 
                'task': task, 
                'task_instance': task_instance, 
                'exception': AirflowException('Test Exception message')}
        return context