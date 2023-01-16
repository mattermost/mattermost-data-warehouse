from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def config_utils(monkeypatch):
    # patching environment variables used in airflow_utils
    monkeypatch.setenv("NAMESPACE", "test_namespace")
    monkeypatch.setenv("AIRFLOW_BASE_URL", "https://test.airflow.mattermost.com")
    monkeypatch.setenv("AIRFLOW_VAR_STITCH_SECRET", "test_secret_value")
    monkeypatch.setenv("AIRFLOW_VAR_STITCH_LOADS_ENDPOINT", "test_loads_endpoint")
    monkeypatch.setenv("AIRFLOW_VAR_STITCH_EXTRACTIONS_ENDPOINT", "test_extractions_endpoint")


@pytest.fixture
def load_data():
    # loads data from a file
    def _load_data(filename):
        with open(Path(__file__).parent / 'fixtures' / filename, encoding='utf-8') as f:
            return f.read()

    return _load_data


@pytest.fixture
def task_instance(load_data):
    # returns mock object of task instance
    class Ti:
        def __init__(self, isSuccess=True):
            self.isSuccess = isSuccess

        def xcom_pull(self, **args):
            if self.isSuccess is None:
                return None
            elif not self.isSuccess:
                return load_data('monitoring/syncs_fail.json')
            elif self.isSuccess:
                return load_data('monitoring/syncs_pass.json')

    def _task_instance(isSuccess):
        return Ti(isSuccess)

    return _task_instance
