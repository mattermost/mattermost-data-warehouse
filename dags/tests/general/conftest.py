import json
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def config_utils(monkeypatch):
    # patching environment variables used in airflow_utils
    monkeypatch.setenv("NAMESPACE", "test_namespace")
    monkeypatch.setenv("AIRFLOW_BASE_URL", "https://test.airflow.mattermost.com")


@pytest.fixture
def load_data():
    # loads data from a file
    def _load_data(filename):
        file = open(Path(__file__).parent / 'fixtures' / filename, encoding='utf-8')
        return file.read()

    return _load_data
