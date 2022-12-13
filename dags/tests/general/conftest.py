from pathlib import Path
import pytest


@pytest.fixture(autouse=True)
def config_utils(monkeypatch):
    # patching environment variables used in airflow_utils
    monkeypatch.setenv(
        "NAMESPACE",
        "test_namespace"
        )
    monkeypatch.setenv(
        "AIRFLOW_BASE_URL",
        "https://test.airflow.mattermost.com"
        )
    monkeypatch.setenv(
        "AIRFLOW_VAR_STITCH_SECRET",
        "test_secret_value"
    )
    monkeypatch.setenv(
        "AIRFLOW_VAR_STITCH_LOADS_ENDPOINT",
        "test_loads_endpoint"
    )
    monkeypatch.setenv(
        "AIRFLOW_VAR_STITCH_EXTRACTIONS_ENDPOINT",
        "test_extractions_endpoint"
    )



@pytest.fixture
def load_data():
    # loads data from a file
    def _load_data(filename):
        file = open(
            Path(__file__).parent / 'fixtures' / filename,
            encoding='utf-8')
        return file.read()

    return _load_data
