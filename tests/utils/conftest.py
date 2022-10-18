from pathlib import Path

import json

import pandas as pd
import pytest


from responses import Response


@pytest.fixture(autouse=True)
def mock_settings_env_vars(mocker):
    """
    Mock any environment variables loaded to global variables.
    TODO: Load configuration using factory pattern.
    """
    mocker.patch("utils.run_dbt_cloud_job.token", "test-dbt-key")
    mocker.patch("utils.run_dbt_cloud_job.account_id", "1001")
    mocker.patch("utils.run_dbt_cloud_job.timeout", 35)


@pytest.fixture()
def given_request_to(responses):
    def _given_request_to(url, response_file, method="GET", status=200):
        with open(Path(__file__).parent / 'fixtures' / 'dbt' / response_file) as fp:
            rsp = Response(method=method, url=url,
                           status=status, headers={"Authorization": f"Token test-dbt-key", "Content-Type": "application/json"},
                           body=fp.read())
            responses.add(rsp)

    return _given_request_to


@pytest.fixture()
def user_agent_df():
    with open(Path(__file__).parent / 'fixtures' / 'user_agent' / 'dataset.json') as fp:
        return pd.DataFrame(json.load(fp))
