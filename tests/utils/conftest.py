from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def mock_settings_env_vars(mocker):
    """
    Mock any environment variables loaded to global variables.
    TODO: Load configuration using factory pattern.
    """
    mocker.patch("utils.run_dbt_cloud_job.token", "test-dbt-key")
    mocker.patch("utils.run_dbt_cloud_job.account_id", "1001")
    mocker.patch("utils.run_dbt_cloud_job.timeout", 35)
