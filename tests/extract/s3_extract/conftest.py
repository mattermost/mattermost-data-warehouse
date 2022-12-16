import os

import pytest


@pytest.fixture()
def mock_snowflake(mocker):
    def _mock_snowflake(module_name):
        mock_engine_factory = mocker.patch(f"{module_name}.snowflake_engine_factory")
        mock_engine = mocker.MagicMock()
        mock_engine_factory.return_value = mock_engine
        mock_connection = mocker.MagicMock()
        mock_engine.connect.return_value = mock_connection
        mock_execute_query = mocker.patch(f"{module_name}.execute_query")

        # Mock pandas' to_sql
        mock_to_sql = mocker.patch("pandas.io.sql.to_sql")
        return mock_engine, mock_connection, mock_execute_query, mock_to_sql

    return _mock_snowflake


@pytest.fixture()
def mock_environment(mocker):
    mock_env_vars = {
        'GITHUB_TOKEN': 'token',
        'DIAGNOSTIC_LOCATION_ONE': 'location-one',
        'DIAGNOSTIC_LOCATION_TWO': 'location-two',
        'RELEASE_LOCATION': 'test-release-location',
        'AWS_ACCOUNT_ID': 'test-aws-account-id',
    }
    mocker.patch.dict(os.environ, mock_env_vars, clear=True)
    mock_env_copy = mocker.patch('os.environ.copy')
    mock_env_copy.return_value = mock_env_vars

    return mock_env_vars
