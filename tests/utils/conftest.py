import json
import os
from pathlib import Path

import pandas as pd
import pytest
from responses import Response
from sqlalchemy import create_engine


@pytest.fixture(autouse=True)
def mock_settings_env_vars(mocker):
    """
    Mock any environment variables loaded to global variables.
    TODO: Load configuration using factory pattern.
    """
    # Mock items loaded from environment on module initialization
    mocker.patch("utils.run_dbt_cloud_job.token", "test-dbt-key")
    mocker.patch("utils.run_dbt_cloud_job.account_id", "1001")
    mocker.patch("utils.run_dbt_cloud_job.timeout", 35)

    # Mock items loaded using os.environ in method calls
    mock_env_vars = {'GITHUB_TOKEN': 'token'}
    mocker.patch.dict(os.environ, mock_env_vars, clear=True)


@pytest.fixture()
def given_request_to(request, responses):
    """
    Loads test data from fixture directory. Defaults can be modified on the module level by defining a dict with name
    __MOCK_REQUEST_DEFAULTS. The dict may contains the following keys:

    'dir': a string with the directory under fixtures/ to use for loading the data from.
    'headers': a dictionary with any default headers to expect.
    """

    def _given_request_to(url, response_file, method="GET", status=200):
        config = getattr(request.module, '__MOCK_REQUEST_DEFAULTS', {})
        target = (
            Path(__file__).parent / 'fixtures' / config.get('dir') / response_file
            if config.get('dir')
            else Path(__file__).parent / 'fixtures' / response_file
        )
        with open(target, 'rb') as fp:
            rsp = Response(
                method=method, url=url, status=status, headers=config.get('headers', {}), body=fp.read(), stream=True
            )
            responses.add(rsp)

    return _given_request_to


@pytest.fixture()
def mock_snowflake(mocker):
    def _mock_snowflake(module_name):
        mock_engine_factory = mocker.patch(f"{module_name}.snowflake_engine_factory")
        mock_engine = mocker.MagicMock()
        mock_engine_factory.return_value = mock_engine
        mock_connection = mocker.MagicMock()
        mock_engine.connect.return_value = mock_connection
        # Handle engine.begin() context manager as well
        mock_engine.begin.return_value.__enter__.return_value = mock_connection
        mock_execute_query = mocker.patch(f"{module_name}.execute_query", create=True)

        return mock_engine, mock_connection, mock_execute_query

    return _mock_snowflake


@pytest.fixture()
def mock_snowflake_pandas(mocker):
    def _mock_snowflake_pandas(module_name):
        # Mock execute_dataframe method
        mock_execute_dataframe = mocker.patch(f"{module_name}.execute_dataframe", create=True)
        # Mock pandas' to_sql
        mock_to_sql = mocker.patch("pandas.io.sql.to_sql")
        return mock_execute_dataframe, mock_to_sql

    return _mock_snowflake_pandas


@pytest.fixture()
def load_dataset():
    def _load_dataset(filename):
        with open(Path(__file__).parent / 'fixtures' / filename) as fp:
            return pd.DataFrame(json.load(fp))

    return _load_dataset


@pytest.fixture()
def user_agent_df():
    with open(Path(__file__).parent / 'fixtures' / 'user_agent' / 'dataset.json') as fp:
        return pd.DataFrame(json.load(fp))


@pytest.fixture()
def user_agent_input():
    """
    Reads a list of user agents from fixtures.
    """
    with open(Path(__file__).parent / 'fixtures' / 'user_agent' / 'agents.txt') as fp:
        return fp.read().splitlines()


@pytest.fixture
def mock_snowflake_connector(mocker):
    def _mock_snowflake_connector(module_name):
        snowflake_connector_factory = mocker.patch(f"{module_name}.snowflake.connector.connect")
        mock_connection = mocker.Mock()
        snowflake_connector_factory.return_value = mock_connection
        mock_execute = mocker.Mock()
        mock_fetchall = [('test row 1 column 1', 'test row 1 column 2'), ('test row 2 column 1', 'test row 2 column 2')]
        mock_cursor = mocker.Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.execute.return_value = mock_execute
        mock_cursor.fetchall.return_value = mock_fetchall

        return mock_cursor, mock_connection, mock_execute, mock_fetchall

    return _mock_snowflake_connector


@pytest.fixture
def post_data_ok(responses):
    response = Response(
        method="POST", url="https://mattermost.example.com/hooks/hookid", status=200, content_type='application/json'
    )
    responses.add(response)


@pytest.fixture
def post_data_error(responses):
    response = Response(
        method="POST", url="https://mattermost.example.com/hooks/hookid", status=401, content_type='application/json'
    )
    responses.add(response)


@pytest.fixture
def config_data(monkeypatch, mocker):
    # mocking loading k8 secrets to environment variables
    monkeypatch.setenv("NPS_WEBHOOK_URL", "https://mattermost.example.com/hooks/hookid")
    monkeypatch.setenv("DOCS_WEBHOOK_URL", "https://mattermost.example.com/hooks/hookid")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "test password")
    monkeypatch.setenv("SNOWFLAKE_USER", "test user")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test account")
    monkeypatch.setenv("SNOWFLAKE_TRANSFORM_WAREHOUSE", "test warehouse")
    monkeypatch.setenv("SNOWFLAKE_TRANSFORM_DATABASE", "test database")
    monkeypatch.setenv("SNOWFLAKE_TRANSFORM_SCHEMA", "test schema")


@pytest.fixture
def sqlalchemy_memory_engine():
    engine = create_engine('sqlite:///:memory:')
    yield engine
    engine.dispose()
