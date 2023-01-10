import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from snowflake.sqlalchemy import URL
from sqlalchemy.exc import SQLAlchemyError

from extract.utils import execute_dataframe, execute_query, snowflake_engine_factory


@pytest.mark.parametrize(
    "environment,role,schema,url",
    [
        pytest.param(
            {
                "SNOWFLAKE_USER": "admin",
                "SNOWFLAKE_PASSWORD": "supersafe",
                "SNOWFLAKE_ACCOUNT": "test-admin-account",
                "SNOWFLAKE_LOAD_DATABASE": "test-admin-database",
                "SNOWFLAKE_LOAD_WAREHOUSE": "test-admin-warehouse",
            },
            "SYSADMIN",
            "",
            URL(
                user="admin",
                password="supersafe",
                account="test-admin-account",
                database="test-admin-database",
                warehouse="test-admin-warehouse",
                role="SYSADMIN",
                schema="",
            ),
            id="SYSADMIN",
        ),
        pytest.param(
            {
                "SNOWFLAKE_LOAD_USER": "loader-1",
                "SNOWFLAKE_LOAD_PASSWORD": "loads-data-1",
                "SNOWFLAKE_ACCOUNT": "test-loader-account-1",
                "SNOWFLAKE_TRANSFORM_DATABASE": "test-loader-database-1",
                "SNOWFLAKE_LOAD_WAREHOUSE": "test-loader-warehouse-1",
            },
            "ANALYTICS_LOADER",
            "",
            URL(
                user="loader-1",
                password="loads-data-1",
                account="test-loader-account-1",
                database="test-loader-database-1",
                warehouse="test-loader-warehouse-1",
                role="LOADER",
                schema="",
            ),
            id="ANALYTICS_LOADER",
        ),
        pytest.param(
            {
                "SNOWFLAKE_LOAD_USER": "loader-2",
                "SNOWFLAKE_LOAD_PASSWORD": "loads-data-2",
                "SNOWFLAKE_ACCOUNT": "test-loader-account-2",
                "SNOWFLAKE_LOAD_DATABASE": "test-loader-database-2",
                "SNOWFLAKE_LOAD_WAREHOUSE": "test-loader-warehouse-2",
            },
            "LOADER",
            "",
            URL(
                user="loader-2",
                password="loads-data-2",
                account="test-loader-account-2",
                database="test-loader-database-2",
                warehouse="test-loader-warehouse-2",
                role="LOADER",
                schema="",
            ),
            id="LOADER",
        ),
        pytest.param(
            {
                "SNOWFLAKE_USER": "transformer",
                "SNOWFLAKE_PASSWORD": "transform-data",
                "SNOWFLAKE_ACCOUNT": "test-transform-account",
                "SNOWFLAKE_TRANSFORM_DATABASE": "test-transform-database",
                "SNOWFLAKE_TRANSFORM_WAREHOUSE": "test-transform-warehouse",
            },
            "TRANSFORMER",
            "",
            URL(
                user="transformer",
                password="transform-data",
                account="test-transform-account",
                database="test-transform-database",
                warehouse="test-transform-warehouse",
                role="TRANSFORMER",
                schema="",
            ),
            id="TRANSFORMER",
        ),
        pytest.param(
            {
                "SNOWFLAKE_USER": "transformer",
                "SNOWFLAKE_PASSWORD": "transform-data",
                "SNOWFLAKE_ACCOUNT": "test-transform-account",
                "SNOWFLAKE_TRANSFORM_DATABASE": "test-transform-database",
                "SNOWFLAKE_TRANSFORM_WAREHOUSE": "test-transform-warehouse",
            },
            "TRANSFORMER",
            "transform-schema",
            URL(
                user="transformer",
                password="transform-data",
                account="test-transform-account",
                database="test-transform-database",
                warehouse="test-transform-warehouse",
                role="TRANSFORMER",
                schema="transform-schema",
            ),
            id="TRANSFORMER-WITH-SCHEMA",
        ),
        pytest.param(
            {
                "PERMISSION_BOT_USER": "permission-bot",
                "PERMISSION_BOT_PASSWORD": "permission-update",
                "PERMISSION_BOT_ACCOUNT": "test-permission-account",
                "PERMISSION_BOT_DATABASE": "test-permission-database",
                "PERMISSION_BOT_WAREHOUSE": "test-permission-warehouse",
            },
            "PERMISSIONS",
            "",
            URL(
                user="permission-bot",
                password="permission-update",
                account="test-permission-account",
                database="test-permission-database",
                warehouse="test-permission-warehouse",
                role="PERMISSION_BOT",
                schema="",
            ),
            id="PERMISSION_BOT",
        ),
    ],
)
def test_snowflake_engine_factory(mocker, environment, role, schema, url):
    """
    Test that if the proper variables are passed to snowflake engine factory, then
    they are used for creating the SQLAlchemy connection.
    """
    # GIVEN: create_engine exists and ready to create connections
    mock_create_engine = mocker.patch("extract.utils.create_engine")

    # WHEN: request to create with given role
    snowflake_engine_factory(environment, role, schema)

    # THEN: expect proper keys have been used to create the engine
    mock_create_engine.assert_called_once_with(url, connect_args={"sslcompression": 0})


def test_execute_query(mocker):
    # GIVEN: an engine with a connection that executes a query and returns results
    mock_engine = mocker.MagicMock()
    mock_connection = mocker.MagicMock()
    mock_engine.connect.return_value = mock_connection
    mock_response = mocker.MagicMock()
    mock_connection.execute.return_value = mock_response
    mock_response.fetchall.return_value = [(1,)]

    # WHEN: an attempt to run a query
    result = execute_query(mock_engine, "SELECT 1")

    # THEN: results should be returned
    assert result == [(1,)]
    # THEN: expect query to have been propagated to connection
    mock_connection.execute.assert_called_once_with("SELECT 1")
    # THEN: expect connection to be closed
    mock_connection.close.assert_called_once()
    # THEN: expect engine to be disposed
    mock_engine.dispose.assert_called_once()


def test_execute_query_should_always_close_connection(mocker):
    # GIVEN: an engine with a connection that will fail to execute query
    mock_engine = mocker.MagicMock()
    mock_connection = mocker.MagicMock()
    mock_engine.connect.return_value = mock_connection
    mock_connection.execute.side_effect = SQLAlchemyError()

    # WHEN: an attempt to run a query, an exception is raised
    with pytest.raises(SQLAlchemyError):
        execute_query(mock_engine, "SELECT 1")

    # THEN: expect query to have been propagated to connection
    mock_connection.execute.assert_called_once_with("SELECT 1")
    # THEN: expect connection to be closed
    mock_connection.close.assert_called_once()
    # THEN: expect engine to be disposed
    mock_engine.dispose.assert_called_once()


def test_execute_dataframe(mocker):
    # GIVEN: an engine with a connection that executes a query and returns results
    mock_engine = mocker.MagicMock()
    mock_connection = mocker.MagicMock()
    mock_cursor = mocker.MagicMock(description=[("value",)])
    mock_engine.raw_connection.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.execute.return_value = [{"value": 1}]

    # WHEN: an attempt to run a query
    result = execute_dataframe(mock_engine, "SELECT 1")

    # THEN: expect result to be a valid dataframe
    assert_frame_equal(result, pd.DataFrame({"value": [1]}))
    # THEN: expect query to have been propagated to connection
    mock_cursor.execute.assert_called_once_with("SELECT 1")
    # THEN: expect cursor to be closed
    mock_cursor.close.assert_called_once()


def test_execute_dataframe_with_error(mocker):
    # GIVEN: an engine with a connection that fails to execute query
    mock_engine = mocker.MagicMock()
    mock_connection = mocker.MagicMock()
    mock_cursor = mocker.MagicMock(description=[("value",)])
    mock_engine.raw_connection.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.execute.side_effect = SQLAlchemyError()

    # WHEN: an attempt to run a query
    result = execute_dataframe(mock_engine, "SELECT 1")

    # THEN: nothing to be returned
    assert result is None
    # THEN: expect query to have been propagated to connection
    mock_cursor.execute.assert_called_once_with("SELECT 1")
    # THEN: expect cursor to be closed
    mock_cursor.close.assert_called_once()
