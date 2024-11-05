from contextlib import contextmanager
from datetime import datetime, timezone
from textwrap import dedent

import pandas as pd
import pytest
from mock.mock import call

from utils.db.helpers import (
    TableStats,
    clone_table,
    get_table_stats_for_schema,
    load_query,
    load_table_definition,
    merge_event_delta_table_into,
    move_table,
    snowflake_engine,
    upload_csv_as_table,
    validate_object_name,
)


@contextmanager
def does_not_raise():
    yield


@pytest.mark.parametrize(
    "config,expected_url",
    [
        pytest.param(
            {
                "SNOWFLAKE_ACCOUNT": "test-account",
                "SNOWFLAKE_USER": "admin",
                "SNOWFLAKE_PASSWORD": "safe",
                "SNOWFLAKE_DATABASE": "test-database",
                "SNOWFLAKE_SCHEMA": "test-schema",
                "SNOWFLAKE_WAREHOUSE": "test-warehouse",
                "SNOWFLAKE_ROLE": "test-role",
            },
            "snowflake://admin:safe@test-account/test-database/test-schema?role=test-role&warehouse=test-warehouse",
            id="all properties",
        ),
        pytest.param(
            {
                "SNOWFLAKE_ACCOUNT": "test-account",
                "SNOWFLAKE_USER": "admin",
                "SNOWFLAKE_PASSWORD": "safe",
                "SNOWFLAKE_DATABASE": "test-database",
                "SNOWFLAKE_SCHEMA": "test-schema",
                "SNOWFLAKE_WAREHOUSE": "test-warehouse",
            },
            "snowflake://admin:safe@test-account/test-database/test-schema?warehouse=test-warehouse",
            id="missing role",
        ),
        pytest.param(
            {
                "SNOWFLAKE_ACCOUNT": "test-account",
                "SNOWFLAKE_USER": "admin",
                "SNOWFLAKE_PASSWORD": "safe",
                "SNOWFLAKE_DATABASE": "test-database",
                "SNOWFLAKE_WAREHOUSE": "test-warehouse",
                "SNOWFLAKE_ROLE": "test-role",
            },
            "snowflake://admin:safe@test-account/test-database?role=test-role&warehouse=test-warehouse",
            id="missing schema",
        ),
        pytest.param(
            {
                "SNOWFLAKE_ACCOUNT": "test-account",
                "SNOWFLAKE_USER": "admin",
                "SNOWFLAKE_PASSWORD": "safe",
            },
            "snowflake://admin:safe@test-account/",
            id="basic properties",
        ),
    ],
)
def test_should_pass_connection_details(mocker, config, expected_url):
    """
    Test that if the proper variables are passed to SQL alchemy engine factory, then
    they are used for creating the SQLAlchemy connection.
    """
    # GIVEN: create_engine exists and ready to create connections
    mock_create_engine = mocker.patch("utils.db.helpers.create_engine")

    # WHEN: request to create with given role
    snowflake_engine(config)

    # THEN: expect proper keys have been used to create the engine
    mock_create_engine.assert_called_once_with(expected_url, connect_args={"sslcompression": 0})


def test_should_return_table_stats(mocker):
    # GIVEN: mock engine and mock connection
    mock_engine = mocker.MagicMock()
    mock_connection = mocker.MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_connection
    # GIVEN: query will return results
    mock_connection.execute.return_value = [
        ('schema', 'table1', 3, datetime(2022, 12, 28, 23, 55, 59, tzinfo=timezone.utc)),
        ('schema', 'table2', 20, datetime(2023, 1, 15, 12, 23, 45, tzinfo=timezone.utc)),
    ]

    # WHEN: request to get table stats
    result = get_table_stats_for_schema(mock_engine, 'database', 'schema')

    # THEN: expect two rows to have been returned
    assert result == [
        TableStats('schema', 'table1', 3, datetime(2022, 12, 28, 23, 55, 59, tzinfo=timezone.utc)),
        TableStats('schema', 'table2', 20, datetime(2023, 1, 15, 12, 23, 45, tzinfo=timezone.utc)),
    ]


def test_should_upload_file_to_table(mocker):
    # GIVEN: mock engine and mock connection
    mock_engine = mocker.MagicMock()
    mock_connection = mocker.MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_connection

    # WHEN: request to get upload a file to a table
    upload_csv_as_table(mock_engine, '/tmp/upload.csv', 'test-schema', 'a-table')

    # THEN: expect the script to has been called.
    mock_connection.execute.assert_has_calls(
        [
            call('TRUNCATE TABLE test-schema.a-table'),
            call('CREATE TEMPORARY STAGE IF NOT EXISTS test-schema.a-table'),
            call('PUT file:///tmp/upload.csv @test-schema.a-table OVERWRITE=TRUE'),
            call(
                "COPY INTO test-schema.a-table FROM @test-schema.a-table FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"')"  # noqa: E501
            ),  # noqa: E501
        ]
    )


def test_should_move_table(mocker):
    # GIVEN: mock connection
    mock_connection = mocker.MagicMock()
    mock_connection.execute = mocker.MagicMock()

    # WHEN: request to move table
    move_table(mock_connection, "table", "source-db", "source-schema", "target-db", "target-schema")

    # THEN: expect an alter table
    mock_connection.execute.assert_called_once_with(
        '''
        ALTER TABLE source-db.source-schema.table
        RENAME TO target-db.target-schema.table
    '''
    )


def test_should_move_table_with_postfix(mocker):
    # GIVEN: mock connection
    mock_connection = mocker.MagicMock()
    mock_connection.execute = mocker.MagicMock()

    # WHEN: request to move table
    move_table(mock_connection, "table", "source-db", "source-schema", "target-db", "target-schema", postfix="_old")

    # THEN: expect an alter table
    mock_connection.execute.assert_called_once_with(
        '''
        ALTER TABLE source-db.source-schema.table
        RENAME TO target-db.target-schema.table_old
    '''
    )


@pytest.mark.parametrize(
    "example_input,expectation",
    [
        ("hello", does_not_raise()),
        ("Hello123", does_not_raise()),
        ("1000", does_not_raise()),
        ("_", does_not_raise()),
        ("hello_123", does_not_raise()),
        ("hello world", pytest.raises(ValueError)),
        ("hello-world", pytest.raises(ValueError)),
    ],
)
def test_validate_object_name(example_input, expectation):
    with expectation:
        validate_object_name("field", example_input)


def test_should_clone_table_without_dropping(mocker):
    # GIVEN: mock connection
    mock_connection = mocker.MagicMock()
    mock_connection.execute = mocker.MagicMock()

    # WHEN: request to clone a table
    clone_table(
        mock_connection, "source_db", "source_schema", "source_table", "target_db", "target_schema", "target_table"
    )

    # THEN: expect an alter table
    mock_connection.execute.assert_called_once_with(
        dedent(
            '''
            CREATE TABLE target_db.target_schema.target_table
            CLONE source_db.source_schema.source_table
        '''
        )
    )


def test_should_clone_table_with_replace(mocker):
    # GIVEN: mock connection
    mock_connection = mocker.MagicMock()
    mock_connection.execute = mocker.MagicMock()

    # WHEN: request to move table
    clone_table(
        mock_connection,
        "source_db",
        "source_schema",
        "source_table",
        "target_db",
        "target_schema",
        "target_table",
        replace_if_exists=True,
    )

    # THEN: expect an alter table
    mock_connection.execute.assert_called_once_with(
        dedent(
            '''
            CREATE OR REPLACE TABLE target_db.target_schema.target_table
            CLONE source_db.source_schema.source_table
        '''
        )
    )


def test_load_table_definition(sqlalchemy_memory_engine):
    # GIVEN: a clean database
    with sqlalchemy_memory_engine.connect() as conn:
        # GIVEN: the schema exists
        conn.execute("ATTACH DATABASE ':memory:' AS 'test_schema'")

        # GIVEN: a table exists
        conn.execute('CREATE TABLE test_schema.test_table (id INTEGER , name VARCHAR(255))')

        # WHEN: request to load the table definition
        result = load_table_definition(conn, 'test_schema', 'test_table')

        # THEN: expect the table to have the correct columns
        assert result.columns.keys() == ['id', 'name']
        assert str(result.columns['id'].type) == 'INTEGER'
        assert str(result.columns['name'].type) == 'VARCHAR(255)'


def test_load_table_definition_unknown_table(sqlalchemy_memory_engine):
    # GIVEN: a clean database
    with sqlalchemy_memory_engine.connect() as conn:
        # GIVEN: the schema exists without any tables
        conn.execute("ATTACH DATABASE ':memory:' AS 'test_schema'")

        # WHEN: request to load the table definition
        result = load_table_definition(conn, 'test_schema', 'test_table')

        # THEN: expect no result
        assert result is None


def test_should_merge_table_with_same_structure(mocker, base_table, delta_table_1):
    # GIVEN: mock connection
    mock_connection = mocker.MagicMock()
    mock_exec = mocker.MagicMock()
    mock_connection.execute = mock_exec

    # GIVEN: deferred merge tables exist
    mocker.patch('utils.db.helpers.load_table_definition', side_effect=[base_table, delta_table_1])

    # GIVEN: delta table contains data
    mock_exec.return_value.scalar.return_value = datetime(2022, 12, 28, 23, 55, 59)

    # WHEN: request to clone a table
    merge_event_delta_table_into(mock_connection, "base_schema", "base_table", "delta_schema", "delta_table")

    # THEN: expect a merge statement according to the spec of the tables
    assert (
        mock_exec.call_args_list[1].args[0].text == 'MERGE INTO base_schema.base_table USING delta_schema.delta_table '
        'ON base_schema.base_table.id = delta_schema.delta_table.id '
        'AND base_schema.base_table.received_at >= :first_duplicate_date '
        'WHEN MATCHED THEN UPDATE SET after = base_table.after '
        'WHEN NOT MATCHED THEN INSERT (id, column_a, "_", after, received_at) '
        'VALUES (delta_table.id, delta_table.column_a, delta_table."_", delta_table.after, delta_table.received_at)'
    )

    # THEN: expect the merge statement to have the correct parameters
    assert mock_exec.call_args_list[1].args[0].compile().params == {"first_duplicate_date": "2022-12-21T23:55:59"}
    # THEN: expect a delete statement
    assert (
        mock_exec.call_args_list[2].args[0].text == 'DELETE FROM delta_schema.delta_table '
        'WHERE id IN (SELECT id FROM base_schema.base_table WHERE received_at >= :first_duplicate_date)'
    )

    # THEN: expect the delete statement to have the correct parameters
    assert mock_exec.call_args_list[2].args[0].compile().params == {"first_duplicate_date": "2022-12-21T23:55:59"}


def test_should_not_merge_table_if_delta_empty(mocker, base_table, delta_table_1):
    # GIVEN: mock connection
    mock_connection = mocker.MagicMock()
    mock_exec = mocker.MagicMock()
    mock_connection.execute = mock_exec

    # GIVEN: deferred merge tables exist
    mocker.patch('utils.db.helpers.load_table_definition', side_effect=[base_table, delta_table_1])

    # GIVEN: delta table does not contain data
    mock_exec.return_value.scalar.return_value = None

    # WHEN: request to clone a table
    merge_event_delta_table_into(mock_connection, "base_schema", "base_table", "delta_schema", "delta_table")

    # THEN: expect no merge query
    assert mock_exec.call_count == 1


def test_should_not_merge_table_if_delta_missing(mocker, base_table):
    # GIVEN: mock connection
    mock_connection = mocker.MagicMock()
    mock_exec = mocker.MagicMock()
    mock_connection.execute = mock_exec

    # GIVEN: deferred merge tables do not exist
    mocker.patch('utils.db.helpers.load_table_definition', side_effect=[base_table, None])

    # GIVEN: delta table does not contain data
    mock_exec.return_value.scalar.return_value = None

    # WHEN: request to clone a table
    with pytest.raises(ValueError) as e:
        # THEN: expect to raise an error
        merge_event_delta_table_into(mock_connection, "base_schema", "base_table", "delta_schema", "delta_table")

    assert e.value.args[0] == 'Base and/or delta table does not exist!'


def test_should_add_new_columns_and_merge_table(mocker, base_table, delta_table_2):
    # GIVEN: mock connection
    mock_connection = mocker.MagicMock()
    mock_exec = mocker.Mock()
    mock_connection.execute = mock_exec

    # GIVEN: deferred merge tables exist
    # GIVEN: delta table contains two extra columns
    mocker.patch('utils.db.helpers.load_table_definition', side_effect=[base_table, delta_table_2])

    # GIVEN: delta table contains data
    mock_exec.return_value.scalar.return_value = datetime(2022, 12, 28, 23, 55, 59)

    # WHEN: request to clone a table
    merge_event_delta_table_into(mock_connection, "base_schema", "base_table", "delta_schema", "delta_table")

    # THEN: expect base statement structure to be updated with the new columns
    mock_exec.assert_any_call('ALTER TABLE base_schema.base_table ADD COLUMN column_b INTEGER, extra VARCHAR(255)')

    # THEN: expect a merge statement according to the spec of the tables
    assert (
        mock_exec.call_args_list[2].args[0].text == 'MERGE INTO base_schema.base_table USING delta_schema.delta_table '
        'ON base_schema.base_table.id = delta_schema.delta_table.id '
        'AND base_schema.base_table.received_at >= :first_duplicate_date '
        'WHEN MATCHED THEN UPDATE SET after = base_table.after '
        'WHEN NOT MATCHED THEN INSERT (id, column_a, "_", after, received_at, extra, column_b) '
        'VALUES (delta_table.id, delta_table.column_a, delta_table."_", delta_table.after, delta_table.received_at,'
        ' delta_table.extra, delta_table.column_b)'
    )

    # THEN: expect the merge statement to have the correct parameters
    assert mock_exec.call_args_list[2].args[0].compile().params == {"first_duplicate_date": "2022-12-21T23:55:59"}

    # THEN: expect a delete statement
    assert (
        mock_exec.call_args_list[3].args[0].text == 'DELETE FROM delta_schema.delta_table '
        'WHERE id IN (SELECT id FROM base_schema.base_table WHERE received_at >= :first_duplicate_date)'
    )

    # THEN: expect the delete statement to have the correct parameters
    assert mock_exec.call_args_list[3].args[0].compile().params == {"first_duplicate_date": "2022-12-21T23:55:59"}


def test_load_query(sqlalchemy_memory_engine, test_data):
    with sqlalchemy_memory_engine.connect() as conn, conn.begin():
        df = load_query(conn, "SELECT id, title FROM books")
        pd.testing.assert_frame_equal(df, test_data)
