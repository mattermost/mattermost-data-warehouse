from pathlib import Path

import pandas as pd
import pytest
import sqlalchemy
from sqlalchemy.exc import ProgrammingError

from utils.cloud_clearbit import cloud_clearbit

# Load setup configuration
with open(Path(__file__).parent / "fixtures" / "clearbit" / "cloud" / "setup" / "clearbit_columns.csv") as fp:
    # List of columns in cloud_clearbit table
    CLEARBIT_COLUMNS = [line.strip() for line in fp.readlines()]


@pytest.mark.parametrize("column_names,table_data,items_to_update,clearbit_responses,expected", [
    pytest.param(
        # GIVEN: table exists
        pd.DataFrame.from_dict({"COLUMN_NAME": CLEARBIT_COLUMNS}),
        # GIVEN: table contains some data
        pd.DataFrame(),
        # GIVEN: one item to update
        pd.DataFrame.from_dict([
            {"LICENSE_EMAIL": "test@example.com", "EMAIL_DOMAIN": "example.com", "SERVER_ID": "server-0009",
             "INSTALLATION_ID": "installation-1042", "FIRST_ACTIVE_DATE": "2022-11-01",
             "LAST_UP_ADDRESS": "42.42.42.42"},
        ]),
        # GIVEN: clearbit returns data for response
        ["example.json"],
        # THEN: expect one item to have been updated
        "example.json",
        id="table exists - contains data - one item to be updated - clearbit returns data"
    ),
    pytest.param(
        # GIVEN: table exists
        pd.DataFrame.from_dict({"COLUMN_NAME": CLEARBIT_COLUMNS}),
        # GIVEN: table contains some data
        pd.DataFrame(),
        # GIVEN: two items to update
        pd.DataFrame.from_dict([
            {"LICENSE_EMAIL": "alex@alexmaccaw.com", "EMAIL_DOMAIN": "example.com", "SERVER_ID": "server-0001",
             "INSTALLATION_ID": "installation-1001", "FIRST_ACTIVE_DATE": "2022-10-01",
             "LAST_UP_ADDRESS": "123.123.123.123"},
            {"LICENSE_EMAIL": "test@example.com", "EMAIL_DOMAIN": "example.com", "SERVER_ID": "server-0009",
             "INSTALLATION_ID": "installation-1042", "FIRST_ACTIVE_DATE": "2022-11-01",
             "LAST_UP_ADDRESS": "42.42.42.42"},
        ]),
        # GIVEN: clearbit returns data for both responses
        ["clearbit.json", "example.json"],
        # THEN: expect two items to have been updated
        "all.json",
        id="table exists - contains data - two items to be updated - clearbit returns both"
    ),
    pytest.param(
        # GIVEN: table exists
        pd.DataFrame.from_dict({"COLUMN_NAME": CLEARBIT_COLUMNS}),
        # GIVEN: table contains some data
        pd.DataFrame(),
        # GIVEN: two items to update
        pd.DataFrame.from_dict([
            {"LICENSE_EMAIL": "alex@alexmaccaw.com", "EMAIL_DOMAIN": "example.com", "SERVER_ID": "server-0001",
             "INSTALLATION_ID": "installation-1001", "FIRST_ACTIVE_DATE": "2022-10-01",
             "LAST_UP_ADDRESS": "123.123.123.123"},
            {"LICENSE_EMAIL": "test@example.com", "EMAIL_DOMAIN": "example.com", "SERVER_ID": "server-0009",
             "INSTALLATION_ID": "installation-1042", "FIRST_ACTIVE_DATE": "2022-11-01",
             "LAST_UP_ADDRESS": "42.42.42.42"},
        ]),
        # GIVEN: clearbit fails on first item but returns for the second
        [None, "example.json"],
        # THEN: expect one item to have been updated
        "all-with-failure.json",
        id="table exists - contains data - two items to be updated - clearbit returns one"
    ),
    pytest.param(
        # GIVEN: table exists
        pd.DataFrame.from_dict({"COLUMN_NAME": CLEARBIT_COLUMNS}),
        # GIVEN: table doesn't contain data
        None,
        # GIVEN: two items to update
        pd.DataFrame.from_dict([
            {"LICENSE_EMAIL": "alex@alexmaccaw.com", "EMAIL_DOMAIN": "example.com", "SERVER_ID": "server-0001",
             "INSTALLATION_ID": "installation-1001", "FIRST_ACTIVE_DATE": "2022-10-01",
             "LAST_UP_ADDRESS": "123.123.123.123"},
            {"LICENSE_EMAIL": "test@example.com", "EMAIL_DOMAIN": "example.com", "SERVER_ID": "server-0009",
             "INSTALLATION_ID": "installation-1042", "FIRST_ACTIVE_DATE": "2022-11-01",
             "LAST_UP_ADDRESS": "42.42.42.42"},
        ]),
        # GIVEN: clearbit returns data only for one of the two items
        ["clearbit.json", "example.json"],
        # THEN: expect two items to have been updated
        "all.json",
        id="table exists - no data - two items to be updated - clearbit returns both"
    ),
    pytest.param(
        # GIVEN: table doesn't exist
        sqlalchemy.exc.ProgrammingError("SELECT 1", {}, None),
        # GIVEN: table doesn't contain data
        sqlalchemy.exc.ProgrammingError("SELECT 1", {}, None),
        # GIVEN: one item to update
        pd.DataFrame.from_dict([
            {"LICENSE_EMAIL": "test@example.com", "EMAIL_DOMAIN": "example.com", "SERVER_ID": "server-0009",
             "INSTALLATION_ID": "installation-1042", "FIRST_ACTIVE_DATE": "2022-11-01",
             "LAST_UP_ADDRESS": "42.42.42.42"},
        ]),
        # GIVEN: clearbit returns data for single responses
        ["example.json"],
        # THEN: expect one item to have been updated
        "example-new-table.json",
        id="table doesn't exist - no data - one item to be updated - clearbit returns one"
    ),
    pytest.param(
        # GIVEN: table exists
        pd.DataFrame.from_dict({"COLUMN_NAME": CLEARBIT_COLUMNS}),
        # GIVEN: table contains some data
        pd.DataFrame(),
        # GIVEN: two items to update
        pd.DataFrame.from_dict([
            {"LICENSE_EMAIL": "test1@example.com", "EMAIL_DOMAIN": "example.com", "SERVER_ID": "server-0010",
             "INSTALLATION_ID": "installation-1042", "FIRST_ACTIVE_DATE": "2022-11-01",
             "LAST_UP_ADDRESS": "42.42.42.42"},
            {"LICENSE_EMAIL": "test2@example.com", "EMAIL_DOMAIN": "example.com", "SERVER_ID": "server-0011",
             "INSTALLATION_ID": "installation-1042", "FIRST_ACTIVE_DATE": "2022-11-01",
             "LAST_UP_ADDRESS": "42.42.42.42"},
        ]),
        # GIVEN: clearbit returns data for both responses, first response doesn't contain company info
        ["person-only.json", "company-only.json"],
        # THEN: expect two items to have been updated
        "person-company.json",
        id="table exists - contains data - three items to be updated - clearbit returns all"
    ),
])
def test_cloud_clearbit(mock_snowflake, mock_snowflake_pandas, mock_clearbit, mock_clearbit_enrichments, expect_data,
                        column_names, table_data, items_to_update, clearbit_responses, expected):
    # GIVEN: snowflake engine and connection are mocked
    mock_snowflake("utils.cloud_clearbit")
    # GIVEN: snowflake pandas interactions are mocked
    mock_exec_df, mock_to_sql = mock_snowflake_pandas("utils.cloud_clearbit")
    # GIVEN: mock clearbit client
    mock_clearbit = mock_clearbit("utils.cloud_clearbit")

    mock_exec_df.side_effect = [
        # GIVEN: does clearbit table already exists? (first request to load dataframe)
        column_names,
        # GIVEN: clearbit table contains data? (second request to load dataframe)
        table_data,
        # GIVEN: items to update
        items_to_update
    ]
    # GIVEN: clearbit returns two enrichments
    mock_clearbit.Enrichment.find.side_effect = mock_clearbit_enrichments(*clearbit_responses)

    # WHEN: request to cloud clearbit
    cloud_clearbit()

    # THEN: expect two items matching existing columns to be loaded to Snowflake
    # Note that column order is ignored.
    pd.testing.assert_frame_equal(
        mock_to_sql.call_args_list[0][0][0].sort_index(axis=1),
        expect_data(expected).sort_index(axis=1),
        check_dtype=False
    )
    # THEN: expect calls to clearbit to be equal to number of items to update
    assert mock_clearbit.Enrichment.find.call_count == len(items_to_update)


@pytest.mark.parametrize("column_names,table_data,items_to_update,clearbit_responses", [
    pytest.param(
        # GIVEN: table exists
        pd.DataFrame.from_dict({"COLUMN_NAME": CLEARBIT_COLUMNS}),
        # GIVEN: table contains some data
        pd.DataFrame(),
        # GIVEN: one item to update
        pd.DataFrame.from_dict([
            {"LICENSE_EMAIL": "alex@alexmaccaw.com", "EMAIL_DOMAIN": "example.com", "SERVER_ID": "server-0001",
             "INSTALLATION_ID": "installation-1001", "FIRST_ACTIVE_DATE": "2022-10-01",
             "LAST_UP_ADDRESS": "123.123.123.123"},
            {"LICENSE_EMAIL": "test@example.com", "EMAIL_DOMAIN": "example.com", "SERVER_ID": "server-0009",
             "INSTALLATION_ID": "installation-1042", "FIRST_ACTIVE_DATE": "2022-11-01",
             "LAST_UP_ADDRESS": "42.42.42.42"},
        ]),
        # GIVEN: clearbit doesn't return data
        [None, None],
        # THEN: expect no items to be updated
        id="table exists - has data - one item to be updated - clearbit doesn't return data"
    ),
    pytest.param(
        # GIVEN: table exists
        pd.DataFrame.from_dict({"COLUMN_NAME": CLEARBIT_COLUMNS}),
        # GIVEN: table contains some data
        pd.DataFrame(),
        # GIVEN: no items to update
        pd.DataFrame(),
        # GIVEN: clearbit doesn't return data
        [],
        # THEN: expect no items to be updated
        id="table exists - has data - no items to be updated - clearbit doesn't return data"
    ),
])
def test_cloud_clearbit_nothing_to_store(mock_snowflake, mock_snowflake_pandas, mock_clearbit,
                                         mock_clearbit_enrichments,
                                         column_names, table_data, items_to_update, clearbit_responses):
    # GIVEN: snowflake engine and connection are mocked
    mock_snowflake("utils.cloud_clearbit")
    # GIVEN: snowflake pandas interactions are mocked
    mock_exec_df, mock_to_sql = mock_snowflake_pandas("utils.cloud_clearbit")
    # GIVEN: mock clearbit client
    mock_clearbit = mock_clearbit("utils.cloud_clearbit")

    mock_exec_df.side_effect = [
        # GIVEN: does clearbit table already exists? (first request to load dataframe)
        column_names,
        # GIVEN: clearbit table contains data (second request to load dataframe)
        table_data,
        # GIVEN: there are two new items to be updated from clearbit
        items_to_update
    ]
    # GIVEN: clearbit returns no results
    mock_clearbit.Enrichment.find.side_effect = mock_clearbit_enrichments(*clearbit_responses)

    # WHEN: request to cloud clearbit
    cloud_clearbit()

    # THEN: expect no call to store the data
    # TODO: This is inconsistent with the case where only few of many fail. In that case failed records are stored with
    # TODO: null values in the columns
    mock_to_sql.assert_not_called()


@pytest.fixture()
def expect_data(expect_data):
    from functools import partial
    return partial(expect_data, "cloud")

