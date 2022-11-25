from pathlib import Path

import pandas as pd
import pytest
import sqlalchemy
from sqlalchemy.exc import ProgrammingError

from utils.onprem_clearbit import onprem_clearbit

# Load setup configuration
with open(
    Path(__file__).parent
    / "fixtures"
    / "clearbit"
    / "onprem"
    / "setup"
    / "clearbit_columns.csv"
) as fp:
    # List of columns in cloud_clearbit table
    CLEARBIT_COLUMNS = [line.strip() for line in fp.readlines()]


@pytest.mark.parametrize(
    "column_names,table_data,exception_data,items_to_update,clearbit_responses,expected_data,expected_exceptions",
    [
        pytest.param(
            # GIVEN: table exists
            pd.DataFrame.from_dict({"COLUMN_NAME": CLEARBIT_COLUMNS}),
            # GIVEN: table contains some data
            pd.DataFrame(),
            # GIVEN: No exception data
            None,
            # GIVEN: one item to update
            pd.DataFrame.from_dict(
                [
                    {
                        "SERVER_ID": "server-1",
                        "INSTALLATION_ID": None,
                        "FIRST_ACTIVE_DATE": "2022-11-08 09:18:04.076",
                        "LAST_IP_ADDRESS": "42.42.42.42",
                    },
                ]
            ),
            # GIVEN: clearbit returns data for response
            ["42.42.42.42.json"],
            # THEN: expect one item to have been updated
            "42.42.42.42.json",
            # THEN: expect no exceptions,
            None,
            id="table exists - contains data but not exceptions - one item to be updated - clearbit returns data",
        ),
        pytest.param(
            # GIVEN: table exists
            pd.DataFrame.from_dict({"COLUMN_NAME": CLEARBIT_COLUMNS}),
            # GIVEN: table contains some data
            pd.DataFrame(),
            # GIVEN: No exception data
            None,
            # GIVEN: two items to update
            pd.DataFrame.from_dict(
                [
                    {
                        "SERVER_ID": "server-1",
                        "INSTALLATION_ID": None,
                        "FIRST_ACTIVE_DATE": "2022-11-08 09:18:04.076",
                        "LAST_IP_ADDRESS": "42.42.42.42",
                    },
                    {
                        "SERVER_ID": "server-2",
                        "INSTALLATION_ID": None,
                        "FIRST_ACTIVE_DATE": "2022-11-08 09:18:04.076",
                        "LAST_IP_ADDRESS": "1.1.1.1",
                    },
                ]
            ),
            # GIVEN: clearbit returns data for item
            ["42.42.42.42.json", "1.1.1.1.json"],
            # THEN: expect one item to have been updated
            "all.json",
            # THEN: expect no exceptions,
            None,
            id="table exists - contains data but not exceptions - two items to be updated - clearbit returns data",
        ),
        pytest.param(
            # GIVEN: table exists
            pd.DataFrame.from_dict({"COLUMN_NAME": CLEARBIT_COLUMNS}),
            # GIVEN: table contains some data
            pd.DataFrame(),
            # GIVEN: exception data
            pd.DataFrame(),
            # GIVEN: two items to update
            pd.DataFrame.from_dict(
                [
                    {
                        "SERVER_ID": "server-1",
                        "INSTALLATION_ID": None,
                        "FIRST_ACTIVE_DATE": "2022-11-08 09:18:04.076",
                        "LAST_IP_ADDRESS": "42.42.42.42",
                    },
                    {
                        "SERVER_ID": "server-2",
                        "INSTALLATION_ID": None,
                        "FIRST_ACTIVE_DATE": "2022-11-08 09:18:04.076",
                        "LAST_IP_ADDRESS": "1.1.1.1",
                    },
                ]
            ),
            # GIVEN: clearbit returns data for both items
            ["42.42.42.42.json", "1.1.1.1.json"],
            # THEN: expect one item to have been updated
            "all.json",
            # THEN: expect no exceptions,
            None,
            id="table exists - contains data and exceptions - two items to be updated - clearbit returns data",
        ),
        pytest.param(
            # GIVEN: table exists
            pd.DataFrame.from_dict({"COLUMN_NAME": CLEARBIT_COLUMNS}),
            # GIVEN: table contains some data
            pd.DataFrame(),
            # GIVEN: No exception data
            None,
            # GIVEN: two items to update
            pd.DataFrame.from_dict(
                [
                    {
                        "SERVER_ID": "server-1",
                        "INSTALLATION_ID": None,
                        "FIRST_ACTIVE_DATE": "2022-11-08 09:18:04.076",
                        "LAST_IP_ADDRESS": "42.42.42.42",
                    },
                    {
                        "SERVER_ID": "server-2",
                        "INSTALLATION_ID": None,
                        "FIRST_ACTIVE_DATE": "2022-11-08 09:18:04.076",
                        "LAST_IP_ADDRESS": "1.1.1.1",
                    },
                ]
            ),
            # GIVEN: clearbit returns data for one item
            ["42.42.42.42.json", None],
            # THEN: expect one item to have been updated
            "all-with-failures.json",
            # THEN: expect no exceptions,
            None,
            id="table exists - contains data but not exceptions - two items to be updated - clearbit returns data on one",
        ),
        pytest.param(
            # GIVEN: table doesn't exist
            sqlalchemy.exc.ProgrammingError("SELECT 1", {}, None),
            # GIVEN: can't get data because table doesn't exist
            sqlalchemy.exc.ProgrammingError("SELECT 1", {}, None),
            # GIVEN: No exception data
            sqlalchemy.exc.ProgrammingError("SELECT 1", {}, None),
            # GIVEN: one item to update
            pd.DataFrame.from_dict(
                [
                    {
                        "SERVER_ID": "server-1",
                        "INSTALLATION_ID": None,
                        "FIRST_ACTIVE_DATE": "2022-11-08 09:18:04.076",
                        "LAST_IP_ADDRESS": "42.42.42.42",
                    }
                ]
            ),
            # GIVEN: clearbit returns data for one item
            ["42.42.42.42.json"],
            # THEN: expect one item to have been updated
            "42.42.42.42-new-table.json",
            # THEN: expect no exceptions,
            None,
            id="table not exists - no data or exceptions - two items to be updated - clearbit returns data",
            marks=pytest.mark.xfail(
                reason="onprem expects column cloud but it's not there - todo: fix"
            ),
        ),
        pytest.param(
            # GIVEN: table exists
            pd.DataFrame.from_dict({"COLUMN_NAME": CLEARBIT_COLUMNS}),
            # GIVEN: table doesn't contain data
            pd.DataFrame(),
            # GIVEN: No exception table
            sqlalchemy.exc.ProgrammingError("SELECT 1", {}, None),
            # GIVEN: two items to update
            pd.DataFrame.from_dict(
                [
                    {
                        "SERVER_ID": "server-1",
                        "INSTALLATION_ID": None,
                        "FIRST_ACTIVE_DATE": "2022-11-08 09:18:04.076",
                        "LAST_IP_ADDRESS": "42.42.42.42",
                    },
                    {
                        "SERVER_ID": "server-2",
                        "INSTALLATION_ID": None,
                        "FIRST_ACTIVE_DATE": "2022-11-08 09:18:04.076",
                        "LAST_IP_ADDRESS": "1.1.1.1",
                    },
                ]
            ),
            # GIVEN: clearbit returns data for one item
            ["42.42.42.42.json", "1.1.1.1.json"],
            # THEN: expect one item to have been updated
            "all.json",
            # THEN: expect no exceptions,
            None,
            id="table exists - data but not exceptions - two items to be updated - clearbit returns data",
        ),
    ],
)
def test_onprem_clearbit(
    mock_snowflake,
    mock_snowflake_pandas,
    mock_clearbit,
    mock_clearbit_reveal,
    expect_data,
    column_names,
    table_data,
    exception_data,
    items_to_update,
    clearbit_responses,
    expected_data,
    expected_exceptions,
):
    # GIVEN: snowflake engine and connection are mocked
    _, _, mock_execute_query = mock_snowflake("utils.onprem_clearbit")
    # GIVEN: snowflake pandas interactions are mocked
    mock_exec_df, mock_to_sql = mock_snowflake_pandas("utils.onprem_clearbit")
    # GIVEN: mock clearbit client
    mock_clearbit = mock_clearbit("utils.onprem_clearbit")

    mock_exec_df.side_effect = [
        # GIVEN: does clearbit table already exists? (first request to load dataframe)
        column_names,
        # GIVEN: clearbit table contains data? (second request to load dataframe)
        table_data,
        # GIVEN: clearbit exception table contains data? (second request to load dataframe)
        exception_data,
        # GIVEN: items to update
        items_to_update,
    ]

    # GIVEN: clearbit returns provided reveal responses
    mock_clearbit.Reveal.find.side_effect = mock_clearbit_reveal(*clearbit_responses)

    # WHEN: request to cloud clearbit
    onprem_clearbit()

    # THEN: compare expected clearbit data vs data loaded to snowflake
    pd.testing.assert_frame_equal(
        mock_to_sql.call_args_list[0][0][0].sort_index(axis=1),
        expect_data(expected_data).sort_index(axis=1),
        check_dtype=False,
    )
    # THEN: compare expected clearbit exception data vs data loaded to snowflake
    pd.testing.assert_frame_equal(
        mock_to_sql.call_args_list[1][0][0].sort_index(axis=1),
        # Load data if expected exceptions otherwise expect empty dataframe
        expect_data(expected_exceptions).sort_index(axis=1)
        if expected_exceptions
        else pd.DataFrame(columns=["server_id"]),
        check_dtype=False,
    )

    # THEN: expect calls to clearbit to be equal to number of items to update
    assert mock_clearbit.Reveal.find.call_count == len(items_to_update)


@pytest.mark.parametrize(
    "column_names,table_data,exception_data,items_to_update,clearbit_responses",
    [
        pytest.param(
            # GIVEN: table exists
            pd.DataFrame.from_dict({"COLUMN_NAME": CLEARBIT_COLUMNS}),
            # GIVEN: table contains some data
            pd.DataFrame(),
            # GIVEN: No exception data
            None,
            # GIVEN: two items to update
            pd.DataFrame.from_dict(
                [
                    {
                        "SERVER_ID": "server-1",
                        "INSTALLATION_ID": None,
                        "FIRST_ACTIVE_DATE": "2022-11-08 09:18:04.076",
                        "LAST_IP_ADDRESS": "42.42.42.42",
                    },
                    {
                        "SERVER_ID": "server-2",
                        "INSTALLATION_ID": None,
                        "FIRST_ACTIVE_DATE": "2022-11-08 09:18:04.076",
                        "LAST_IP_ADDRESS": "1.1.1.1",
                    },
                ]
            ),
            # GIVEN: clearbit returns no data
            [None, None],
            id="two items to be updated - clearbit does not return data",
        ),
        pytest.param(
            # GIVEN: table exists
            pd.DataFrame.from_dict({"COLUMN_NAME": CLEARBIT_COLUMNS}),
            # GIVEN: table contains some data
            pd.DataFrame(),
            # GIVEN: No exception data
            None,
            # GIVEN: no items to update
            pd.DataFrame(),
            # GIVEN: clearbit is not called
            [],
            id="no items to be updated",
        ),
    ],
)
def test_onprem_clearbit_no_data_to_update(
    mock_snowflake,
    mock_snowflake_pandas,
    mock_clearbit,
    mock_clearbit_reveal,
    expect_data,
    column_names,
    table_data,
    exception_data,
    items_to_update,
    clearbit_responses,
):
    # GIVEN: snowflake engine and connection are mocked
    _, _, mock_execute_query = mock_snowflake("utils.onprem_clearbit")
    # GIVEN: snowflake pandas interactions are mocked
    mock_exec_df, mock_to_sql = mock_snowflake_pandas("utils.onprem_clearbit")
    # GIVEN: mock clearbit client
    mock_clearbit = mock_clearbit("utils.onprem_clearbit")

    mock_exec_df.side_effect = [
        # GIVEN: does clearbit table already exists? (first request to load dataframe)
        column_names,
        # GIVEN: clearbit table contains data? (second request to load dataframe)
        table_data,
        # GIVEN: clearbit exception table contains data? (second request to load dataframe)
        exception_data,
        # GIVEN: items to update
        items_to_update,
    ]

    # GIVEN: clearbit returns provided reveal responses
    mock_clearbit.Reveal.find.side_effect = mock_clearbit_reveal(*clearbit_responses)

    # WHEN: request to cloud clearbit
    onprem_clearbit()

    # THEN: expect no call to store the data
    # TODO: This is inconsistent with the case where only few of many fail. In that case failed records are stored with
    # TODO: null values in the columns
    mock_to_sql.assert_not_called()

    # THEN: expect calls to clearbit to be equal to number of items to update
    assert mock_clearbit.Reveal.find.call_count == len(items_to_update)


@pytest.fixture()
def expect_data(expect_data):
    from functools import partial

    return partial(expect_data, "onprem")
