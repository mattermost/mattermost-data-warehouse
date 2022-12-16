import pandas as pd

from utils.user_agent_parser import CREATE_TABLE_QUERY, GET_USER_AGENT_STRINGS_QUERY, parse_user_agent


def test_parse_user_agent(mocker, user_agent_input, user_agent_df):
    # GIVEN: mock operations to snowflake
    mock_engine_factory = mocker.patch("utils.user_agent_parser.snowflake_engine_factory")
    mock_engine = mocker.MagicMock()
    mock_engine_factory.return_value = mock_engine
    mock_connection = mocker.MagicMock()
    mock_engine.connect.return_value = mock_connection
    mock_execute_query = mocker.patch("utils.user_agent_parser.execute_query")
    mock_execute_df = mocker.patch("utils.user_agent_parser.execute_dataframe")
    mock_execute_df.return_value = pd.DataFrame({"CONTEXT_USERAGENT": user_agent_input})
    # Capture calls to pandas' to_sql. This is a trick to capture the dataframe that is created internally.
    # TODO: separate processing and storing logic in user_agent_parser.py
    mock_to_sql = mocker.patch("pandas.io.sql.to_sql")

    # WHEN: trigger user agent parsing
    parse_user_agent()

    # THEN: expect attempt to create table if not exists
    mock_execute_query.assert_called_once_with(mock_engine, CREATE_TABLE_QUERY)
    # THEN: expect attempt to load user agent strings
    mock_execute_df.assert_called_once_with(mock_engine, GET_USER_AGENT_STRINGS_QUERY)
    # THEN: expect to have saved a single batch to database
    mock_to_sql.assert_called_once()
    # THEN: expect user agents have been parsed correctly - this is the most important check regarding data
    pd.testing.assert_frame_equal(mock_to_sql.call_args_list[0][0][0], user_agent_df)


def test_parse_user_agent_batching(mocker):
    # GIVEN: mock operations to snowflake
    mock_engine_factory = mocker.patch("utils.user_agent_parser.snowflake_engine_factory")
    mock_engine = mocker.MagicMock()
    mock_engine_factory.return_value = mock_engine
    mock_connection = mocker.MagicMock()
    mock_engine.connect.return_value = mock_connection
    mock_execute_query = mocker.patch("utils.user_agent_parser.execute_query")
    mock_execute_df = mocker.patch("utils.user_agent_parser.execute_dataframe")
    # GIVEN: 32k user agents (enough for 2 batches)
    mock_execute_df.return_value = pd.DataFrame(
        {
            "CONTEXT_USERAGENT": [
                "Mozilla/5.0 "
                f"(Linux; Android {i}.0; Nexus 6 Build/NBD90Z) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/53.0.2785.124 Mobile Safari/537.36"
                for i in range(1, 32_000)
            ]
        }
    )
    mock_to_sql = mocker.patch("pandas.io.sql.to_sql")

    # WHEN: trigger user agent parsing
    parse_user_agent()

    # THEN: expect attempt to create table if not exists
    mock_execute_query.assert_called_once_with(mock_engine, CREATE_TABLE_QUERY)
    # THEN: expect attempt to load user agent strings
    mock_execute_df.assert_called_once_with(mock_engine, GET_USER_AGENT_STRINGS_QUERY)
    # THEN: expect to have saved two batches to database
    assert mock_to_sql.call_count == 2
    # THEN: First batch should be full, with expected start and end
    assert len(mock_to_sql.call_args_list[0][0][0]) == 16384
    assert mock_to_sql.call_args_list[0][0][0].iloc[0]["os_version"] == "1.0"
    assert mock_to_sql.call_args_list[0][0][0].iloc[-1]["os_version"] == "16384.0"

    # THEN: First batch should be partial, with expected start and end
    assert len(mock_to_sql.call_args_list[1][0][0]) == 15615
    assert mock_to_sql.call_args_list[1][0][0].iloc[0]["os_version"] == "16385.0"
    assert mock_to_sql.call_args_list[1][0][0].iloc[-1]["os_version"] == "31999.0"


def test_parse_user_agent_empty_dataset(mocker):
    # GIVEN: mock operations to snowflake
    mock_engine_factory = mocker.patch("utils.user_agent_parser.snowflake_engine_factory")
    mock_engine = mocker.MagicMock()
    mock_engine_factory.return_value = mock_engine
    mock_connection = mocker.MagicMock()
    mock_engine.connect.return_value = mock_connection
    mock_execute_query = mocker.patch("utils.user_agent_parser.execute_query")
    mock_execute_df = mocker.patch("utils.user_agent_parser.execute_dataframe")
    # GIVEN: no user agents
    mock_execute_df.return_value = pd.DataFrame({})
    mock_to_sql = mocker.patch("pandas.io.sql.to_sql")

    # WHEN: trigger user agent parsing
    parse_user_agent()

    # THEN: expect attempt to create table if not exists
    mock_execute_query.assert_called_once_with(mock_engine, CREATE_TABLE_QUERY)
    # THEN: expect attempt to load user agent strings
    mock_execute_df.assert_called_once_with(mock_engine, GET_USER_AGENT_STRINGS_QUERY)
    # THEN: expect not to try to save any data
    assert mock_to_sql.call_count == 0
