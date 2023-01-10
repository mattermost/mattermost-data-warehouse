from pathlib import Path
from unittest.mock import call

import pytest

from utils.run_snowflake_queries import parser, run_queries

ROLE = "TEST-ROLE"
SCHEMA = "TEST-SCHEMA"
FIXTURE_PATH = str(Path(__file__).parent / "fixtures" / "snowflake_queries")


def test_parser():
    # WHEN: request to parse arguments
    args = parser.parse_args(["test-file", ROLE, SCHEMA])

    # THEN: expect arguments to have been parsed
    assert args.role == ROLE
    assert args.schema == SCHEMA
    assert args.sql_file == "test-file"


@pytest.mark.parametrize(
    "filename,expected_queries",
    [
        pytest.param("single", ["SELECT a, b FROM table", ""], id="single statement"),
        pytest.param(
            "multiple",
            [
                "update analytics.staging.ald_test set test = 'new' where id = 1",
                "\n\nupdate analytics.staging.ald_test set test = 'old' where id = 2",
                "",
            ],
            id="multiple statements",
        ),
        pytest.param(
            "escape",
            ["SELECT user || ';' || age FROM users", "\nSELECT 1", ""],
            id="multiple statements with escape char",
        ),
    ],
)
def test_run_queries(mock_snowflake, filename, expected_queries):
    # GIVEN: connection to snowflake
    _, mock_connection, _ = mock_snowflake("utils.run_snowflake_queries")

    # GIVEN: request to execute queries from a specific file
    args = parser.parse_args([filename, ROLE, SCHEMA])

    # WHEN:
    run_queries(args, base_path=FIXTURE_PATH)

    # THEN: Expect execute to have been called once for each query
    mock_connection.execute.assert_has_calls([call(q) for q in expected_queries])
