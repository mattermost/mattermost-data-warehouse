from datetime import date
from unittest.mock import call

import pytest

from utils.push_proxy.service import join_s3_path, load_stage_to_table


@pytest.mark.parametrize(
    "prefix,parts",
    [
        pytest.param("/path1/path2/path3", ["extra1", "extra2"], id="leading /"),
        pytest.param("/path1/path2/path3/", ["extra1", "extra2"], id="leading and trailing /"),
        pytest.param("/path1/path2///path3/", ["extra1", "extra2"], id="multiple //"),
        pytest.param("//path1/path2///path3/", ["extra1", "extra2"], id="multiple leading //"),
        pytest.param("/path1/path2///path3/", ["/extra1", "/extra2"], id="leading / in parts"),
        pytest.param("/path1/path2///path3/", ["/extra1/", "/extra2/"], id="leading and trailing / in parts"),
        pytest.param("/path1/path2///path3/", ["/extra1/extra2/"], id="/ in parts"),
        pytest.param("/path1/path2///path3/", ["/extra1//extra2/"], id="duplicate / in parts"),
    ],
)
def test_join_s3_path_removes_leading_and_trailing_prefix(prefix, parts):
    assert join_s3_path(prefix, *parts) == "path1/path2/path3/extra1/extra2"


@pytest.mark.parametrize(
    "prefix,parts,expected",
    [
        pytest.param("/path1/path2/", [2024, 2], "path1/path2/2024/2", id="integers"),
        pytest.param("/path1/path2/", [True, False], "path1/path2/True/False", id="boolean"),
        pytest.param("/path1/path2/", [2024, "02"], "path1/path2/2024/02", id="mix"),
    ],
)
def test_join_s3_path_non_string_parts(prefix, parts, expected):
    assert join_s3_path(prefix, *parts) == expected


def test_load_stage_to_empty_table(mocker):
    # GIVEN: a connection
    mock_conn = mocker.Mock()
    mock_max_col_value = mocker.patch("utils.push_proxy.service.max_column_value")
    mock_copy_from_stage = mocker.patch("utils.push_proxy.service.copy_from_stage")

    # GIVEN: no data have been loaded to table
    mock_max_col_value.return_value = None

    # WHEN: request to copy from stage
    load_stage_to_table(
        mock_conn,
        "stage_schema",
        "stage",
        "/path/to/data/",
        "target_schema",
        "target_table",
        timestamp_column="timestamp",
    )

    # THEN: expect to attempt to load only once
    mock_copy_from_stage.assert_called_once_with(
        mock_conn, "stage_schema", "stage", "path/to/data", "target_schema", "target_table"
    )


def test_load_stage_to_table_with_data(mocker, freezer):
    # GIVEN: date is 2024-02-15
    freezer.move_to("2024-02-15")

    # GIVEN: a connection
    mock_conn = mocker.Mock()
    mock_max_col_value = mocker.patch("utils.push_proxy.service.max_column_value")
    mock_copy_from_stage = mocker.patch("utils.push_proxy.service.copy_from_stage")

    # GIVEN: table contains data up to 2024-02-12 (3 days before)
    mock_max_col_value.return_value = date(2024, 2, 12)

    # WHEN: request to copy from stage
    load_stage_to_table(
        mock_conn,
        "stage_schema",
        "stage",
        "/path/to/data/",
        "target_schema",
        "target_table",
        timestamp_column="timestamp",
    )

    # THEN: expect to attempt to load data, starting at last date and up to (but not including) today
    mock_copy_from_stage.assert_has_calls(
        [
            call(mock_conn, "stage_schema", "stage", "path/to/data/2024/02/12", "target_schema", "target_table"),
            call(mock_conn, "stage_schema", "stage", "path/to/data/2024/02/13", "target_schema", "target_table"),
            call(mock_conn, "stage_schema", "stage", "path/to/data/2024/02/14", "target_schema", "target_table"),
        ]
    )
