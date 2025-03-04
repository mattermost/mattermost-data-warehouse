import pytest
from mock import call

from extract.s3_extract.stage_import import (
    diagnostics_import,
    extract_from_stage,
    get_diagnostics_pattern,
    get_path,
    licenses_import,
    releases_import,
)


def test_extract_from_stage(mock_snowflake):
    # GIVEN: snowflake engine and connection
    _, _, mock_execute_query, _ = mock_snowflake("extract.s3_extract.stage_import")

    # WHEN: request to import data from S3
    extract_from_stage("test-table", "dev", "test-schema", "data/valuable", "*.csv", {})

    # THEN: expect query to have been executed once
    mock_execute_query.assert_called_once()
    assert _flatten_whitespaces(mock_execute_query.call_args_list[0][0][1]) == _flatten_whitespaces(
        """
        COPY INTO raw.test-schema.test-table
        FROM @dev/data/valuable
        PATTERN = '*.csv'
        ON_ERROR = 'CONTINUE';
    """
    )


def test_diagnostics_import(mocker, mock_environment):
    # GIVEN: environment configured for handling two diagnostic imports -- see mock_environment
    # GIVEN: calls to extract from stage are captured
    mock_extract = mocker.patch("extract.s3_extract.stage_import.extract_from_stage")

    # WHEN: diagnostics job is triggered for a specific date
    diagnostics_import("2022-10-01")

    # THEN: expect extract to have been called once for each import
    assert mock_extract.call_count == 2
    mock_extract.assert_has_calls(
        [
            call(
                "log_entries",
                "diagnostics_stage",
                "diagnostics",
                "location-one",
                ".*location-one.2022-10-01.*",
                mock_environment,
            ),
            call(
                "log_entries",
                "diagnostics_stage",
                "diagnostics",
                "location-two",
                ".*location-two.2022-10-01.*",
                mock_environment,
            ),
        ]
    )


@pytest.mark.parametrize(
    "loc,import_date,pattern",
    [
        ("location1", "2022-10-01", ".*location1.2022-10-01.*"),
        ("location2", "2021-12-25", ".*location2.2021-12-25.*"),
    ],
)
def test_get_diagnostics_pattern(loc, import_date, pattern):
    assert get_diagnostics_pattern(loc, import_date) == pattern


@pytest.mark.parametrize(
    "aws_account_id,az,expected",
    [
        ("account-1", "az-1", "AWSLogs/account-1/elasticloadbalancing/az-1"),
        ("account-2", "az-2", "AWSLogs/account-2/elasticloadbalancing/az-2"),
    ],
)
def test_get_path(aws_account_id, az, expected):
    assert get_path(aws_account_id, az) == expected


def test_licenses_import(mocker, mock_environment):
    # GIVEN: environment configured for handling two diagnostic imports -- see mock_environment
    # GIVEN: calls to extract from stage are captured
    mock_extract = mocker.patch("extract.s3_extract.stage_import.extract_from_stage")

    # WHEN: license job is triggered for a specific date
    licenses_import("2022-10-01")

    # THEN: expect extract to have been called once
    mock_extract.assert_called_once_with(
        "licenses",
        "licenses_stage",
        "licenses",
        "2022-10-01",
        ".*2022-10-01.csv",
        mock_environment,
    )


def test_releases_import(mocker, mock_environment):
    # GIVEN: environment configured for handling releases import -- see mock_environment
    # GIVEN: calls to extract from stage are captured
    mock_extract = mocker.patch("extract.s3_extract.stage_import.extract_from_stage")

    # WHEN: license job is triggered for a specific date
    releases_import("2022-10-01")

    # THEN: expect extract to have been called once
    mock_extract.assert_called_once_with(
        "log_entries",
        "releases_stage",
        "releases",
        "releases.mattermost.com-cloudfront/test-release-location",
        ".*test-release-location.2022-10-01.*",
        mock_environment,
    )


def _flatten_whitespaces(text):
    """
    Utility method to remove continuous whitespaces from multiline strings
    """
    return "\n".join([line.strip() for line in text.split("\n")])
