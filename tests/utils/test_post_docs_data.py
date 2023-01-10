import pytest

import responses as test_responses  # importing as test_responses, due to conflict with import in conftest.py
import snowflake.connector

from utils import post_docs_data

"""
This file contains unit tests for utils.post_data_job.py
In case of change in `test_format_row`, make changes in utils.post_nps_job.py as well
"""


class TestPostDocsJob:

    # This test validates output from method `format_row`

    @pytest.mark.parametrize(
        "input_row, output_row",
        [
            (('sample feedback 1', ''), ('sample feedback 1', '')),
            (('sample\nfeedback2', ''), ('sample feedback2', '')),
            (('sample\n\nfeedback ""3', ''), ('sample  feedback 3', '')),
            (('sample feedback""""\n""""4', ''), ('sample feedback 4', '')),
            (
                ('sample feedback""""\n""""5', 'untouched string goes here\n\n""'),
                ('sample feedback 5', 'untouched string goes here\n\n""'),
            ),
        ],
    )
    def test_format_row(self, config_data, input_row, output_row):
        # function returns output after removing newline and string quotes
        assert post_docs_data.format_row(input_row) == output_row

    # This test validates that script runs and data is post successfully to mattermost channel

    def test_post_to_channel_success(self, config_data, responses, post_data_ok, mock_snowflake_connector):

        mock_snowflake_connector('utils.post_docs_data')
        data = {
            "text": "| Feedback            | Path                |\n|---------------------|---------------------|\n| test row 1 column 1 | test row 1 column 2 |\n| test row 2 column 1 | test row 2 column 2 |",
            "channel": "mattermost-documentation-feedback",
        }
        test_responses.post(
            url="https://mattermost.example.com/hooks/hookid",
            body="",
            match=[test_responses.matchers.json_params_matcher(data)],
        )
        post_docs_data.post_docs()
        snowflake.connector.connect.assert_called_once()
        responses.assert_call_count("https://mattermost.example.com/hooks/hookid", 1)

    # This test validates that script runs but data is not post to mattermost channel due to some error

    def test_post_to_channel_error(self, config_data, responses, post_data_error, mock_snowflake_connector):

        mock_snowflake_connector('utils.post_docs_data')
        with pytest.raises(ValueError) as error:
            post_docs_data.post_docs()
        assert 'Request to Mattermost returned an error' in str(error.value)
        snowflake.connector.connect.assert_called_once()
        responses.assert_call_count("https://mattermost.example.com/hooks/hookid", 1)
