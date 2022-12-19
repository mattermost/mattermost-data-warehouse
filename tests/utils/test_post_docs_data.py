import os
import importlib
import pytest
import snowflake.connector
from utils import post_docs_data

class TestPostDocsJob():

    @pytest.mark.parametrize("input_row, output_row",
                                [
                                    (('sample feedback 1',''),('sample feedback 1','')),
                                    (('sample\nfeedback2',''),('sample feedback2','')),
                                    (('sample\n\nfeedback ""3',''),('sample  feedback 3','')),
                                    (('sample feedback""""\n""""4',''),('sample feedback 4','')),
                                    (('sample feedback""""\n""""5','untouched string goes here\n\n""'),('sample feedback 5','untouched string goes here\n\n""'))
                                ]
                            )
    def test_format_row(self, config_feedback, input_row, output_row):
        # function returns output after removing newline and string quotes
        assert post_docs_data.format_row(input_row) == output_row
    
    def test_post_to_channel_success(self, config_feedback, responses, post_mattermost_ok, mock_snowflake_connector):

        mock_snowflake_connector('utils.post_docs_data')
        data = {"text": "| Feedback            | Path                |\n|---------------------|---------------------|\n| test row 1 column 1 | test row 1 column 2 |\n| test row 2 column 1 | test row 2 column 2 |", "channel": "mattermost-documentation-feedback"}
        responses.post(
            url = "https://mattermost.example.com/hooks/hookid",
            body = "",
            match=[responses.matchers.json_params_matcher(data)],
        )

        post_docs_data.post_docs()
        snowflake.connector.connect.assert_called_once()
        responses.assert_call_count("https://mattermost.example.com/hooks/hookid", 1)

    def test_post_to_channel_error(self, config_feedback, responses, post_mattermost_error, mock_snowflake_connector):

        mock_snowflake_connector('utils.post_docs_data')
        with pytest.raises(ValueError) as error:
            post_docs_data.post_docs()
        assert 'Request to Mattermost returned an error' in str(error.value)
        snowflake.connector.connect.assert_called_once()
        responses.assert_call_count("https://mattermost.example.com/hooks/hookid", 1)
