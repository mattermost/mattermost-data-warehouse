import os
import importlib
import pytest
import snowflake.connector
from utils import post_nps_data
import responses
class TestPostNpsJob():

    @pytest.mark.parametrize("input_row, output_row",
                                [
                                    (('sample feedback 1',''),('sample feedback 1','')),
                                    (('sample\nfeedback2',''),('sample feedback2','')),
                                    (('sample\n\nfeedback ""3',''),('sample  feedback 3','')),
                                    (('sample feedback""""\n""""4',''),('sample feedback 4','')),
                                    (('sample feedback""""\n""""5','untouched string goes here\n\n""'),('sample feedback 5','untouched string goes here\n\n""'))
                                ]
                            )
    def test_format_row(self, config_nps, input_row, output_row):
        # function returns output after removing newline and string quotes
        assert post_nps_data.format_row(input_row) == output_row

    def test_snowflake_connection(self, config_nps, responses, post_nps_ok, mock_snowflake_connector):
        
        assert os.getenv("SNOWFLAKE_USER") == "test user"
        assert os.getenv("SNOWFLAKE_PASSWORD") == "test password"
        assert os.getenv("SNOWFLAKE_ACCOUNT") == "test account"
        assert os.getenv("SNOWFLAKE_TRANSFORM_WAREHOUSE") == "test warehouse"
        assert os.getenv("SNOWFLAKE_TRANSFORM_DATABASE") == "test database"
        assert os.getenv("SNOWFLAKE_TRANSFORM_SCHEMA") == "test schema"
        assert os.getenv("NPS_WEBHOOK_URL") == "https://mattermost.example.com/hooks/hookid"

        mock_snowflake_connector('utils.post_nps_data')
        post_nps_data.post_nps()
        snowflake.connector.connect.assert_called_once()

    def test_post_to_channel_success(self, config_nps, responses, post_nps_ok, mock_snowflake_connector):

        mock_snowflake_connector('utils.post_nps_data')
        post_nps_data.post_nps()
        snowflake.connector.connect.assert_called_once()
        responses.assert_call_count("https://mattermost.example.com/hooks/hookid", 1)

    def test_post_to_channel_error(self, config_nps, responses, post_nps_error, mock_snowflake_connector):

        mock_snowflake_connector('utils.post_nps_data')
        try:
            post_nps_data.post_nps()
            snowflake.connector.connect.assert_called_once()
            responses.assert_call_count("https://mattermost.example.com/hooks/hookid", 1)
            assert False
        except ValueError as e:
            assert True







