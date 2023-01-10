import pytest
import snowflake.connector

from utils import post_nps_data


class TestPostNpsJob:
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
        assert post_nps_data.format_row(input_row) == output_row

    def test_feedback_generate_success(self, config_data, mock_snowflake_connector, mocker):
        mock_snowflake_connector('utils.post_nps_data')
        requests_mock = mocker.patch('utils.post_nps_data.requests.post')
        requests_mock.return_value = mocker.Mock()
        # Expected to give error since mock object methods cant be called
        with pytest.raises(ValueError):
            post_nps_data.post_nps()
        data = (
            b'{"text": "| Feedback            | Category            |\n'
            b'|---------------------|---------------------|\n'
            b'| test row 1 column 1 | test row 1 column 2 |\n'
            b'| test row 2 column 1 | test row 2 column 2 |"'
            b', "channel": "mattermost-nps-feedback"}'
        )
        # Validate post data sent to the mattermost webhook
        requests_mock.assert_called_once_with(
            "https://mattermost.example.com/hooks/hookid", data=data, headers={'Content-Type': 'application/json'}
        )

    def test_post_to_channel_success(self, config_data, responses, post_data_ok, mock_snowflake_connector, mocker):
        mock_snowflake_connector('utils.post_nps_data')
        post_nps_data.post_nps()
        snowflake.connector.connect.assert_called_once()
        responses.assert_call_count("https://mattermost.example.com/hooks/hookid", 1)

    def test_post_to_channel_error(self, config_data, responses, post_data_error, mock_snowflake_connector):
        mock_snowflake_connector('utils.post_nps_data')
        with pytest.raises(ValueError) as error:
            post_nps_data.post_nps()
        assert 'Request to Mattermost returned an error' in str(error.value)
        snowflake.connector.connect.assert_called_once()
        responses.assert_call_count("https://mattermost.example.com/hooks/hookid", 1)
