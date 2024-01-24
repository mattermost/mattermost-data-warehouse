import pytest
from responses import Response

from airflow.models import Connection


@pytest.fixture
def ok_response(responses):
    rsp = Response(method="POST", url="http://mattermost.example.com/hooks/myhookid", status=200)
    responses.add(rsp)


@pytest.fixture
def mock_connection(mocker):
    mock_connection = Connection(
        conn_type="http",
        password="myhookid",
        host="mattermost.example.com",
    )
    mock_connection_uri = mock_connection.get_uri()
    mocker.patch.dict("os.environ", AIRFLOW_CONN_SOME_CONN_ID=mock_connection_uri)


@pytest.fixture
def mock_connection_without_token(mocker):
    mock_connection = Connection(
        conn_type="http",
        host="mattermost.example.com",
    )
    mock_connection_uri = mock_connection.get_uri()
    mocker.patch.dict("os.environ", AIRFLOW_CONN_SOME_CONN_ID=mock_connection_uri)
