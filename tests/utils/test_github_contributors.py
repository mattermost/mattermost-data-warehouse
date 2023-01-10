import pandas as pd

from utils.github_contributors import contributors

# Customize defaults for given_request_to
__MOCK_REQUEST_DEFAULTS = {"dir": "github", "headers": {"Authorization": "Bearer token"}}

GITHUB_GRAPHQL_URL = "https://api.github.com/graphql"


def test_contributors(responses, given_request_to, mock_snowflake, mock_snowflake_pandas, load_dataset):
    # GIVEN: repo query returns two pages of results
    given_request_to(GITHUB_GRAPHQL_URL, "repo.page.1.json", method="POST")
    given_request_to(GITHUB_GRAPHQL_URL, "repo.page.2.json", method="POST")
    given_request_to(GITHUB_GRAPHQL_URL, "mattermost-server.page.1.json", method="POST")
    given_request_to(GITHUB_GRAPHQL_URL, "mattermost-server.page.2.json", method="POST")
    given_request_to(GITHUB_GRAPHQL_URL, "docs.page.1.json", method="POST")
    given_request_to(GITHUB_GRAPHQL_URL, "focalboard.page.1.json", method="POST")

    # GIVEN: mock snowflake connection
    _, mock_connection, _ = mock_snowflake("utils.github_contributors")
    _, mock_to_sql = mock_snowflake_pandas("utils.github_contributors")

    # WHEN: request to load contributors
    contributors()

    # THEN: expect github's graphql url to have been called for getting repos
    responses.assert_call_count(GITHUB_GRAPHQL_URL, 6)

    # THEN: expect request to delete existing contributors table
    mock_connection.execute.assert_called_once_with("DELETE FROM staging.github_contributions_all")

    # THEN: expect proper data to be loaded to snowflake table
    mock_to_sql.assert_called_once()
    pd.testing.assert_frame_equal(
        mock_to_sql.call_args_list[0][0][0], pd.DataFrame(data=load_dataset('github/dataset.json'))
    )

    # THEN: expect connection to snowflake to be closed
    mock_connection.close.assert_called_once()


def test_contributors_fail_to_get_repos(
    responses, given_request_to, mock_snowflake, mock_snowflake_pandas, load_dataset
):
    # GIVEN: repo query fails due to authentication error
    given_request_to(GITHUB_GRAPHQL_URL, "auth.error.json", method="POST", status=401)

    # GIVEN: mock snowflake connection
    _, mock_connection, _ = mock_snowflake("utils.github_contributors")
    _, mock_to_sql = mock_snowflake_pandas("utils.github_contributors")

    # WHEN: request to load contributors
    contributors()

    # THEN: expect github's graphql url to have been called just for the first time
    responses.assert_call_count(GITHUB_GRAPHQL_URL, 1)

    # THEN: no interactions with database
    mock_connection.execute.assert_not_called()
    mock_to_sql.assert_not_called()
    mock_connection.close.assert_not_called()


def test_contributors_fail_to_get_repo_details(
    responses, given_request_to, mock_snowflake, mock_snowflake_pandas, load_dataset
):
    # GIVEN: repo query returns two pages of results
    given_request_to(GITHUB_GRAPHQL_URL, "repo.page.1.json", method="POST")
    given_request_to(GITHUB_GRAPHQL_URL, "repo.page.2.json", method="POST")
    # GIVEN: error to get list of contributors
    given_request_to(GITHUB_GRAPHQL_URL, "auth.error.json", method="POST", status=401)

    # GIVEN: mock snowflake connection
    _, mock_connection, _ = mock_snowflake("utils.github_contributors")
    _, mock_to_sql = mock_snowflake_pandas("utils.github_contributors")

    # WHEN: request to load contributors
    contributors()

    # THEN: expect github's graphql url to have been called just for the first repo
    responses.assert_call_count(GITHUB_GRAPHQL_URL, 3)

    # THEN: no interactions with database
    mock_connection.execute.assert_not_called()
    mock_to_sql.assert_not_called()
    mock_connection.close.assert_not_called()
