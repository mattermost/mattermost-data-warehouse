import tempfile

import pytest

from utils.github.service import GithubService

# Mock data for repositories.
REPO_PAGE1 = [
    {
        # Omitting full response for brevity
        "id": 1296223,
        "name": "Hello-World",
        "full_name": "test-organization/Hello-World",
    },
    {"id": 1296285, "name": "Foo", "full_name": "test-organization/foo"},
]
REPO_PAGE2 = [
    {"id": 1296301, "name": "Example", "full_name": "test-organization/Example"},
    {"id": 1296302, "name": "repo-4", "full_name": "test-organization/repo-4"},
]


@pytest.mark.parametrize(
    'sample_data',
    [
        pytest.param(
            [],
            id='empty response',
        ),
        pytest.param(
            [
                # Single page
                REPO_PAGE1
            ],
            id='single page',
        ),
        pytest.param(
            [REPO_PAGE1, REPO_PAGE2],
            id='multiple pages',
        ),
    ],
)
def test_should_fetch_repositories(mocker, sample_data):
    # GIVEN: an instance of the github service
    service = GithubService(token='a token')
    # GIVEN: request returns two pages
    mock_paged = mocker.patch('utils.github.service.paged')
    mock_paged.return_value = sample_data

    # WHEN: request to get repositories
    result = service.repositories('test-organization')

    # THEN: expect result as a list to be a flattened version of returned pages
    assert list(result) == [repo for page in sample_data for repo in page]


PULL_PAGE1 = [
    {
        # Omitting full response for brevity
        "id": 1,
        "state": "open",
        "title": "Amazing new feature",
        "number": 1234,
        "user": {
            "login": "joedoe",
        },
        "merged_at": None,
    },
    {
        "id": 2,
        "state": "closed",
        "title": "Chore: add README.md",
        "number": 1347,
        "user": {
            "login": "joedoe",
        },
        "merged_at": "2011-01-26T19:01:12Z",
    },
]

PULL_PAGE2 = [
    {
        "id": 3,
        "state": "closed",
        "title": "Bugfix: proper base url",
        "number": 1359,
        "user": {
            "login": "test-user",
        },
        "merged_at": "2012-08-25T09:23:52Z",
    },
    {
        "id": 4,
        "state": "open",
        "title": "AB-1234: New feature",
        "number": 1421,
        "user": {
            "login": "bilbo",
        },
        "merged_at": None,
    },
]


@pytest.mark.parametrize(
    'sample_data',
    [
        pytest.param(
            [],
            id='empty response',
        ),
        pytest.param(
            [PULL_PAGE1],
            id='single page',
        ),
        pytest.param(
            [PULL_PAGE1, PULL_PAGE2],
            id='multiple pages',
        ),
    ],
)
def test_should_fetch_prs(mocker, sample_data):
    # GIVEN: an instance of the github service
    service = GithubService(token='a token')
    # GIVEN: request returns two pages
    mock_paged = mocker.patch('utils.github.service.paged')
    mock_paged.return_value = sample_data

    # WHEN: request to get pulls
    result = service.pulls('test-organization', 'example-repo')

    # THEN: expect result as a list to be a flattened version of returned pages
    assert list(result) == [pr for page in sample_data for pr in page]


def test_should_download_prs(mocker):
    # GIVEN: an instance of the github service
    service = GithubService(token='a token')
    mock_paged = mocker.patch('utils.github.service.paged')
    # GIVEN: request returns two repositories and 1 page of pr per repo
    mock_paged.side_effect = [
        [REPO_PAGE1],  # Response for getting repos
        [PULL_PAGE1],  # Response for getting PRs for first repo
        [PULL_PAGE2],  # Response for getting PRs for second repo
    ]

    # WHEN: request to download PRs to a file
    with tempfile.NamedTemporaryFile() as tmp:
        with open(tmp.name, 'w') as fp:
            service.download_pulls('test-organization', fp)
        with open(tmp.name, 'r') as fp:
            result = fp.readlines()

    # THEN: expect CSV to contain all data
    assert result == [
        "PR_NUMBER,MERGED_AT,AUTHOR,REPO\n",
        "1234,,joedoe,Hello-World\n",
        "1347,2011-01-26T19:01:12Z,joedoe,Hello-World\n",
        "1359,2012-08-25T09:23:52Z,test-user,Foo\n",
        "1421,,bilbo,Foo\n",
    ]


def test_should_download_prs_with_filter(mocker):
    # GIVEN: an instance of the github service
    service = GithubService(token='a token')
    mock_paged = mocker.patch('utils.github.service.paged')
    # GIVEN: request returns two repositories and 1 page of pr per repo
    mock_paged.side_effect = [
        [REPO_PAGE1],  # Response for getting repos
        [PULL_PAGE1],  # Response for getting PRs for first repo
        [PULL_PAGE2],  # Response for getting PRs for second repo
    ]

    # WHEN: request to download PRs to a file, but filtering only merged
    with tempfile.NamedTemporaryFile() as tmp:
        with open(tmp.name, 'w') as fp:
            service.download_pulls(
                'test-organization', fp, state='closed', pr_filter=lambda pr: pr['merged_at'] is not None
            )
        with open(tmp.name, 'r') as fp:
            result = fp.readlines()

    # THEN: expect CSV to contain all data
    assert result == [
        "PR_NUMBER,MERGED_AT,AUTHOR,REPO\n",
        "1347,2011-01-26T19:01:12Z,joedoe,Hello-World\n",
        "1359,2012-08-25T09:23:52Z,test-user,Foo\n",
    ]
