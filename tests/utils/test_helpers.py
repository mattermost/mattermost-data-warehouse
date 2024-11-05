import json
from datetime import date
from textwrap import dedent

import pandas as pd
from responses import Response

from utils.helpers import daterange, post_df_to_mattermost


def test_include_start_exclude_end():
    assert list(daterange(date(2024, 2, 15), date(2024, 2, 18))) == [
        date(2024, 2, 15),
        date(2024, 2, 16),
        date(2024, 2, 17),
    ]


def test_start_date_after_end_date():
    assert list(daterange(date(2024, 2, 18), date(2024, 2, 5))) == []


def test_start_date_end_date_match():
    assert list(daterange(date(2024, 2, 18), date(2024, 2, 18))) == []


def test_end_date_next_day_of_start_date():
    assert list(daterange(date(2024, 2, 18), date(2024, 2, 19))) == [date(2024, 2, 18)]


def test_post_to_mattermost(responses):
    # GIVEN: A Mattermost URL, and channel that returns HTTP OK
    url = 'https://mattermost.a-test-server.com/path/to/hookid'
    channel = 'test-channel'

    response = Response(
        method="POST",
        url='https://mattermost.a-test-server.com/path/to/hookid',
        status=200,
        content_type='application/json',
    )
    responses.add(response)

    # GIVEN: Some data to post
    df = pd.DataFrame({'id': [1, 2], 'title': ['The Great Gatsby', 'The Lord of the Rings']})

    # WHEN: request is made to Mattermost
    post_df_to_mattermost(url, channel, df, ['ISBN', 'Book Title'], 'No books found')

    # THEN: A request is made to the Mattermost server
    assert len(responses.calls) == 1
    assert json.loads(responses.calls[0].request.body) == {
        'text': dedent(
            '''
            |   ISBN | Book Title            |
            |--------|-----------------------|
            |      1 | The Great Gatsby      |
            |      2 | The Lord of the Rings |
        '''
        ).strip(),
        'channel': 'test-channel',
    }


def test_post_to_mattermost_with_fallback(responses):
    # GIVEN: A Mattermost URL, and channel that returns HTTP OK
    url = 'https://mattermost.a-test-server.com/path/to/hookid'
    channel = 'test-channel'

    response = Response(
        method="POST",
        url='https://mattermost.a-test-server.com/path/to/hookid',
        status=200,
        content_type='application/json',
    )
    responses.add(response)

    # GIVEN: An empty dataframe
    df = pd.DataFrame()

    # WHEN: request is made to Mattermost
    post_df_to_mattermost(url, channel, df, ['ISBN', 'Book Title'], 'No books found')

    # THEN: A request is made to the Mattermost server with fallback message
    assert len(responses.calls) == 1
    assert json.loads(responses.calls[0].request.body) == {
        'text': 'No books found',
        'channel': 'test-channel',
    }
