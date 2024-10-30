import json
from textwrap import dedent

from responses import Response

from utils.db.service import post_query_results


def test_post_query_results(sqlalchemy_memory_engine, test_data, responses):
    # GIVEN: a mattermost server waiting for a request

    response = Response(
        method="POST",
        url='https://mattermost.a-test-server.com',
        status=200,
        content_type='application/json',
    )
    responses.add(response)

    # GIVEN: a database with some data
    with sqlalchemy_memory_engine.connect() as conn, conn.begin():
        # WHEN: request to post query results to Mattermost
        post_query_results(
            conn,
            'select id, title from books order by id',
            ['ISBN', 'Title'],
            'https://mattermost.a-test-server.com',
            'book-club',
        )

    # THEN: a request is made to the Mattermost server
    assert json.loads(responses.calls[0].request.body) == {
        'text': dedent(
            '''
            |   ISBN | Title                 |
            |--------|-----------------------|
            |      1 | The Great Gatsby      |
            |      2 | The Lord of the Rings |
        '''
        ).strip(),
        'channel': 'book-club',
    }