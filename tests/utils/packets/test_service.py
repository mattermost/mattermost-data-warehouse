import pandas as pd
from pandas._testing import assert_frame_equal

from tests.utils.packets import SURVEY_DIR
from utils.packets.service import ingest_survey_packet


def test_ingest_survey_packet(sqlalchemy_memory_engine):
    # GIVEN: a clean database
    with sqlalchemy_memory_engine.connect() as conn:
        # GIVEN: the schema exists
        conn.execute("ATTACH DATABASE ':memory:' AS 'test_schema'")

        # WHEN: attempt to ingest a user survey in a clean database
        ingest_survey_packet(conn, 'test_schema', SURVEY_DIR / 'valid.zip', 's3://bucket/valid.zip')

        # THEN: expect the data to be in the database
        df = pd.read_sql("SELECT * FROM 'test_schema'.user_survey", conn)

        # THEN: expect responses to be loaded correctly
        assert_frame_equal(
            df[
                [
                    'user_id',
                    'question',
                    'answer',
                    'question_type',
                    'metadata_server_id',
                    'metadata_extras_plugin_id',
                    'metadata_extras_plugin_version',
                ]
            ],
            pd.DataFrame(
                {
                    'user_id': ['f8ama5so7bnaix5z94zj4x77sr'] * 3,
                    'question': [
                        'How likely are you to suggest this app to someone else?',
                        'How can we make this app better for you?',
                        'What is 2 + 2?',
                    ],
                    'answer': ['10', 'response 1!!!', 'Its 4, obviously, but this might be a trick question 🤔🤔🤔'],
                    'question_type': ['linear_scale', 'text', 'text'],
                    'metadata_server_id': ['rmg9ib5rspy93jxswyc454bwzo'] * 3,
                    'metadata_extras_plugin_id': ['com.mattermost.user-survey'] * 3,
                    'metadata_extras_plugin_version': ['1.1.0'] * 3,
                }
            ),
        )


def test_ingest_survey_packet_twice(sqlalchemy_memory_engine):
    # GIVEN: a clean database
    with sqlalchemy_memory_engine.connect() as conn:
        # GIVEN: the schema exists
        conn.execute("ATTACH DATABASE ':memory:' AS 'test_schema'")

        # GIVEN: user surveys already ingested
        ingest_survey_packet(conn, 'test_schema', SURVEY_DIR / 'valid.zip', 's3://bucket/valid.zip')
        ingest_survey_packet(conn, 'test_schema', SURVEY_DIR / 'valid.zip', 's3://bucket/valid2.zip')

        # WHEN: attempt to re-ingest a user survey in a clean database
        ingest_survey_packet(conn, 'test_schema', SURVEY_DIR / 'valid.zip', 's3://bucket/valid.zip')

        # THEN: expect the data to be in the database only once
        df = pd.read_sql("SELECT * FROM 'test_schema'.user_survey", conn)

        # THEN: expect responses to be loaded correctly and only once
        assert_frame_equal(
            df[
                [
                    'user_id',
                    'question',
                    'answer',
                    'question_type',
                    'metadata_server_id',
                    'metadata_extras_plugin_id',
                    'metadata_extras_plugin_version',
                    'source',
                ]
            ],
            pd.DataFrame(
                {
                    'user_id': ['f8ama5so7bnaix5z94zj4x77sr'] * 6,
                    'question': [
                        'How likely are you to suggest this app to someone else?',
                        'How can we make this app better for you?',
                        'What is 2 + 2?',
                    ]
                    * 2,
                    'answer': ['10', 'response 1!!!', 'Its 4, obviously, but this might be a trick question 🤔🤔🤔'] * 2,
                    'question_type': ['linear_scale', 'text', 'text'] * 2,
                    'metadata_server_id': ['rmg9ib5rspy93jxswyc454bwzo'] * 6,
                    'metadata_extras_plugin_id': ['com.mattermost.user-survey'] * 6,
                    'metadata_extras_plugin_version': ['1.1.0'] * 6,
                    'source': ['s3://bucket/valid2.zip'] * 3 + ['s3://bucket/valid.zip'] * 3,
                }
            ),
        )
