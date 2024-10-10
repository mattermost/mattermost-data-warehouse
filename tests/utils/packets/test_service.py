import pandas as pd
from pandas._testing import assert_frame_equal

from tests.utils.packets import SUPPORT_DIR, SURVEY_DIR
from utils.packets.service import ingest_support_packet, ingest_survey_packet


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

        # WHEN: attempt to re-ingest a user survey
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


def test_ingest_support_packet(sqlalchemy_memory_engine):
    # GIVEN: a clean database
    with sqlalchemy_memory_engine.connect() as conn:
        # GIVEN: the schema exists
        conn.execute("ATTACH DATABASE ':memory:' AS 'test_schema'")

        # WHEN: attempt to ingest a support packet in a clean database
        ingest_support_packet(
            conn, 'test_schema', SUPPORT_DIR / 'valid_with_metadata.zip', 's3://bucket/valid_with_metadata.zip'
        )

        # THEN: expect the package data to be in the database only once
        results = sqlalchemy_memory_engine.execute("SELECT * FROM 'test_schema'.support_packet_v1").fetchall()
        assert len(results) == 1

        result = results[0]
        assert result['server_os'] == 'linux'
        assert result['database_type'] == 'postgres'
        assert result['database_version'] == '13.10'
        assert result['license_to'] == 'Mattermost'
        assert result['license_supported_users'] == 200000
        assert result['active_users'] == 1
        assert result['daily_active_users'] == 0
        assert result['metadata_server_id'] == 'rmg9ib5rspy93jxswyc454bwzo'
        assert result['source'] == 's3://bucket/valid_with_metadata.zip'

        # THEN: expect the job data to be in the database
        results = sqlalchemy_memory_engine.execute("SELECT * FROM 'test_schema'.support_packet_jobs_v1").fetchall()
        assert len(results) == 1

        result = results[0]
        assert result['id'] == '4555h6cxb38q3rhnyfu95dypxh'
        assert result['type'] == 'migrations'
        assert result['priority'] == 0
        assert result['createat'] == '2024-08-15 15:16:06.121000'
        assert result['startat'] == '2024-08-15 15:16:20.530000'
        assert result['lastactivityat'] == '2024-08-15 15:16:21.002000'
        assert result['status'] == 'success'
        assert result['progress'] == 0
        assert (
            result['data']
            == "{\'last_done\': \'{\"current_table\":\"ChannelMembers\",\"last_team_id\":\"crro7gj13bdzfjm4rmm6ept6sa\",\"last_channel_id\":\"mpmdxijsftdodkzbehncatthcr\",\"last_user\":\"wg94o7yd4jyxjbxoihettwgmah\"}\', \'migration_key\': \'migration_advanced_permissions_phase_2\'}"  # noqa: E501
        )
        assert result['metadata_server_id'] == 'rmg9ib5rspy93jxswyc454bwzo'
        assert result['source'] == 's3://bucket/valid_with_metadata.zip'


def test_ingest_support_packet_twice(sqlalchemy_memory_engine):
    # GIVEN: a clean database
    with sqlalchemy_memory_engine.connect() as conn:
        # GIVEN: the schema exists
        conn.execute("ATTACH DATABASE ':memory:' AS 'test_schema'")

        # GIVEN: packages has been ingested
        ingest_support_packet(
            conn, 'test_schema', SUPPORT_DIR / 'valid_with_metadata.zip', 's3://bucket/valid_with_metadata.zip'
        )
        ingest_support_packet(
            conn, 'test_schema', SUPPORT_DIR / 'valid_without_metadata.zip', 's3://bucket/valid_without_metadata.zip'
        )

        # WHEN: attempt to re-ingest a support package
        ingest_support_packet(
            conn, 'test_schema', SUPPORT_DIR / 'valid_with_metadata.zip', 's3://bucket/valid_with_metadata.zip'
        )

        # THEN: expect the package data to be in the database only once
        results = sqlalchemy_memory_engine.execute(
            "SELECT * FROM 'test_schema'.support_packet_v1 WHERE source = 's3://bucket/valid_with_metadata.zip'"
        ).fetchall()
        assert len(results) == 1

        result = results[0]
        assert result['server_os'] == 'linux'
        assert result['database_type'] == 'postgres'
        assert result['database_version'] == '13.10'
        assert result['license_to'] == 'Mattermost'
        assert result['license_supported_users'] == 200000
        assert result['active_users'] == 1
        assert result['daily_active_users'] == 0
        assert result['metadata_server_id'] == 'rmg9ib5rspy93jxswyc454bwzo'
        assert result['source'] == 's3://bucket/valid_with_metadata.zip'

        # THEN: expect the job data to be in the database
        results = sqlalchemy_memory_engine.execute(
            "SELECT * FROM 'test_schema'.support_packet_jobs_v1 WHERE source = 's3://bucket/valid_with_metadata.zip'"
        ).fetchall()
        assert len(results) == 1

        result = results[0]
        assert result['id'] == '4555h6cxb38q3rhnyfu95dypxh'
        assert result['type'] == 'migrations'
        assert result['priority'] == 0
        assert result['createat'] == '2024-08-15 15:16:06.121000'
        assert result['startat'] == '2024-08-15 15:16:20.530000'
        assert result['lastactivityat'] == '2024-08-15 15:16:21.002000'
        assert result['status'] == 'success'
        assert result['progress'] == 0
        assert (
            result['data']
            == "{\'last_done\': \'{\"current_table\":\"ChannelMembers\",\"last_team_id\":\"crro7gj13bdzfjm4rmm6ept6sa\",\"last_channel_id\":\"mpmdxijsftdodkzbehncatthcr\",\"last_user\":\"wg94o7yd4jyxjbxoihettwgmah\"}\', \'migration_key\': \'migration_advanced_permissions_phase_2\'}"  # noqa: E501
        )
        assert result['metadata_server_id'] == 'rmg9ib5rspy93jxswyc454bwzo'
        assert result['source'] == 's3://bucket/valid_with_metadata.zip'