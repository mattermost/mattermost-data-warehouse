from datetime import date, datetime, timezone

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pydantic import ValidationError

from tests.utils.packets import METADATA_DIR, SUPPORT_DIR, SURVEY_DIR
from utils.packets.loaders import (
    load_metadata,
    load_support_packet_file,
    load_support_packet_info,
    load_user_survey,
    load_user_survey_package,
)
from utils.packets.models.metadata import Extras, SupportPacketMetadata, SupportPacketTypeEnum
from utils.packets.models.support import JobV1

#
# Metadata loader tests
#


@pytest.mark.parametrize(
    'metadata,expected',
    [
        pytest.param(
            METADATA_DIR / 'valid' / 'full.yaml',
            SupportPacketMetadata(
                version=1,
                type=SupportPacketTypeEnum.plugin_packet,
                generated_at=1721728796871,
                server_version='9.11.0',
                server_id='p7j6wmx6269jan410vjrylfb2u',
                license_id='0ubekqbvxkptxoasnq1qdadkz1',
                customer_id='jcvj1vkppgc7takujqe4449itu',
                extras=Extras(plugin_id='com.mattermost.plugin', plugin_version='1.0.1', more='data', answer=42),
            ),
            id='full metadata file with extra fields in extras',
        ),
        pytest.param(
            METADATA_DIR / 'valid' / 'empty_license.yaml',
            SupportPacketMetadata(
                version=1,
                type=SupportPacketTypeEnum.plugin_packet,
                generated_at=1721728796871,
                server_version='9.11.0',
                server_id='p7j6wmx6269jan410vjrylfb2u',
                license_id=None,
                customer_id=None,
                extras=Extras(plugin_id='com.mattermost.plugin', plugin_version='1.0.1'),
            ),
            id='full metadata file with empty license fields',
        ),
        pytest.param(
            METADATA_DIR / 'valid' / 'missing_license.yaml',
            SupportPacketMetadata(
                version=1,
                type=SupportPacketTypeEnum.plugin_packet,
                generated_at=1721728796871,
                server_version='9.11.0',
                server_id='p7j6wmx6269jan410vjrylfb2u',
                license_id=None,
                customer_id=None,
                extras=Extras(plugin_id='com.mattermost.plugin', plugin_version='1.0.1'),
            ),
            id='full metadata file with missing license fields',
        ),
    ],
)
def test_load_full_metadata(metadata, expected):
    with open(metadata, 'r') as fp:
        result = load_metadata(fp)

    assert result == expected


@pytest.mark.parametrize(
    'metadata,error_fields',
    [
        pytest.param(METADATA_DIR / 'invalid' / 'invalid_timestamp.yaml', [('generated_at',)], id='invalid timestamp'),
        pytest.param(METADATA_DIR / 'invalid' / 'invalid_type.yaml', [('type',)], id='invalid type'),
        pytest.param(
            METADATA_DIR / 'invalid' / 'missing_fields.yaml',
            [('generated_at',), ('server_id',), ('server_version',)],
            id='missing fields',
        ),
        pytest.param(
            METADATA_DIR / 'invalid' / 'missing_fields_in_extras.yaml',
            [
                (
                    'extras',
                    'plugin_id',
                )
            ],
            id='missing fields in extras',
        ),
    ],
)
def test_load_invalid_metadata(metadata, error_fields):
    # WHEN: attempt to load invalid metadata file
    with pytest.raises(ValidationError) as exc, open(metadata, 'r') as fp:
        load_metadata(fp)

    # THEN: expect fields with invalid or missing data to be reported as errors
    assert sorted([e['loc'] for e in exc.value.errors()]) == error_fields


#
# User survey tests
#


def test_load_user_survey():
    # WHEN: attempt to load a user survey
    with open(SURVEY_DIR / 'valid_metadata.json', 'r') as metadata_fp, open(
        SURVEY_DIR / 'responses.csv', 'r'
    ) as data_fp:
        df = load_user_survey(metadata_fp, data_fp)

    # THEN: expect specific columns
    assert df.columns.tolist() == ['User ID', 'Submitted At', 'Question', 'Answer', 'Survey ID', 'Question Type']

    # THEN: expect survey id to be the same
    assert (df['Survey ID'] == 'xsafa7f17tg67xgbtpxux5ua7o').all()
    # THEN: expect submitted at to be the same value (single answer)
    assert (df['Submitted At'].dt.date == date(2024, 7, 23)).all()

    # THEN: expect the rest of the dataframe to contain the correct data
    assert_frame_equal(
        df[['User ID', 'Question', 'Answer', 'Question Type']],
        pd.DataFrame(
            {
                'User ID': ['f8ama5so7bnaix5z94zj4x77sr'] * 3,
                'Question': [
                    'How likely are you to suggest this app to someone else?',
                    'How can we make this app better for you?',
                    'What is 2 + 2?',
                ],
                'Answer': ['10', 'response 1!!!', 'Its 4, obviously, but this might be a trick question 🤔🤔🤔'],
                'Question Type': ['linear_scale', 'text', 'text'],
            }
        ),
    )


def test_load_with_invalid_values_in_metadata():
    # WHEN: attempt to load a user survey with invalid metadata
    with pytest.raises(ValidationError) as exc, open(SURVEY_DIR / 'invalid_metadata.json', 'r') as metadata_fp, open(
        SURVEY_DIR / 'responses.csv', 'r'
    ) as data_fp:
        load_user_survey(metadata_fp, data_fp)

    # THEN: expect fields with invalid or missing data to be reported as errors
    assert sorted([e['loc'] for e in exc.value.errors()]) == [('questions', 2, 'type'), ('start_time',)]


def test_load_with_missing_values_in_metadata():
    # WHEN: attempt to load a user survey with invalid metadata
    with pytest.raises(ValidationError) as exc, open(
        SURVEY_DIR / 'metadata_without_questions.json', 'r'
    ) as metadata_fp, open(SURVEY_DIR / 'responses.csv', 'r') as data_fp:
        load_user_survey(metadata_fp, data_fp)

    # THEN: expect fields with invalid or missing data to be reported as errors
    assert sorted([e['loc'] for e in exc.value.errors()]) == [('questions',)]


def test_load_with_inconsistent_data():
    # WHEN: attempt to load a user survey with invalid metadata
    with pytest.raises(ValueError) as exc, open(SURVEY_DIR / 'inconsistent_metadata.json', 'r') as metadata_fp, open(
        SURVEY_DIR / 'responses.csv', 'r'
    ) as data_fp:
        load_user_survey(metadata_fp, data_fp)

    # THEN: expect proper issue to be raised
    assert str(exc.value) == 'Feedback submitted before survey start time'


def test_load_with_missing_questions_in_responses():
    # WHEN: attempt to load a user survey with invalid metadata
    with pytest.raises(ValueError) as exc, open(SURVEY_DIR / 'valid_metadata.json', 'r') as metadata_fp, open(
        SURVEY_DIR / 'responses_small.csv', 'r'
    ) as data_fp:
        load_user_survey(metadata_fp, data_fp)

    # THEN: expect proper issue to be raised
    assert str(exc.value) == 'Questions appearing in metadata are missing from metadata file'


#
# Full loader for user survey package
#


def test_load_valid_user_survey_package():
    # WHEN: attempt to load a valid user survey package
    metadata, survey = load_user_survey_package(SURVEY_DIR / 'valid.zip')

    # THEN: expect metadata to be loaded correctly
    assert metadata == SupportPacketMetadata(
        version=1,
        type=SupportPacketTypeEnum.plugin_packet,
        generated_at=1723794756273,
        server_version='9.11.0',
        server_id='rmg9ib5rspy93jxswyc454bwzo',
        license_id='',
        customer_id='',
        extras=Extras(plugin_id='com.mattermost.user-survey', plugin_version='1.1.0'),
    )

    # THEN: expect responses to be loaded correctly
    assert_frame_equal(
        survey[['User ID', 'Question', 'Answer', 'Question Type']],
        pd.DataFrame(
            {
                'User ID': ['f8ama5so7bnaix5z94zj4x77sr'] * 3,
                'Question': [
                    'How likely are you to suggest this app to someone else?',
                    'How can we make this app better for you?',
                    'What is 2 + 2?',
                ],
                'Answer': ['10', 'response 1!!!', 'Its 4, obviously, but this might be a trick question 🤔🤔🤔'],
                'Question Type': ['linear_scale', 'text', 'text'],
            }
        ),
    )


def test_load_invalid_plugin_id():
    # WHEN: attempt to load a user survey package with invalid plugin id
    with pytest.raises(ValueError) as exc:
        load_user_survey_package(SURVEY_DIR / 'invalid_plugin_id.zip')

    # THEN: expect proper issue to be raised
    assert str(exc.value) == 'Not a user survey packet - packet type is com.mattermost.plugin'


#
# Support packet loader tests
#


def test_load_support_packet_full():
    # WHEN: attempt to load a full support packet
    with open(SUPPORT_DIR / 'full.yaml', 'r') as fp:
        sp = load_support_packet_info(fp)

    # THEN: expect support packet to be loaded correctly
    assert sp.license_to == 'Mattermost'
    assert sp.server_os == 'linux'
    assert sp.server_architecture == 'amd64'
    assert sp.build_hash == '4c83724516242843802cc75840b08ead6afbd37b'
    assert sp.database_type == 'postgres'
    assert sp.database_version == '13.10'
    assert sp.database_schema_version == '113'
    assert sp.active_users == 1
    assert sp.license_supported_users == 200000
    assert sp.total_channels == 2
    assert sp.total_posts == 6
    assert sp.total_teams == 1
    assert sp.daily_active_users == 1
    assert sp.monthly_active_users == 1
    assert sp.websocket_connections == 0
    assert sp.master_db_connections == 14
    assert sp.read_db_connections == 0
    assert sp.inactive_user_count == 0
    assert sp.elastic_post_indexing_jobs == []
    assert sp.elastic_post_aggregation_jobs == []
    assert sp.ldap_sync_jobs == []
    assert sp.message_export_jobs == []
    assert sp.data_retention_jobs == []
    assert sp.compliance_jobs == []
    assert sp.bleve_post_indexing_jobs is None
    assert sp.migration_jobs == [
        JobV1(
            id='4555h6cxb38q3rhnyfu95dypxh',
            type='migrations',
            priority=0,
            createat=datetime(2024, 8, 15, 15, 16, 6, 121000, timezone.utc),
            startat=datetime(2024, 8, 15, 15, 16, 20, 530000, timezone.utc),
            lastactivityat=datetime(2024, 8, 15, 15, 16, 21, 2000, timezone.utc),
            status='success',
            progress=0,
            data={
                'last_done': '{"current_table":"ChannelMembers","last_team_id":"crro7gj13bdzfjm4rmm6ept6sa","last_channel_id":"mpmdxijsftdodkzbehncatthcr","last_user":"wg94o7yd4jyxjbxoihettwgmah"}',  # noqa: E501
                'migration_key': 'migration_advanced_permissions_phase_2',
            },
        )
    ]


def test_support_packet_invalid():
    # WHEN: attempt to load an invalid support packet
    with pytest.raises(ValidationError) as exc, open(SUPPORT_DIR / 'invalid.yaml', 'r') as fp:
        load_support_packet_info(fp)

    assert sorted([e['loc'] for e in exc.value.errors()]) == [
        ('database_schema_version',),  # Int instead of string
        ('server_version',),  # Missing
    ]


#
# Full loader for support packet v1
#


def test_load_full_support_packet_v1_with_metadata():
    # WHEN: attempt to load a valid support packet
    metadata, sp = load_support_packet_file(SUPPORT_DIR / 'valid_with_metadata.zip')

    # THEN: expect metadata to be loaded correctly
    assert metadata == SupportPacketMetadata(
        version=1,
        type=SupportPacketTypeEnum.support_packet,
        generated_at=datetime(2024, 8, 19, 13, 46, 45, 94000, tzinfo=timezone.utc),
        server_version='9.11.0',
        server_id='rmg9ib5rspy93jxswyc454bwzo',
        license_id='mud3ihm4938dxncqasxt14xxch',
        customer_id='p9un369a67gimj4yd6i6ib39wh',
    )

    # THEN: expect responses to be loaded correctly
    assert sp.license_to == 'Mattermost'
    assert sp.server_os == 'linux'
    assert sp.server_architecture == 'amd64'
    assert sp.build_hash == '0bc2ddd42375a75ab14e63f038165150d4f07659'


def test_load_full_support_packet_v1_without_metadata():
    # WHEN: attempt to load a valid support packet
    metadata, sp = load_support_packet_file(SUPPORT_DIR / 'valid_without_metadata.zip')

    # THEN: expect metadata to be loaded correctly
    assert metadata is None

    # THEN: expect responses to be loaded correctly
    assert sp.license_to == 'Mattermost'
    assert sp.server_os == 'linux'
    assert sp.server_architecture == 'amd64'
    assert sp.build_hash == '4c83724516242843802cc75840b08ead6afbd37b'
