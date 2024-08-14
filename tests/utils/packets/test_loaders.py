from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pydantic import ValidationError

from utils.packets.loaders import load_metadata, load_user_survey
from utils.packets.models.metadata import Extras, SupportPacketMetadata, SupportPacketTypeEnum

FIXTURE_DIR = Path(__file__).parent.parent / 'fixtures' / 'packets'
METADATA_DIR = FIXTURE_DIR / 'metadata'
SURVEY_DIR = FIXTURE_DIR / 'user_survey'


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
    result = load_metadata(metadata)

    assert result == expected


@pytest.mark.parametrize(
    'metadata,error_fields',
    [
        pytest.param(METADATA_DIR / 'invalid' / 'invalid_timestamp.yaml', [('generated_at',)], id='invalid timestamp'),
        pytest.param(METADATA_DIR / 'invalid' / 'invalid_type.yaml', [('type',)], id='invalid type'),
        pytest.param(METADATA_DIR / 'invalid' / 'missing_extras.yaml', [('extras',)], id='missing extras'),
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
    with pytest.raises(ValidationError) as exc:
        load_metadata(metadata)

    # THEN: expect fields with invalid or missing data to be reported as errors
    assert sorted([e['loc'] for e in exc.value.errors()]) == error_fields


#
# User survey tests
#


def test_load_user_survey():
    # WHEN: attempt to load a user survey
    df = load_user_survey(SURVEY_DIR / 'valid_metadata.json', SURVEY_DIR / 'responses.csv')

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
                'Answer': [10, 'response 1!!!', 'Its 4, obviously, but this might be a trick question 🤔🤔🤔'],
                'Question Type': ['linear_scale', 'text', 'text'],
            }
        ),
    )


def test_load_with_invalid_values_in_metadata():
    # WHEN: attempt to load a user survey with invalid metadata
    with pytest.raises(ValidationError) as exc:
        load_user_survey(SURVEY_DIR / 'invalid_metadata.json', SURVEY_DIR / 'responses.csv')

    # THEN: expect fields with invalid or missing data to be reported as errors
    assert sorted([e['loc'] for e in exc.value.errors()]) == [('questions', 2, 'type'), ('start_time',)]


def test_load_with_missing_values_in_metadata():
    # WHEN: attempt to load a user survey with invalid metadata
    with pytest.raises(ValidationError) as exc:
        load_user_survey(SURVEY_DIR / 'metadata_without_questions.json', SURVEY_DIR / 'responses.csv')

    # THEN: expect fields with invalid or missing data to be reported as errors
    assert sorted([e['loc'] for e in exc.value.errors()]) == [('questions',)]


def test_load_with_inconsistent_data():
    # WHEN: attempt to load a user survey with invalid metadata
    with pytest.raises(ValueError) as exc:
        load_user_survey(SURVEY_DIR / 'inconsistent_metadata.json', SURVEY_DIR / 'responses.csv')

    # THEN: expect proper issue to be raised
    assert str(exc.value) == 'Feedback submitted before survey start time'


def test_load_with_missing_questions_in_responses():
    # WHEN: attempt to load a user survey with invalid metadata
    with pytest.raises(ValueError) as exc:
        load_user_survey(SURVEY_DIR / 'valid_metadata.json', SURVEY_DIR / 'responses_small.csv')

    # THEN: expect proper issue to be raised
    assert str(exc.value) == 'Questions appearing in metadata are missing from metadata file'
