import json
import os
from enum import Enum
from typing import IO, Tuple
from zipfile import ZipFile

import pandas as pd
import yaml

from utils.packets.models.metadata import SupportPacketMetadata
from utils.packets.models.user_survey import UserSurveyMetadata

SUPPORT_PACKET_METADATA_FILE = 'metadata.yaml'
SURVEY_METADATA_FILE = 'survey_metadata.json'
SURVEY_DATA_FILE = 'responses.csv'


class KnownPacketTypes(str, Enum):
    USER_SURVEY = 'com.mattermost.user-survey'


def load_metadata(metadata_file: IO) -> SupportPacketMetadata:
    """
    Load support packet metadata from a YAML file.
    """
    metadata = yaml.safe_load(metadata_file)
    return SupportPacketMetadata(**metadata)


class UserSurveyFixedColumns(str, Enum):
    user_id = 'User ID'
    submitted_at = 'Submitted At'


def load_user_survey(survey_metadata_file: IO, survey_data_file: IO) -> pd.DataFrame:
    """
    Load user survey data and metadata.

    :param survey_metadata_file: A file-like object to survey metadata file (survey_metadata.json).
    :param survey_metadata_file: A file-like object to survey data file (results.csv).
    """
    data = json.load(survey_metadata_file)
    metadata = UserSurveyMetadata(**data)

    df = pd.read_csv(survey_data_file, parse_dates=[UserSurveyFixedColumns.submitted_at], date_format='%d %B %Y')

    question_types = {q.text: q.type.value for q in metadata.questions}

    # Unpivot dataframe so that a shared structure can be used
    df = df.melt(id_vars=[e.value for e in UserSurveyFixedColumns], var_name='Question', value_name='Answer')

    # Add survey data
    df['Survey ID'] = metadata.id

    # Add question type in each row
    df['Question Type'] = df['Question'].map(question_types)

    # Validation
    if df['Question Type'].isnull().values.any():
        raise ValueError('Invalid value in question type - possibly caused by invalid value in metadata')

    if (df[UserSurveyFixedColumns.submitted_at.value].dt.date < metadata.start_time).any():
        raise ValueError('Feedback submitted before survey start time')

    if set(df['Question'].unique().tolist()) != set(question_types.keys()):
        raise ValueError('Questions appearing in metadata are missing from metadata file')

    return df


def load_user_survey_package(user_survey_zip_file: str | os.PathLike) -> Tuple[SupportPacketMetadata, pd.DataFrame]:
    with ZipFile(user_survey_zip_file, 'r') as zipfile:
        with zipfile.open(SUPPORT_PACKET_METADATA_FILE) as metadata_fp:
            metadata = load_metadata(metadata_fp)

        if metadata.extras.plugin_id != KnownPacketTypes.USER_SURVEY:
            raise ValueError(f'Not a user survey packet - packet type is {metadata.extras.plugin_id}')

        with zipfile.open(SURVEY_METADATA_FILE) as survey_metadata_fp, zipfile.open(SURVEY_DATA_FILE) as survey_data_fp:
            survey_data = load_user_survey(survey_metadata_fp, survey_data_fp)

    return metadata, survey_data
