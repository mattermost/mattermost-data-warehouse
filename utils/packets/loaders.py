import json
import os
from enum import Enum

import pandas as pd
import yaml

from utils.packets.models.metadata import SupportPacketMetadata
from utils.packets.models.user_survey import UserSurveyMetadata


def load_metadata(path: str | os.PathLike) -> SupportPacketMetadata:
    with open(path, 'r') as fp:
        metadata = yaml.safe_load(fp)
        return SupportPacketMetadata(**metadata)


class UserSurveyFixedColumns(str, Enum):
    user_id = 'User ID'
    submitted_at = 'Submitted At'


def load_user_survey(survey_metadata_path: str | os.PathLike, survey_data_path: str | os.PathLike) -> pd.DataFrame:
    with open(survey_metadata_path, 'r') as fp:
        data = json.load(fp)
        metadata = UserSurveyMetadata(**data)

    df = pd.read_csv(survey_data_path, parse_dates=[UserSurveyFixedColumns.submitted_at], date_format='%d %B %Y')

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
