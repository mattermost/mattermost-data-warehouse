from datetime import date, datetime
from enum import Enum
from typing import List
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class QuestionType(str, Enum):
    linear_scale = 'linear_scale'
    text = 'text'


class Question(BaseModel):
    id: UUID
    text: str
    type: QuestionType


class UserSurveyMetadata(BaseModel):
    id: str = Field(min_length=26, max_length=26)
    detractor_count: int
    passive_count: int
    promoter_count: int
    receipt_count: int
    response_count: int
    start_time: date
    duration: str  # Shouldn't it be integer?
    questions: List[Question] = Field(min_items=1)

    @field_validator('start_time', mode='before')
    def start_time_validate(cls, v):
        return datetime.strptime(v, '%d %B %Y')
