from datetime import date

from utils.helpers import daterange


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
