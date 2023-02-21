from datetime import datetime, timezone

import pytest

from utils.db.helpers import TableStats
from utils.rudderstack.service import list_event_tables

MOCK_TABLE_STATS = [
    TableStats('schema', 'table1', 3, datetime(2022, 12, 28, 23, 55, 59, tzinfo=timezone.utc)),
    TableStats('schema', 'table2', 20, datetime(2023, 1, 15, 12, 23, 45, tzinfo=timezone.utc)),
    TableStats('schema', 'table3', 200, datetime(2021, 11, 15, 12, 23, 45, tzinfo=timezone.utc)),
    TableStats('schema', 'tracks', 223, datetime(2021, 11, 15, 12, 23, 45, tzinfo=timezone.utc)),
]


@pytest.mark.parametrize(
    'min_rows,max_rows,max_age,expected_tables',
    [
        pytest.param(
            None,
            None,
            None,
            ['table1', 'table2', 'table3'],
            id='no extra filters',
        ),
        pytest.param(
            20,
            None,
            None,
            ['table2', 'table3'],
            id='min rows',
        ),
        pytest.param(
            None,
            20,
            None,
            ['table1', 'table2'],
            id='max rows',
        ),
        pytest.param(
            None,
            None,
            datetime.utcnow() - datetime(2023, 1, 1),
            ['table2'],
            id='max age',
        ),
        pytest.param(
            5,
            None,
            datetime.utcnow() - datetime(2022, 1, 1),
            ['table2'],
            id='multiple filters',
        ),
    ],
)
def test_list_event_tables(mocker, min_rows, max_rows, max_age, expected_tables):
    # GIVEN: a mock SQL engine
    mock_engine = mocker.Mock()
    # GIVEN: table_stats are returned
    mock_get_table_stats = mocker.patch('utils.rudderstack.service.get_table_stats_for_schema')
    mock_get_table_stats.return_value = MOCK_TABLE_STATS

    # WHEN: call to list tables is performed
    result = list_event_tables(mock_engine, 'database', 'schema', min_rows=min_rows, max_rows=max_rows, max_age=max_age)

    # THEN: expect result to match expected table names
    assert result == expected_tables
