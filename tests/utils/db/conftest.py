import pytest
from sqlalchemy import VARCHAR, Column, DateTime, Integer, MetaData, Table


@pytest.fixture
def test_deferred_merge_tables():
    base_metadata = MetaData(schema='base_schema')
    base_table = Table(
        'base_table',
        base_metadata,
        Column('id', VARCHAR(10)),
        Column('column_a', VARCHAR(10)),
        Column('_', VARCHAR(10)),
        Column('after', Integer()),
        Column('received_at', DateTime()),
    )

    delta_metadata = MetaData(schema='delta_schema')
    delta_table = Table(
        'delta_table',
        delta_metadata,
        Column('id', VARCHAR(10)),
        Column('column_a', VARCHAR(10)),
        Column('_', VARCHAR(10)),
        Column('after', Integer()),
        Column('received_at', DateTime()),
    )

    return base_table, delta_table
