import pytest
from sqlalchemy import VARCHAR, Column, DateTime, Integer, MetaData, Table


@pytest.fixture
def base_table():
    base_metadata = MetaData(schema='base_schema')
    return Table(
        'base_table',
        base_metadata,
        Column('id', VARCHAR(10)),
        Column('column_a', VARCHAR(10)),
        Column('_', VARCHAR(10)),
        Column('after', Integer()),
        Column('received_at', DateTime()),
    )


@pytest.fixture
def delta_table_1():
    delta_metadata = MetaData(schema='delta_schema')
    return Table(
        'delta_table',
        delta_metadata,
        Column('id', VARCHAR(10)),
        Column('column_a', VARCHAR(10)),
        Column('_', VARCHAR(10)),
        Column('after', Integer()),
        Column('received_at', DateTime()),
    )


@pytest.fixture
def delta_table_2():
    delta_metadata = MetaData(schema='delta_schema')
    return Table(
        'delta_table',
        delta_metadata,
        Column('id', VARCHAR(10)),
        Column('column_a', VARCHAR(10)),
        Column('_', VARCHAR(10)),
        Column('after', Integer()),
        Column('received_at', DateTime()),
        Column('extra', VARCHAR(255)),
        Column('column_b', Integer()),
    )
