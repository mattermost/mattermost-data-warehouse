from datetime import datetime, timedelta, timezone
from typing import List

from sqlalchemy.engine import Engine

from utils.db.helpers import get_table_stats_for_schema

# Tables that exist in rudderstack schema.
RUDDERSTACK_TABLES = frozenset(
    ['IDENTIFIES', 'USERS', 'TRACKS', 'PAGES', 'SCREENS', 'GROUPS', 'ALIASES', 'RUDDER_DISCARDS']
)


def list_event_tables(
    engine: Engine, database: str, schema: str, min_rows: int = None, max_rows: int = None, max_age: timedelta = None
) -> List[str]:
    """
    Loads stats for all tables in a schema.

    :param engine: the SQLAlchemy engine to use for loading the table information.
    :param database: the database where the schema is located at.
    :param schema: the schema to load information for.
    :param min_rows: ignore tables with less than this number of rows (if specified).
    :param max_rows: ignore tables with more than this number of rows (if specified).
    :param max_age: ignore tables created more than this timedelta ago.
    :return: a list of stats for all tables in the schema.
    """
    stats = get_table_stats_for_schema(engine, database, schema)
    existing_rudderstack_tables = [t for t in stats if t.name.upper() in RUDDERSTACK_TABLES]

    if not existing_rudderstack_tables:
        raise ValueError(f'Schema {database}.{schema} does not contain any Rudderstack tables.')

    current_time = datetime.utcnow()
    filters = [
        lambda table: table.name.upper() not in RUDDERSTACK_TABLES,
        lambda table: table.rows >= min_rows if min_rows else True,
        lambda table: table.rows <= max_rows if max_rows else True,
        lambda table: table.created_at.astimezone(timezone.utc).replace(tzinfo=None) > current_time - max_age
        if max_age
        else True,
    ]
    filtered_tables = filter(lambda x: all(f(x) for f in filters), stats)
    return [t.name for t in filtered_tables]
