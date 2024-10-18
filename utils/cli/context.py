from contextlib import contextmanager

from utils.db.helpers import snowflake_engine


@contextmanager
def snowflake_engine_context(conn_dict: dict):
    try:
        engine = snowflake_engine(conn_dict)
        yield engine
    finally:
        engine.dispose()
