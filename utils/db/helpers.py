from collections import namedtuple
from typing import List, TextIO

from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

TableStats = namedtuple('TableStats', ['schema', 'name', 'rows', 'created_at'])


def snowflake_engine(config: dict) -> Engine:
    """
    Creates a new SQLAlchemy engine for connecting to Snowflake using the provided configuration.

    :param config: A dictionary containing the connection details.
    :return: an SQL alchemy engine.
    """
    full_config = {
        'account': config.get('SNOWFLAKE_ACCOUNT'),
        'user': config.get('SNOWFLAKE_USER'),
        'password': config.get('SNOWFLAKE_PASSWORD'),
        'database': config.get('SNOWFLAKE_DATABASE'),
        'schema': config.get('SNOWFLAKE_SCHEMA'),
        'warehouse': config.get('SNOWFLAKE_WAREHOUSE'),
        'role': config.get('SNOWFLAKE_ROLE'),
    }
    return create_engine(
        URL(**{k: v for k, v in full_config.items() if v is not None}), connect_args={"sslcompression": 0}
    )


def get_table_stats_for_schema(engine: Engine, database: str, schema: str) -> List[TableStats]:
    """
    Loads stats for all tables in a schema.

    :param engine: the SQLAlchemy engine to use for loading the table information.
    :param database: the database where the schema is located at.
    :param schema: the schema to load information for.
    :return: a list of stats for all tables in the schema.
    """
    with engine.begin() as conn:
        # Get row count for each table in schema
        result = conn.execute(
            f'''
            SELECT DISTINCT
                table_schema ,
                table_name,
                row_count,
                created
            FROM
                "{database}".information_schema.tables
            WHERE
                table_schema ILIKE '{schema}'
        '''
        )
        return [TableStats(*row) for row in result]


def upload_csv_as_table(engine: Engine, file: str, schema: str, table: str) -> None:
    """
    Uploads a CSV file to target table in snowflake. Truncates data if table already exists.

    CSV file must have a header.

    :param engine: the engine to use for connecting to Snowflake.
    :param file: the absolute filepath of the file to upload to Snowflake.
    :param schema: the schema to upload the file to.
    :param table: the name of the table to upload the data from the file to.
    """
    # Truncate table, upload file and replace table content's within a transaction.
    with engine.begin() as conn:
        conn.execute(f"TRUNCATE TABLE {schema}.{table}")
        conn.execute(f"CREATE TEMPORARY STAGE IF NOT EXISTS {schema}.{table}")
        conn.execute(f"PUT file://{file} @{schema}.{table} OVERWRITE=TRUE")
        conn.execute(f"COPY INTO {schema}.{table} FROM @{schema}.{table} FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)")
