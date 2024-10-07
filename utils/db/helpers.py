from collections import namedtuple
from typing import List, Optional

import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection, Engine

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
        conn.execute(
            f"COPY INTO {schema}.{table} FROM @{schema}.{table} FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"')"  # noqa: E501
        )


def move_table(
    conn: Connection,
    table: str,
    source_database: str,
    source_schema: str,
    target_database: str,
    target_schema: str,
    postfix: str = None,
) -> None:
    """
    Moves table from source database/schema to target database/schema.

    :param conn: the SQLAlchemy connection to use for moving the table information.
    :param table: the table to move.
    :param source_database: the database where the table is currently located at.
    :param source_schema: the schema where the table is currently located at.
    :param target_database: the database to move the table to.
    :param target_schema: the schema to move the table to.
    :param postfix: (optional) postfix to append to the table at the target database.
    """
    query = f'''
        ALTER TABLE {source_database}.{source_schema}.{table}
        RENAME TO {target_database}.{target_schema}.{table}{postfix if postfix else ''}
    '''
    conn.execute(query)


def max_column_value(conn: Connection, target_schema: str, table: str, column: str) -> Optional:
    """
    Returns the last date of a column.
    """
    return conn.execute(f"SELECT MAX({column})::date AS latest_date FROM {target_schema}.{table}").scalar()


def copy_from_stage(
    conn: Connection, stage_schema: str, stage_name: str, path: str, target_schema: str, target_table: str
):
    """
    Full load the data on the target table.
    """
    query = f"""
        COPY INTO {target_schema}.{target_table}
        FROM @{stage_schema}.{stage_name}/{path}
        ON_ERROR = 'CONTINUE';
    """
    conn.execute(query)


def table_exists(conn: Connection, target_schema: str, target_table: str) -> bool:
    """
    Returns True if the table exists.
    """
    return inspect(conn).has_table(target_table, schema=target_schema)


def upsert_dataframe_to_table(
    conn: Connection, target_schema: str, target_table: str, df: pd.DataFrame, server_id: str, source_uri: str
):
    """
    Upserts a dataframe into a table. If the table doesn't exist, it's automatically created.
    """
    if table_exists(conn, target_schema, target_table):
        conn.execute(
            text(
                f"""
                DELETE FROM {target_schema}.{target_table}
                WHERE
                    metadata_server_id = :server_id
                    AND source = :source_uri
            """
            ),
            server_id=server_id,
            source_uri=source_uri,
        )

    df.to_sql(target_table, conn, schema=target_schema, if_exists="append", index=False)
