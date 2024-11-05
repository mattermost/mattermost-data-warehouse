import logging
import re
from collections import namedtuple
from datetime import timedelta
from textwrap import dedent
from typing import List, Optional, Tuple

import pandas as pd
from snowflake.sqlalchemy import URL, MergeInto
from sqlalchemy import Column, MetaData, Table, and_, bindparam, create_engine, inspect, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import InvalidRequestError, ProgrammingError

logger = logging.getLogger(__name__)

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


def validate_object_name(object: str, name: str):
    """
    Validates that the object name contains only alphanumeric characters and underscores.
    """
    if not bool(re.match(r'^[\w]+$', name)):
        raise ValueError(
            f"{object} name '{name}' is invalid. Only alphanumeric characters and underscores are allowed."
        )


def clone_table(
    conn: Connection,
    source_database: str,
    source_schema: str,
    source_table: str,
    target_database: str,
    target_schema: str,
    target_table: str,
    replace_if_exists=False,
):
    """
    Clones a table from a source database/schema to a target database/schema. Assumes that target database/schema
    already exist.
    """

    validate_object_name('Source database', source_database)
    validate_object_name('Source schema', source_schema)
    validate_object_name('Source table', source_table)
    validate_object_name('Target database', target_database)
    validate_object_name('Target schema', target_schema)
    validate_object_name('Target table', target_table)

    source = f'{source_database}.{source_schema}.{source_table}'
    target = f'{target_database}.{target_schema}.{target_table}'

    if replace_if_exists:
        logger.info(f'Creating (or replacing) table {target} as a clone of {source}')
        conn.execute(
            dedent(
                f"""
                CREATE OR REPLACE TABLE {target_database}.{target_schema}.{target_table}
                CLONE {source_database}.{source_schema}.{source_table}
            """
            )
        )
    else:
        logger.info(f'Creating table {target} as a clone of {source}')
        try:
            conn.execute(
                dedent(
                    f"""
                    CREATE TABLE {target_database}.{target_schema}.{target_table}
                    CLONE {source_database}.{source_schema}.{source_table}
                """
                )
            )
        except ProgrammingError as e:
            if 'already exists' in str(e):
                logger.info(f'Table {target} already exists. Skipping creation.')
            else:
                raise e


def diff_columns(t1: Table, t2: Table) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
    """
    Compares two tables and returns the columns that exist only on each table.
    """
    table_1_cols = frozenset([(c.name, str(c.type)) for c in t1.columns])
    table_2_cols = frozenset([(c.name, str(c.type)) for c in t2.columns])
    return [col for col in table_1_cols if col not in table_2_cols], [
        col for col in table_2_cols if col not in table_1_cols
    ]


def escape_special_column_names(column: str | Column) -> str:
    if isinstance(column, Column) and column.name == '_':
        return f'{column.table.name}."_"'
    if column == '_':
        return '"_"'
    return column


def load_table_definition(conn: Connection, schema: str, table: str) -> Table:
    """
    Loads the table definition from the database.
    """
    try:
        meta = MetaData(schema=schema)
        meta.reflect(conn, only=[table])
        return meta.tables[f'{schema}.{table}']
    except InvalidRequestError:
        # Table doesn't exist
        return None


def merge_event_delta_table_into(
    conn: Connection,
    base_schema: str,
    base_table: str,
    delta_schema: str,
    delta_table: str,
    dedup_prune_days: int = 7,
):
    """
    Merges a delta table containing event data into a base table. Assumes that the base table already exists.

    On match: do nothing.
    On not match: insert the new row.
    """

    validate_object_name('Base schema', base_schema)
    validate_object_name('Base table', base_table)
    validate_object_name('Delta schema', delta_schema)
    validate_object_name('Delta table', delta_table)

    # Load base and delta table information
    logging.info('Loading table definitions')
    t_base = load_table_definition(conn, base_schema, base_table)
    t_delta = load_table_definition(conn, delta_schema, delta_table)

    if t_base is None or t_delta is None:
        raise ValueError('Base and/or delta table does not exist!')

    _, delta_only_columns = diff_columns(t_base, t_delta)
    if delta_only_columns:
        logging.info('Adding new columns to base table')
        # Add new columns to base table in alphabetical order
        new_columns = ', '.join([f'{col[0]} {col[1]}' for col in sorted(delta_only_columns, key=lambda x: x[0])])
        conn.execute(f'ALTER TABLE {base_schema}.{base_table} ADD COLUMN {new_columns}')

    logger.info('Checking if delta table contains data')
    min_date = conn.execute(text(f'SELECT MIN(received_at) FROM {delta_schema}.{delta_table}')).scalar()

    if not min_date:
        # Nothing to merge
        logger.info('No data to merge')
        return

    logger.info('Merging')

    first_duplicate_date = (min_date - timedelta(days=dedup_prune_days)).isoformat()

    merge = MergeInto(
        target=t_base,
        source=t_delta,
        on=and_(t_base.c.id == t_delta.c.id, t_base.c.received_at >= bindparam('first_duplicate_date')),
    )
    merge.when_matched_then_update().values(after=t_base.c.after)
    value_mapping = {
        escape_special_column_names(c.name): escape_special_column_names(t_delta.c[c.name]) for c in t_delta.columns
    }

    merge.when_not_matched_then_insert().values(**value_mapping)
    # Workaround for https://github.com/snowflakedb/snowflake-sqlalchemy/issues/536
    stmt = text(merge.__repr__()).bindparams(first_duplicate_date=first_duplicate_date)
    conn.execute(stmt)

    # Delete rows from delta table that were merged
    conn.execute(
        text(
            f'DELETE FROM {delta_schema}.{delta_table} '
            f'WHERE id IN (SELECT id FROM {base_schema}.{base_table} WHERE received_at >= :first_duplicate_date)'
        ).bindparams(first_duplicate_date=first_duplicate_date)
    )


def load_query(conn: Connection, query: str) -> pd.DataFrame:
    """
    Loads a query into a DataFrame.
    """
    return pd.read_sql(query, conn)
