import json
from datetime import timedelta

import click
from click import ClickException, UsageError
from snowflake.connector import ProgrammingError
from sqlalchemy.exc import SQLAlchemyError

from utils.db.helpers import snowflake_engine
from utils.rudderstack.service import list_event_tables, move_tables


@click.group()
def rudder() -> None:
    """
    Rudderstack helpers. Offers a variety of subcommands for managing rudderstack tables from the command line.
    """
    pass


@rudder.command('list')
@click.argument('database')
@click.argument('schema')
@click.option('-a', '--account', envvar='SNOWFLAKE_ACCOUNT', required=True, help='the name of the snowflake account')
@click.option(
    '-u', '--user', envvar='SNOWFLAKE_USER', required=True, help='the name of the user for connecting to snowflake'
)
@click.option(
    '-p',
    '--password',
    envvar='SNOWFLAKE_PASSWORD',
    required=True,
    prompt=True,
    hide_input=True,
    help='the password for connecting to snowflake',
)
@click.option(
    '-w', '--warehouse', envvar='SNOWFLAKE_WAREHOUSE', help='the warehouse to use when connecting to snowflake'
)
@click.option('-r', '--role', envvar='SNOWFLAKE_ROLE', help='the role to use when connecting to snowflake')
@click.option('--min-rows', type=click.IntRange(0), help='include tables with at least this number of rows (inclusive)')
@click.option(
    '--max-rows', type=click.IntRange(0), help='include tables with no more rows than this number of rows (inclusive)'
)
@click.option(
    '--min-age', type=click.IntRange(0), help='include tables that have been created before to this number of days ago'
)
@click.option(
    '--max-age', type=click.IntRange(0), help='include tables that have been created up to this number of days ago'
)
@click.option('--format-json', is_flag=True, help='use JSON as output format')
def list_tables(
    database: str,
    schema: str,
    account: str,
    user: str,
    password: str,
    warehouse: str,
    role: str,
    min_rows: int,
    max_rows: int,
    min_age: int,
    max_age: int,
    format_json: bool,
) -> None:
    """
    List Rudderstack event tables for given database and schema.
    """
    engine = snowflake_engine(
        {
            "SNOWFLAKE_ACCOUNT": account,
            "SNOWFLAKE_USER": user,
            "SNOWFLAKE_PASSWORD": password,
            "SNOWFLAKE_DATABASE": database,
            "SNOWFLAKE_SCHEMA": schema,
            "SNOWFLAKE_WAREHOUSE": warehouse,
            "SNOWFLAKE_ROLE": role,
        }
    )
    try:
        result = list_event_tables(
            engine,
            database,
            schema,
            min_rows=min_rows,
            max_rows=max_rows,
            min_age=timedelta(days=min_age) if min_age else None,
            max_age=timedelta(days=max_age) if max_age else None,
        )
        if format_json:
            click.echo(json.dumps({"new_tables": result}))
        else:
            for table in result:
                click.echo(table)
        # Make sure that exit code is > 0 if tables are found
        if result:
            raise click.ClickException("New tables found...")
    except ValueError as e:
        raise UsageError(str(e))
    finally:
        engine.dispose()


@rudder.command('move', short_help='Move tables to target database/schema.')
@click.argument('source_database')
@click.argument('source_schema')
@click.argument('target_database')
@click.argument('target_schema')
@click.argument('input', type=click.File('r'))
@click.option('-a', '--account', envvar='SNOWFLAKE_ACCOUNT', required=True, help='the name of the snowflake account')
@click.option(
    '-u', '--user', envvar='SNOWFLAKE_USER', required=True, help='the name of the user for connecting to snowflake'
)
@click.option(
    '-p',
    '--password',
    envvar='SNOWFLAKE_PASSWORD',
    required=True,
    prompt=True,
    hide_input=True,
    help='the password for connecting to snowflake',
)
@click.option(
    '-w', '--warehouse', envvar='SNOWFLAKE_WAREHOUSE', help='the warehouse to use when connecting to snowflake'
)
@click.option('-r', '--role', envvar='SNOWFLAKE_ROLE', help='the role to use when connecting to snowflake')
@click.option('--postfix', type=str, help='append this postfix to all tables moved')
def move(
    source_database: str,
    source_schema: str,
    target_database: str,
    target_schema: str,
    input: click.File,
    account: str,
    user: str,
    password: str,
    warehouse: str,
    role: str,
    postfix: str = None,
) -> None:
    """
    Move all tables in <INPUT> from source database/schema to target/database schema.

    INPUT can be either a filename or - for stdin.

    Example:
    rudder move RAW EVENTS ARCHIVE OLD_EVENTS tables.txt # add connection options here
    """
    engine = snowflake_engine(
        {
            "SNOWFLAKE_ACCOUNT": account,
            "SNOWFLAKE_USER": user,
            "SNOWFLAKE_PASSWORD": password,
            "SNOWFLAKE_DATABASE": source_database,
            "SNOWFLAKE_SCHEMA": source_schema,
            "SNOWFLAKE_WAREHOUSE": warehouse,
            "SNOWFLAKE_ROLE": role,
        }
    )

    def table_provider():
        for table in input:
            if table.strip():
                click.echo(f"Moving {source_database}.{source_schema}.{table}", nl=False)
                yield table
                click.secho(" \N{check mark}", fg="green", bold=True)

    try:
        move_tables(engine, table_provider(), source_database, source_schema, target_database, target_schema, postfix)
    except (ProgrammingError, SQLAlchemyError) as e:
        raise ClickException(str(e))
    finally:
        engine.dispose()


if __name__ == '__main__':
    rudder()
