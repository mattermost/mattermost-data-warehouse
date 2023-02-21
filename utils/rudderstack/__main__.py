from datetime import timedelta

import click
from click import UsageError

from utils.db.helpers import snowflake_engine
from utils.rudderstack.service import list_event_tables


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
    '--max-age', type=click.IntRange(0), help='include tables that have been created up to this number of days ago'
)
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
    max_age: int,
) -> None:
    """
    List Rudderstack event tables for given database and schema.
    \f

    :param database: the database containing the schema.
    :param schema: the schema containing the events.
    :param account: the name of the snowflake account.
    :param user: the name of the user for connecting to snowflake.
    :param password: the password for connecting to snowflake.
    :param warehouse: the warehouse to use when connecting to snowflake.
    :param role: the role to use when connecting to snowflake.
    :param min_rows: include tables with at least this number of rows (inclusive).
    :param max_rows: include tables with no more rows than this number of rows (inclusive).
    :param max_age: include tables that have been created up to this number of days ago.
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
            max_age=timedelta(days=max_age) if max_age else None,
        )
        for table in result:
            click.echo(table)
    except ValueError as e:
        raise UsageError(str(e))


if __name__ == '__main__':
    rudder()
