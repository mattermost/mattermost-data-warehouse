import logging

import click

from utils.cli.logging import initialize_cli_logging
from utils.db.helpers import snowflake_engine
from utils.push_proxy.service import load_stage_to_table

initialize_cli_logging(logging.INFO, 'stderr')


@click.command()
@click.argument('stage')
@click.argument('table')
@click.option('--prefix', help='the prefix to use for loading data from the S3 bucket')
@click.option('-a', '--account', envvar='SNOWFLAKE_ACCOUNT', required=True, help='the name of the snowflake account')
@click.option('-d', '--database', envvar='SNOWFLAKE_DATABASE', required=True, help='the name of the snowflake database')
@click.option(
    '-s',
    '--schema',
    envvar='SNOWFLAKE_SCHEMA',
    required=True,
    help='the name of the snowflake schema where the stage and the target table exist.',
)
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
def push_proxy(
    stage: str,
    table: str,
    prefix: str,
    account: str,
    database: str,
    schema: str,
    user: str,
    password: str,
    warehouse: str,
    role: str,
) -> None:
    """
    Load data from S3 to Snowflake table using the provided stage. Handle details such as incremental reading etc.
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

    with engine.connect() as conn:
        load_stage_to_table(conn, schema, stage, prefix, schema, table)
    engine.dispose()


if __name__ == '__main__':
    push_proxy()
