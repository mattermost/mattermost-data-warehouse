import logging

import click

from utils.cli.context import snowflake_engine_context
from utils.cli.logging import initialize_cli_logging
from utils.db.helpers import clone_table, merge_event_delta_table_into

initialize_cli_logging(logging.INFO, 'stderr')


@click.group()
@click.option('-a', '--account', envvar='SNOWFLAKE_ACCOUNT', required=True, help='the name of the snowflake account')
@click.option('-d', '--database', envvar='SNOWFLAKE_DATABASE', required=True, help='the name of the snowflake database')
@click.option('-s', '--schema', envvar='SNOWFLAKE_SCHEMA', required=True, help='the name of the snowflake schema')
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
@click.pass_context
def snowflake(
    ctx: click.Context, account: str, database: str, schema: str, user: str, password: str, warehouse: str, role: str
) -> None:
    """
    Snowflake helpers. Offers a variety of subcommands for performing dynamic actions in snowflake database.
    """
    ctx.ensure_object(dict)
    ctx.obj['engine'] = ctx.with_resource(
        snowflake_engine_context(
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
    )


@snowflake.command()
@click.pass_context
@click.argument('source_table', type=str)
@click.argument('target_db', type=str)
@click.argument('target_schema', type=str)
@click.argument('target_table', type=str)
@click.option('--replace', is_flag=True, help='Replace the target table if it already exists.', default=False)
def clone(
    ctx: click.Context,
    source_table: str,
    target_db: str,
    target_schema: str,
    target_table: str,
    replace: bool,
):
    with ctx.obj['engine'].begin() as conn, conn.begin():
        clone_table(
            conn,
            ctx.parent.params['database'],
            ctx.parent.params['schema'],
            source_table,
            target_db,
            target_schema,
            target_table,
            replace_if_exists=replace,
        )


@snowflake.command()
@click.pass_context
@click.argument('base_table', type=str)
@click.argument('delta_table_db', type=str)
@click.argument('delta_table_schema', type=str)
@click.argument('delta_table', type=str)
@click.option(
    '--dedup-prune-days', type=int, default=7, help='Number of most recent days in base table to check for duplicates.'
)
def merge(
    ctx: click.Context,
    base_table: str,
    delta_table_schema: str,
    delta_table: str,
    dedup_prune_days: int,
):
    with ctx.obj['engine'].begin() as conn, conn.begin():
        merge_event_delta_table_into(
            conn,
            ctx.parent.params['schema'],
            base_table,
            delta_table_schema,
            delta_table,
            dedup_prune_days=dedup_prune_days,
        )


if __name__ == '__main__':
    snowflake()
