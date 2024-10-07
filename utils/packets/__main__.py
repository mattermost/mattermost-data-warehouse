import logging
from contextlib import contextmanager

import click

from utils.db.helpers import snowflake_engine
from utils.helpers import initialize_cli_logging
from utils.packets.service import ingest_support_packet, ingest_survey_packet

initialize_cli_logging(logging.INFO, 'stderr')


@contextmanager
def snowflake_engine_context(conn_dict: dict):
    try:
        engine = snowflake_engine(conn_dict)
        yield engine
    finally:
        engine.dispose()


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
def packets(
    ctx: click.Context, account: str, database: str, schema: str, user: str, password: str, warehouse: str, role: str
) -> None:
    """
    Packets helpers. Offers a variety of subcommands for ingesting different types of support packets.
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


@packets.command()
@click.argument('input', type=click.Path(exists=True, dir_okay=False, readable=True, resolve_path=True))
@click.pass_context
def user_survey(
    ctx: click.Context,
    input: click.Path,
) -> None:
    """
    Ingest a user survey packet.
    :param input: The zip file with the user survey packet data.
    """
    with ctx.obj['engine'].begin() as conn:
        ingest_survey_packet(conn, ctx.parent.params['schema'], input, click.format_filename(input))


@packets.command()
@click.argument('input', type=click.Path(exists=True, dir_okay=False, readable=True, resolve_path=True))
def support_v1(
    input: click.Path,
) -> None:
    """
    Ingest a support packet using the original support package specification.

    :param input: The zip file with the support package.
    """
    ingest_support_packet(input)
