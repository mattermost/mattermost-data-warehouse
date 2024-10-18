import logging

import click

from utils.cli.context import snowflake_engine_context
from utils.cli.logging import initialize_cli_logging
from utils.packets.service import (
    ingest_support_packet,
    ingest_support_packets_from_s3,
    ingest_survey_packet,
    ingest_surveys_from_s3,
)

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
@click.argument('bucket', type=str)
@click.argument('prefix', type=str)
@click.pass_context
def user_survey_s3(ctx: click.Context, bucket: str, prefix: str) -> None:
    """
    Ingest user survey packets from S3.
    :param bucket: The S3 bucket to ingest from/
    :param prefix: The prefix to search for support packages.
    """
    with ctx.obj['engine'].begin() as conn:
        ingest_surveys_from_s3(conn, ctx.parent.params['schema'], bucket, prefix)


@packets.command()
@click.argument('input', type=click.Path(exists=True, dir_okay=False, readable=True, resolve_path=True))
@click.pass_context
def support_v1(
    ctx: click.Context,
    input: click.Path,
) -> None:
    """
    Ingest a support packet using the original support package specification.

    :param input: The zip file with the support package.
    """
    with ctx.obj['engine'].begin() as conn:
        ingest_support_packet(conn, ctx.parent.params['schema'], input, click.format_filename(input))


@packets.command()
@click.argument('bucket', type=str)
@click.argument('prefix', type=str)
@click.pass_context
def support_v1_s3(ctx: click.Context, bucket: str, prefix: str) -> None:
    """
    Ingest support packets from S3.
    :param bucket: The S3 bucket to ingest from/
    :param prefix: The prefix to search for support packages.
    """
    with ctx.obj['engine'].begin() as conn:
        ingest_support_packets_from_s3(conn, ctx.parent.params['schema'], bucket, prefix)
