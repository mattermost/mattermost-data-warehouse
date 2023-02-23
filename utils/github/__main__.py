import tempfile

import click

from utils.db.helpers import snowflake_engine, upload_csv_as_table
from utils.github.service import GithubService


@click.command()
@click.argument('organization')
@click.argument('table')
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
@click.option('--token', envvar='GITHUB_TOKEN', help='the github token to use', required=True)
def contributors(
    organization: str,
    table: str,
    account: str,
    database: str,
    schema: str,
    user: str,
    password: str,
    warehouse: str,
    role: str,
    token: str,
) -> None:
    """
    Store github contributors to target table.
    \f

    :param organization: the name of the organization to get contributors from.
    :param table: the name of the table to store data to.
    :param account: the name of the snowflake account.
    :param database: the database containing the schema.
    :param schema: the schema containing the events.
    :param user: the name of the user for connecting to snowflake.
    :param password: the password for connecting to snowflake.
    :param warehouse: the warehouse to use when connecting to snowflake.
    :param role: the role to use when connecting to snowflake.
    :param token: the github token to use for authenticating to github's API.
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
    with tempfile.NamedTemporaryFile(mode='w') as fp:
        api = GithubService(token=token)
        click.echo('Downloading pull requests...')
        api.download_pulls(
            organization, fp, state='closed', pr_filter=lambda pr: pr['merged_at'] is not None, per_page=100
        )
        fp.flush()
        click.echo('Uploading to snowflake...')
        upload_csv_as_table(engine, fp.name, schema, table)


if __name__ == '__main__':
    contributors()
