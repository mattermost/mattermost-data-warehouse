import tempfile
from pathlib import Path

import click

from utils.db.helpers import snowflake_engine, upload_csv_as_table
from utils.geolite.service import FILENAME_TO_TABLE, Edition, MaxMindClient


@click.command()
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
@click.option('--geo-account-id', envvar='GEO_ACCOUNT_ID', help='the account ID for MaxMind API calls')
@click.option('--geo-license-id', envvar='GEO_LICENSE_ID', help='the licese ID for MaxMind API calls')
def geolite(
    account: str,
    database: str,
    schema: str,
    user: str,
    password: str,
    warehouse: str,
    role: str,
    geo_account_id: str,
    geo_license_id: str,
) -> None:
    """
    Download GeoLite2 dataset from MaxMind and refresh tables in snowflake.
    \f

    :param account: the name of the snowflake account.
    :param database: the database containing the schema.
    :param schema: the schema containing the events.
    :param user: the name of the user for connecting to snowflake.
    :param password: the password for connecting to snowflake.
    :param warehouse: the warehouse to use when connecting to snowflake.
    :param role: the role to use when connecting to snowflake.
    :param geo_account_id: the account id for MaxMind API calls.
    :param geo_license_id: the license id for MaxMind API calls.
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
    client = MaxMindClient(geo_account_id, geo_license_id)
    for edition in Edition:
        with tempfile.TemporaryDirectory() as target_dir:
            click.echo(f"Downloading {edition}")
            files = client.download(edition, target_dir)
            for fullpath in files:
                filename = Path(fullpath).name
                click.echo(f'Uploading {filename} to snowflake...')
                upload_csv_as_table(engine, fullpath, schema, FILENAME_TO_TABLE[filename])


if __name__ == '__main__':
    geolite()
