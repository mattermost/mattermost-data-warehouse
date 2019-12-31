from datetime import datetime
import os

from extract.utils import snowflake_engine_factory, execute_query


def extract_from_stage(import_table, stage, schema, path, pattern, env):
    engine = snowflake_engine_factory(env, 'LOADER', schema)

    query = f"""
        COPY INTO raw.{schema}.{import_table}
        FROM @{stage}/{path}
        PATTERN = '{pattern}'
        ON_ERROR = 'CONTINUE';
    """
    execute_query(engine, query)


PUSH_PROXY_LOCATIONS = {
    'US': {
        'table': 'logs',
        'stage': 'push_proxy_stage',
        'az': 'us-east-1'
    },
    'TEST': {
        'table': 'test_logs',
        'stage': 'push_proxy_test_stage',
        'az': 'us-east-1'
    },
    'DE': {
        'table': 'de_logs',
        'stage': 'push_proxy_de_stage',
        'az': 'eu-central-1'
    }
}

DIAGNOSTICS_LOCATIONS = [
    'DIAGNOSTIC_LOCATION_ONE', 'DIAGNOSTIC_LOCATION_TWO'
]


def releases_import(import_date):
    loc = os.getenv('RELEASE_LOCATION')
    # Releases and diagnostics S3 folders have the same format so we re-use the pattern
    extract_from_stage('log_entries', 'releases_stage', 'releases', loc, get_diagnostics_pattern(loc, import_date), os.environ.copy())


def diagnostics_import(import_date):
    for env_loc in DIAGNOSTICS_LOCATIONS:
        loc = os.getenv(env_loc)
        extract_from_stage('log_entries', 'diagnostics_stage', 'diagnostics', loc, get_diagnostics_pattern(loc, import_date), os.environ.copy())


def push_proxy_import(log_type, import_date):
    """
    Function to load data from a previously set up Snowflake stage
    for push proxy data from AWS ELB

    @log_type: Str with valid values of US, TEST, or DE
    @import_date: Date with format "%Y/%m/%d"
    """
    loc = PUSH_PROXY_LOCATIONS[log_type]
    aws_account_id = os.getenv('AWS_ACCOUNT_ID')
    az = loc['az']

    extract_from_stage(loc['table'], loc['stage'], 'push_proxy', get_path(aws_account_id, az), get_push_proxy_pattern(import_date), os.environ.copy())


def get_push_proxy_pattern(import_date):
    date = import_date.replace('/', '\\/')
    return f".*{date}\\/.*"


def get_diagnostics_pattern(loc, import_date):
    return f".*{loc}.{import_date}.*"


def get_path(aws_account_id, az):
    return f'AWSLogs/{aws_account_id}/elasticloadbalancing/{az}'