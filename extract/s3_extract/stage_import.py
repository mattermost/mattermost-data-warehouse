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

    extract_from_stage(loc['table'], loc['stage'], 'push_proxy', get_path(aws_account_id, az), import_date, os.environ)


def get_path(aws_account_id, az):
    return f'AWSLogs/{aws_account_id}/elasticloadbalancing/{az}'