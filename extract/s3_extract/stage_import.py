import os

from extract.utils import execute_query, snowflake_engine_factory


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
    'US': {'table': 'logs', 'stage': 'push_proxy_stage', 'az': 'us-east-1'},
    'TEST': {'table': 'test_logs', 'stage': 'push_proxy_test_stage', 'az': 'us-east-1'},
    'DE': {'table': 'de_logs', 'stage': 'push_proxy_de_stage', 'az': 'eu-central-1'},
}

DIAGNOSTICS_LOCATIONS = ['DIAGNOSTIC_LOCATION_ONE', 'DIAGNOSTIC_LOCATION_TWO']


def licenses_import(import_date):
    extract_from_stage('licenses', 'licenses_stage', 'licenses', import_date, f".*{import_date}.csv", os.environ.copy())


def releases_import(import_date):
    loc = os.getenv('RELEASE_LOCATION')
    # Releases and diagnostics S3 folders have the same format so we re-use the pattern
    extract_from_stage(
        'log_entries',
        'releases_stage',
        'releases',
        f'releases.mattermost.com-cloudfront/{loc}',
        get_diagnostics_pattern(loc, import_date),
        os.environ.copy(),
    )


def diagnostics_import(import_date):
    for env_loc in DIAGNOSTICS_LOCATIONS:
        loc = os.getenv(env_loc)
        extract_from_stage(
            'log_entries',
            'diagnostics_stage',
            'diagnostics',
            loc,
            get_diagnostics_pattern(loc, import_date),
            os.environ.copy(),
        )


def get_diagnostics_pattern(loc, import_date):
    return f".*{loc}.{import_date}.*"


def get_path(aws_account_id, az):
    return f'AWSLogs/{aws_account_id}/elasticloadbalancing/{az}'
