from airflow.contrib.kubernetes.secret import Secret

# AWS
AWS_ACCOUNT_ID = Secret("env", "AWS_ACCOUNT_ID", "airflow", "AWS_ACCOUNT_ID")
AWS_ACCESS_KEY_ID = Secret("env", "AWS_ACCESS_KEY_ID", "airflow", "AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Secret(
    "env", "AWS_SECRET_ACCESS_KEY", "airflow", "AWS_SECRET_ACCESS_KEY"
)


# blapi
BLAPI_DATABASE_URL = Secret(
    "env", "BLAPI_DATABASE_URL", "airflow", "BLAPI_DATABASE_URL"
)
BLAPI_TOKEN = Secret("env", "BLAPI_TOKEN", "airflow", "BLAPI_TOKEN")
BLAPI_URL = Secret("env", "BLAPI_URL", "airflow", "BLAPI_URL")

# dbt Cloud
DBT_CLOUD_API_KEY = Secret("env", "DBT_CLOUD_API_KEY", "airflow", "DBT_CLOUD_API_KEY")
DBT_CLOUD_API_ACCOUNT_ID = Secret(
    "env", "DBT_CLOUD_API_ACCOUNT_ID", "airflow", "DBT_CLOUD_API_ACCOUNT_ID"
)

# Snowflake Load
SNOWFLAKE_LOAD_DATABASE = Secret(
    "env", "SNOWFLAKE_LOAD_DATABASE", "airflow", "SNOWFLAKE_LOAD_DATABASE"
)
SNOWFLAKE_LOAD_ROLE = Secret(
    "env", "SNOWFLAKE_LOAD_ROLE", "airflow", "SNOWFLAKE_LOAD_ROLE"
)
SNOWFLAKE_LOAD_PASSWORD = Secret(
    "env", "SNOWFLAKE_LOAD_PASSWORD", "airflow", "SNOWFLAKE_LOAD_PASSWORD"
)
SNOWFLAKE_LOAD_USER = Secret(
    "env", "SNOWFLAKE_LOAD_USER", "airflow", "SNOWFLAKE_LOAD_USER"
)
SNOWFLAKE_LOAD_WAREHOUSE = Secret(
    "env", "SNOWFLAKE_LOAD_WAREHOUSE", "airflow", "SNOWFLAKE_LOAD_WAREHOUSE"
)

# Snowflake Transform
SNOWFLAKE_TRANSFORM_ROLE = Secret(
    "env", "SNOWFLAKE_TRANSFORM_ROLE", "airflow", "SNOWFLAKE_TRANSFORM_ROLE"
)
SNOWFLAKE_TRANSFORM_SCHEMA = Secret(
    "env", "SNOWFLAKE_TRANSFORM_SCHEMA", "airflow", "SNOWFLAKE_TRANSFORM_SCHEMA"
)
SNOWFLAKE_TRANSFORM_USER = Secret(
    "env", "SNOWFLAKE_TRANSFORM_USER", "airflow", "SNOWFLAKE_TRANSFORM_USER"
)
SNOWFLAKE_TRANSFORM_WAREHOUSE = Secret(
    "env", "SNOWFLAKE_TRANSFORM_WAREHOUSE", "airflow", "SNOWFLAKE_TRANSFORM_WAREHOUSE"
)
SNOWFLAKE_TRANSFORM_DATABASE = Secret(
    "env", "SNOWFLAKE_TRANSFORM_DATABASE", "airflow", "SNOWFLAKE_TRANSFORM_DATABASE"
)
SNOWFLAKE_USER = Secret("env", "SNOWFLAKE_USER", "airflow", "SNOWFLAKE_USER")
SNOWFLAKE_ACCOUNT = Secret("env", "SNOWFLAKE_ACCOUNT", "airflow", "SNOWFLAKE_ACCOUNT")
SNOWFLAKE_PASSWORD = Secret(
    "env", "SNOWFLAKE_PASSWORD", "airflow", "SNOWFLAKE_PASSWORD"
)

# MM
MATTERMOST_WEBHOOK_URL = Secret(
    "env", "MATTERMOST_WEBHOOK_URL", "airflow", "MATTERMOST_WEBHOOK_URL"
)

# Permission Bot
PERMISSION_BOT_USER = Secret(
    "env", "PERMISSION_BOT_USER", "airflow", "SNOWFLAKE_PERMISSION_USER"
)
PERMISSION_BOT_PASSWORD = Secret(
    "env", "PERMISSION_BOT_PASSWORD", "airflow", "SNOWFLAKE_PERMISSION_PASSWORD"
)
PERMISSION_BOT_ACCOUNT = Secret(
    "env", "PERMISSION_BOT_ACCOUNT", "airflow", "SNOWFLAKE_ACCOUNT"
)
PERMISSION_BOT_DATABASE = Secret(
    "env", "PERMISSION_BOT_DATABASE", "airflow", "SNOWFLAKE_PERMISSION_DATABASE"
)
PERMISSION_BOT_ROLE = Secret(
    "env", "PERMISSION_BOT_ROLE", "airflow", "SNOWFLAKE_PERMISSION_ROLE"
)
PERMISSION_BOT_WAREHOUSE = Secret(
    "env", "PERMISSION_BOT_WAREHOUSE", "airflow", "SNOWFLAKE_PERMISSION_WAREHOUSE"
)


# Diagnostic Locations
DIAGNOSTIC_LOCATION_ONE = Secret(
    "env", "DIAGNOSTIC_LOCATION_ONE", "airflow", "DIAGNOSTIC_LOCATION_ONE"
)
DIAGNOSTIC_LOCATION_TWO = Secret(
    "env", "DIAGNOSTIC_LOCATION_TWO", "airflow", "DIAGNOSTIC_LOCATION_TWO"
)
RELEASE_LOCATION = Secret("env", "RELEASE_LOCATION", "airflow", "RELEASE_LOCATION")

# Heroku
HEROKU_POSTGRESQL_URL = Secret(
    "env", "HEROKU_POSTGRESQL_URL", "airflow", "HEROKU_POSTGRESQL_URL"
)
PG_IMPORT_BUCKET = Secret("env", "PG_IMPORT_BUCKET", "airflow", "PG_IMPORT_BUCKET")

# SSH Key
SSH_KEY = Secret("env", "SSH_KEY", "airflow", "SSH_KEY")

# Twitter Keys
TWITTER_CONSUMER_KEY = Secret(
    "env", "TWITTER_CONSUMER_KEY", "airflow", "TWITTER_CONSUMER_KEY"
)
TWITTER_CONSUMER_SECRET = Secret(
    "env", "TWITTER_CONSUMER_SECRET", "airflow", "TWITTER_CONSUMER_SECRET"
)
TWITTER_ACCESS_KEY = Secret(
    "env", "TWITTER_ACCESS_KEY", "airflow", "TWITTER_ACCESS_KEY"
)
TWITTER_ACCESS_SECRET = Secret(
    "env", "TWITTER_ACCESS_SECRET", "airflow", "TWITTER_ACCESS_SECRET"
)

# Pipelinewise secrets
PIPELINEWISE_SECRETS = Secret("volume", "/app/wrk", "pipelinewise-secrets")

CLEARBIT_KEY = Secret("env", "CLEARBIT_KEY", "airflow", "CLEARBIT_KEY")

GITHUB_TOKEN = Secret("env", "GITHUB_TOKEN", "airflow", "GITHUB_TOKEN")
