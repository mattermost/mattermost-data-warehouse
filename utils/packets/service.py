import os
from enum import Enum
from logging import getLogger

from click import ClickException
from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.db.helpers import append_dataframe_to_table, table_exists
from utils.packets.loaders import UserSurveyFixedColumns, load_support_packet_file, load_user_survey_package

logger = getLogger(__name__)


class TableNames(str, Enum):
    USER_SURVEY = 'user_survey'
    SUPPORT_PACKET = 'support_packet'


def ingest_survey_packet(conn: Connection, target_schema: str, survey_packet: str | os.PathLike, source_uri: str):
    """
    Load user survey data and metadata.

    :conn: The SQLAlchemy connection to use for ingesting the data.
    :survey_packet: The path to the survey packet file.
    :target_schema: The schema to ingest the data into.
    :source_uri: The source URI of the survey packet. Used as a unique key.
    """
    try:
        metadata, df = load_user_survey_package(survey_packet)
        logger.info('Loaded survey packet')
        logger.info(f' -> Server ID: {metadata.server_id}')
        logger.info(f' -> License ID: {metadata.license_id}')
        logger.info('')
        logger.info(f' Total {df[UserSurveyFixedColumns.user_id.value].nunique()} unique users')

        # Enrich data with metadata and ingest in database
        enriched_df = df.copy()
        enriched_df.columns = enriched_df.columns.str.replace(' ', '_', regex=True).str.lower()

        enriched_df['metadata_version'] = metadata.version
        enriched_df['metadata_generated_at'] = metadata.generated_at
        enriched_df['metadata_server_version'] = metadata.server_version
        enriched_df['metadata_server_id'] = metadata.server_id
        enriched_df['metadata_license_id'] = metadata.license_id
        enriched_df['metadata_customer_id'] = metadata.customer_id
        enriched_df['metadata_extras_plugin_id'] = metadata.extras.plugin_id
        enriched_df['metadata_extras_plugin_version'] = metadata.extras.plugin_version
        enriched_df['source'] = source_uri

        # Delete survey if already existed
        if table_exists(conn, target_schema, TableNames.USER_SURVEY.value):
            conn.execute(
                text(
                    f"""
                    DELETE FROM {target_schema}.{TableNames.USER_SURVEY.value}
                    WHERE
                        metadata_server_id = :server_id
                        AND source = :source_uri
                """
                ),
                server_id=metadata.server_id,
                source_uri=source_uri,
            )

        append_dataframe_to_table(conn, target_schema, TableNames.USER_SURVEY.value, enriched_df)
    except ValueError as e:
        raise ClickException(f'Error loading survey packet: {e}')


def ingest_support_packet(support_packet: str | os.PathLike):
    """
    Load support package data and metadata.

    :support_package: The path to the survey packet file.
    """
    try:
        metadata, sp = load_support_packet_file(support_packet)
        logger.info('Loaded support packet')
        if metadata:
            logger.info(f' -> Server ID: {metadata.server_id}')
            logger.info(f' -> License ID: {metadata.license_id}')
        else:
            logger.info('Support packet does not include metadata file')
        logger.info('')
        logger.info(f' Server: {sp.server_version} ({sp.server_os} {sp.server_architecture})')
        logger.info(f' Database: {sp.database_type} {sp.database_version} (schema {sp.database_schema_version})')

        # TODO: ingest in database
    except ValueError as e:
        raise ClickException(f'Error loading support package: {e}')
