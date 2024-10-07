import os
from enum import Enum
from logging import getLogger

import pandas as pd
from click import ClickException
from sqlalchemy.engine import Connection

from utils.db.helpers import upsert_dataframe_to_table
from utils.packets.loaders import UserSurveyFixedColumns, load_support_packet_file, load_user_survey_package

logger = getLogger(__name__)


class TableNames(str, Enum):
    USER_SURVEY = 'user_survey'
    SUPPORT_PACKET_V1 = 'support_packet_v1'
    SUPPORT_PACKET_JOBS_V1 = 'support_packet_jobs_v1'


def ingest_survey_packet(conn: Connection, target_schema: str, survey_packet: str | os.PathLike, source_uri: str):
    """
    Load user survey data and metadata.

    :conn: The SQLAlchemy connection to use for ingesting the data.
    :target_schema: The schema to ingest the data into.
    :survey_packet: The path to the survey packet file.
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

        # Upsert data
        upsert_dataframe_to_table(
            conn, target_schema, TableNames.USER_SURVEY.value, enriched_df, metadata.server_id, source_uri
        )

    except ValueError as e:
        raise ClickException(f'Error loading survey packet: {e}')


def ingest_support_packet(conn: Connection, target_schema: str, support_packet: str | os.PathLike, source_uri: str):
    """
    Load support package data and metadata.

    :conn: The SQLAlchemy connection to use for ingesting the data.
    :target_schema: The schema to ingest the data into.
    :support_package: The path to the survey packet file.
    :source_uri: The source URI of the survey packet. Used as a unique key.
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

        # Enrich data with metadata and ingest in database
        sp_df = pd.DataFrame(
            [
                sp.dict(
                    exclude={
                        'data_retention_jobs',
                        'bleve_post_indexing_jobs',
                        'message_export_jobs',
                        'elastic_post_indexing_jobs',
                        'elastic_post_aggregation_jobs',
                        'ldap_sync_jobs',
                        'migration_jobs',
                        'compliance_jobs',
                    }
                )
            ]
        )

        server_id = metadata.server_id if metadata else 'Unknown'
        sp_df['metadata_version'] = metadata.version if metadata else None
        sp_df['metadata_generated_at'] = metadata.generated_at if metadata else None
        sp_df['metadata_server_version'] = metadata.server_version if metadata else None
        sp_df['metadata_server_id'] = server_id
        sp_df['metadata_license_id'] = metadata.license_id if metadata else None
        sp_df['metadata_customer_id'] = metadata.customer_id if metadata else None
        sp_df['source'] = source_uri

        job_df = pd.DataFrame([job.model_dump() for job in sp.all_jobs()])
        job_df['data'] = job_df['data'].astype(str)
        job_df['metadata_server_id'] = server_id
        job_df['source'] = source_uri

        # Upsert data
        upsert_dataframe_to_table(conn, target_schema, TableNames.SUPPORT_PACKET_V1.value, sp_df, server_id, source_uri)
        upsert_dataframe_to_table(
            conn, target_schema, TableNames.SUPPORT_PACKET_JOBS_V1.value, job_df, server_id, source_uri
        )

    except ValueError as e:
        raise ClickException(f'Error loading support package: {e}')
