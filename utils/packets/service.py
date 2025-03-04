import os
from datetime import datetime, timedelta
from enum import Enum
from logging import getLogger
from typing import Callable

import pandas as pd
from click import ClickException
from sqlalchemy.engine import Connection
from sqlalchemy.exc import OperationalError

from utils.db.helpers import upsert_dataframe_to_table
from utils.helpers import daterange
from utils.packets.aws import bucket_exists, object_iter
from utils.packets.loaders import UserSurveyFixedColumns, load_support_packet_file, load_user_survey_package

DATE_FORMAT = '%Y-%m-%d'

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
        enriched_df['ingestion_date'] = datetime.today().strftime(DATE_FORMAT)

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
        sp_df['ingestion_date'] = datetime.today().strftime(DATE_FORMAT)

        job_df = pd.DataFrame([job.model_dump() for job in sp.all_jobs()])
        job_df['data'] = job_df['data'].astype(str)
        job_df['metadata_server_id'] = server_id
        job_df['source'] = source_uri
        sp_df['ingestion_date'] = datetime.today().strftime(DATE_FORMAT)

        # Upsert data
        upsert_dataframe_to_table(conn, target_schema, TableNames.SUPPORT_PACKET_V1.value, sp_df, server_id, source_uri)
        upsert_dataframe_to_table(
            conn, target_schema, TableNames.SUPPORT_PACKET_JOBS_V1.value, job_df, server_id, source_uri
        )

    except ValueError as e:
        raise ClickException(f'Error loading support package: {e}')


def _ingest_packet_from_s3(
    conn: Connection, target_schema: str, bucket: str, prefix: str, ingest_func: Callable, table: TableNames
):
    if not bucket_exists(bucket):
        raise ValueError(f'Bucket {bucket} does not exist')

    try:
        checkpoint = conn.execute(
            f'SELECT MAX(ingestion_date) as checkpoint FROM \'{target_schema}\'.{table.value}'
        ).scalar()
    except OperationalError:
        # Table does not exist
        checkpoint = None
    if checkpoint:
        logger.info(f'Resuming ingestion from: {checkpoint}')
        start_date = datetime.strptime(checkpoint, DATE_FORMAT)
        for date in daterange(start_date, datetime.today() + timedelta(days=1)):
            for key, content in object_iter(bucket, f'{prefix}/{date.strftime("%Y/%m/%d")}'):
                logger.info(f'Ingesting {key}')
                ingest_func(conn, target_schema, content, f's3://{bucket}/{key}')
    else:
        logger.info('Starting ingestion from the beginning')
        for key, content in object_iter(bucket, f'{prefix}/'):
            logger.info(f'Ingesting {key}')
            ingest_func(conn, target_schema, content, f's3://{bucket}/{key}')


def ingest_surveys_from_s3(conn: Connection, target_schema: str, bucket: str, prefix: str):
    """
    Load all survey packages from S3

    :conn: The SQLAlchemy connection to use for ingesting the data.
    :target_schema: The schema to ingest the data into.
    :bucket: The S3 bucket name.
    :prefix: The S3 bucket prefix where the survey packets are located at.
    """
    _ingest_packet_from_s3(conn, target_schema, bucket, prefix, ingest_survey_packet, TableNames.USER_SURVEY)


def ingest_support_packets_from_s3(conn: Connection, target_schema: str, bucket: str, prefix: str):
    """
    Load all support packets from S3

    :conn: The SQLAlchemy connection to use for ingesting the data.
    :target_schema: The schema to ingest the data into.
    :bucket: The S3 bucket name.
    :prefix: The S3 bucket prefix where the survey packets are located at.
    """
    _ingest_packet_from_s3(conn, target_schema, bucket, prefix, ingest_support_packet, TableNames.SUPPORT_PACKET_V1)
