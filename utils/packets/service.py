import os
from logging import getLogger

from click import ClickException

from utils.packets.loaders import UserSurveyFixedColumns, load_support_packet_file, load_user_survey_package

logger = getLogger(__name__)


def ingest_survey_packet(survey_packet: str | os.PathLike):
    """
    Load user survey data and metadata.

    :survey_packet: The path to the survey packet file.
    """
    try:
        metadata, df = load_user_survey_package(survey_packet)
        logger.info('Loaded survey packet')
        logger.info(f' -> Server ID: {metadata.server_id}')
        logger.info(f' -> License ID: {metadata.license_id}')
        logger.info('')
        logger.info(f' Total {df[UserSurveyFixedColumns.user_id.value].nunique()} unique users')

        # TODO: ingest in database
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
