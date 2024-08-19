import os
from logging import getLogger

from click import ClickException

from utils.packets.loaders import UserSurveyFixedColumns, load_user_survey_package

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
        logger.info('\n')
        logger.info(f' Total {df[UserSurveyFixedColumns.user_id.value].nunique()} unique users')

        # TODO: ingest in database
    except ValueError as e:
        raise ClickException(f'Error loading survey packet: {e}')
