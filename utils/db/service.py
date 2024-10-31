import logging
from typing import List

from sqlalchemy.engine import Connection

from utils.db.helpers import load_query
from utils.helpers import post_df_to_mattermost

logger = logging.getLogger(__name__)


def post_query_results(
    conn: Connection, query: str, headers: List[str], url: str, channel: str, empty_data_message: str
):
    """
    Post query results to Mattermost.
    """
    logger.info('Querying data...')
    df = load_query(conn, query)
    logger.info('Sending dataframe to mattermost...')
    post_df_to_mattermost(url, channel, df, headers, empty_data_message)
