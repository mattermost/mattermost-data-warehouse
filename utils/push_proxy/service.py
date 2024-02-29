from datetime import datetime
from logging import getLogger

from sqlalchemy.engine import Connection

from utils.db.helpers import copy_from_stage, max_column_value
from utils.helpers import daterange

logger = getLogger(__name__)


def load_stage_to_table(
    conn: Connection,
    stage_schema: str,
    stage_name: str,
    prefix: str,
    target_schema: str,
    target_table: str,
    timestamp_column: str = "time",
):
    """
    Load data from given stage to target table. Checks if a full load or incremental load is required.
    """

    latest_date = max_column_value(conn, target_schema, target_table, timestamp_column)

    if latest_date:
        logger.info(f"Last loaded date: {latest_date}")
        # Load data for all dates up to yesterday.
        for partition_date in daterange(latest_date, datetime.utcnow().date()):
            partition_prefix = join_s3_path(prefix, partition_date.strftime("%Y/%m/%d"))
            logger.info(f"Loading data from {stage_name}{partition_prefix} to {target_table}")
            copy_from_stage(conn, stage_schema, stage_name, partition_prefix, target_schema, target_table)
    else:
        # Load everything
        logger.info(f"Loading data from {stage_name}{prefix} to {target_table}")
        normalized_prefix = join_s3_path(prefix)
        copy_from_stage(conn, stage_schema, stage_name, normalized_prefix, target_schema, target_table)


def join_s3_path(prefix: str, *parts):
    """
    Join key prefix with all remaining parts. Removes trailing and leading `/` characters, as well as ensures
     no duplicate `/` exists.
    """
    prefix_parts = [p for p in prefix.split("/") if p] if prefix else []
    # Convert to strings
    normalized_parts = [str(p) for p in parts]
    # Check for / in part
    normalized_parts = [flat for p in normalized_parts for flat in p.split("/") if flat]
    return "/".join([*prefix_parts, *normalized_parts])
