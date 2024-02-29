import logging
import sys
from datetime import timedelta


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def initialize_cli_logging(log_level: int, logging_stream: str):
    """
    Configure logging for CLI commands.
    """
    stream = getattr(sys, logging_stream)
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(message)s"))
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(log_level)

    # Disable verbose loggers
    logging.getLogger('snowflake').setLevel(logging.WARNING)
