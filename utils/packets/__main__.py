import logging

import click

from utils.helpers import initialize_cli_logging
from utils.packets.service import ingest_survey_packet

initialize_cli_logging(logging.INFO, 'stderr')


@click.group()
def packets() -> None:
    """
    Packets helpers. Offers a variety of subcommands for ingesting different types of support packets.
    """
    pass


@packets.command()
@click.argument('input', type=click.Path(exists=True, dir_okay=False, readable=True, resolve_path=True))
def user_survey(
    input: click.Path,
) -> None:
    """
    Ingest a user survey packet.

    :param input: The zip file with the user survey packet data.
    """
    ingest_survey_packet(input)
