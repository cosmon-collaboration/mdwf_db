"""Generate GLU input files via BaseCommand."""

from ..command import BaseCommand


class GluInputCommand(BaseCommand):
    name = "glu-input"
    aliases = ["glu"]
    help = "Generate GLU input file"
    job_type = None
    input_type = "glu_input"
    job_schema = []


def register(subparsers):
    GluInputCommand().register(subparsers)
