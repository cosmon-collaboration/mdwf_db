"""Generate GLU input files via BaseCommand."""

from ..command import BaseCommand
from ..param_schemas import SMEAR_INPUT_SCHEMA


class GluInputCommand(BaseCommand):
    name = "glu-input"
    help = "Generate GLU input file"
    job_type = None
    input_type = "glu_input"
    input_schema = SMEAR_INPUT_SCHEMA
    job_schema = []


def register(subparsers):
    GluInputCommand().register(subparsers)
