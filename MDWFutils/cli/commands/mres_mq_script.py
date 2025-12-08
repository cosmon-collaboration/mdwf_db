"""mres-mq script command built on BaseCommand."""

from ..command import BaseCommand
from ...exceptions import ValidationError


class MresMQCommand(BaseCommand):
    name = "mres-mq-script"
    aliases = ["mres-mq"]
    help = "Generate WIT mres single-mass script"
    job_type = "mres_mq"
    input_type = "wit_input"
    default_variant = "charm"


def register(subparsers):
    MresMQCommand().register(subparsers)
