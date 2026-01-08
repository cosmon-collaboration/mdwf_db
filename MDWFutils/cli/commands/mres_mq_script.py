"""mres-mq script command built on BaseCommand."""

from ..command import BaseCommand
from ...jobs.mres_mq import MresMQContextBuilder
from ...jobs.wit import WitContextBuilder


class MresMQCommand(BaseCommand):
    name = "mres-mq-script"
    aliases = ["mres-mq"]
    help = "Generate WIT mres single-mass script"
    job_builder_class = MresMQContextBuilder
    input_builder_class = WitContextBuilder
    default_variant = "charm"


def register(subparsers):
    MresMQCommand().register(subparsers)
