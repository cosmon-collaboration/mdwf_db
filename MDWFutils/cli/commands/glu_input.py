"""Generate GLU input files via BaseCommand."""

from ..command import BaseCommand
from ...jobs.glu import GluContextBuilder


class GluInputCommand(BaseCommand):
    name = "glu-input"
    aliases = ["glu"]
    help = "Generate GLU input file"
    input_builder_class = GluContextBuilder


def register(subparsers):
    GluInputCommand().register(subparsers)
