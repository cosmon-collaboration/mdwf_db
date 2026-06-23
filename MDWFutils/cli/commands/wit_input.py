"""Generate WIT input files via BaseCommand."""

from ..command import BaseCommand
from ...jobs.wit import WitContextBuilder


class WitInputCommand(BaseCommand):
    name = "wit-input"
    aliases = ["wit"]
    help = "Generate WIT input file"
    input_builder_class = WitContextBuilder


def register(subparsers):
    WitInputCommand().register(subparsers)
