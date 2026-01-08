"""Residual mass script implemented via BaseCommand."""

from ..command import BaseCommand
from ...jobs.mres import MresContextBuilder
from ...jobs.wit import WitContextBuilder


class MresCommand(BaseCommand):
    name = "mres-script"
    aliases = ["mres"]
    help = "Generate WIT mres measurement script"
    job_builder_class = MresContextBuilder
    input_builder_class = WitContextBuilder
    default_variant = "default"


def register(subparsers):
    MresCommand().register(subparsers)
