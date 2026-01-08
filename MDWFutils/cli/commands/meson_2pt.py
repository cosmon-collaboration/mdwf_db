"""Meson 2pt script command built on BaseCommand."""

from ..command import BaseCommand
from ...jobs.meson2pt import Meson2ptContextBuilder
from ...jobs.wit import WitContextBuilder


class Meson2ptCommand(BaseCommand):
    name = "meson2pt-script"
    aliases = ["meson2pt"]
    help = "Generate WIT meson 2pt SLURM script"
    job_builder_class = Meson2ptContextBuilder
    input_builder_class = WitContextBuilder
    default_variant = "default"


def register(subparsers):
    Meson2ptCommand().register(subparsers)
