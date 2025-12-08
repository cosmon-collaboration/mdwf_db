"""Meson 2pt script command built on BaseCommand."""

from ..command import BaseCommand
from ...exceptions import ValidationError


class Meson2ptCommand(BaseCommand):
    name = "meson2pt-script"
    aliases = ["meson2pt"]
    help = "Generate WIT meson 2pt SLURM script"
    job_type = "meson2pt"
    input_type = "wit_input"
    default_variant = "default"


def register(subparsers):
    Meson2ptCommand().register(subparsers)
