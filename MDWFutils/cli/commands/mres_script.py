"""Residual mass script implemented via BaseCommand."""

from ..command import BaseCommand
from ...exceptions import ValidationError


class MresCommand(BaseCommand):
    name = "mres-script"
    aliases = ["mres"]
    help = "Generate WIT mres measurement script"
    job_type = "mres"
    input_type = "wit_input"
    default_variant = "default"


def register(subparsers):
    MresCommand().register(subparsers)
