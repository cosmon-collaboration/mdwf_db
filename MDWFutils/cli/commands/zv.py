"""Zv script command built on BaseCommand."""

from ..command import BaseCommand
from ...exceptions import ValidationError


class ZvCommand(BaseCommand):
    name = "zv-script"
    aliases = ["zv"]
    help = "Generate Zv measurement script"
    job_type = "zv"
    input_type = "wit_input"
    default_variant = "default"


def register(subparsers):
    ZvCommand().register(subparsers)
