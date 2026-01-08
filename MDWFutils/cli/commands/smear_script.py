"""Smear script command implemented via BaseCommand."""

from ..command import BaseCommand
from ...exceptions import ValidationError
from ...jobs.smear import SmearContextBuilder
from ...jobs.glu import GluContextBuilder


class SmearCommand(BaseCommand):
    name = "smear-script"
    aliases = ["smear"]
    help = "Generate GLU smearing SLURM script"
    job_builder_class = SmearContextBuilder
    input_builder_class = GluContextBuilder
    default_variant = "stout8"

    def custom_validation(self, input_params, job_params, ensemble):
        start = job_params.get("config_start")
        end = job_params.get("config_end")
        if start is not None and end is not None and end < start:
            raise ValidationError("config_end must be >= config_start")
        if input_params.get("SMITERS", 0) < 1:
            raise ValidationError("SMITERS must be >= 1")


def register(subparsers):
    SmearCommand().register(subparsers)
