"""Meson 2pt script command built on BaseCommand."""

from ..command import BaseCommand
from ..param_schemas import COMMON_JOB_SCHEMA, WIT_INPUT_SCHEMA
from ...exceptions import ValidationError


class Meson2ptCommand(BaseCommand):
    name = "meson2pt-script"
    help = "Generate WIT meson 2pt SLURM script"
    job_type = "meson2pt"
    input_type = "wit_input"
    input_schema = WIT_INPUT_SCHEMA
    job_schema = COMMON_JOB_SCHEMA
    default_variant = "default"

    def custom_validation(self, input_params, job_params, ensemble):
        if input_params.get("Configurations.first") is None:
            raise ValidationError("Configurations.first is required")
        if input_params.get("Configurations.last") is None:
            raise ValidationError("Configurations.last is required")


def register(subparsers):
    Meson2ptCommand().register(subparsers)
