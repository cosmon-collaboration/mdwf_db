"""Residual mass script implemented via BaseCommand."""

from ..command import BaseCommand
from ..param_schemas import COMMON_JOB_SCHEMA, WIT_INPUT_SCHEMA
from ...exceptions import ValidationError


class MresCommand(BaseCommand):
    name = "mres-script"
    help = "Generate WIT mres measurement script"
    job_type = "mres"
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
    MresCommand().register(subparsers)
