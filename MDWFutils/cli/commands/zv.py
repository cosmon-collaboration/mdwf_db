"""Zv script command built on BaseCommand."""

from ..command import BaseCommand
from ..param_schemas import WIT_INPUT_SCHEMA
from ...exceptions import ValidationError


class ZvCommand(BaseCommand):
    name = "zv-script"
    help = "Generate Zv measurement script"
    job_type = "zv"
    input_type = "wit_input"
    input_schema = WIT_INPUT_SCHEMA
    default_variant = "default"

    def custom_validation(self, input_params, job_params, ensemble):
        if input_params.get("Configurations.first") is None:
            raise ValidationError("Configurations.first is required")
        if input_params.get("Configurations.last") is None:
            raise ValidationError("Configurations.last is required")


def register(subparsers):
    ZvCommand().register(subparsers)
