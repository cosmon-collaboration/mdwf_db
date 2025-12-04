"""Residual mass script implemented via BaseCommand."""

from ..command import BaseCommand
from ...exceptions import ValidationError


class MresCommand(BaseCommand):
    name = "mres-script"
    help = "Generate WIT mres measurement script"
    job_type = "mres"
    input_type = "wit_input"
    default_variant = "default"

    def custom_validation(self, input_params, job_params, ensemble):
        if input_params.get("Configurations.first") is None:
            raise ValidationError("Configurations.first is required")
        if input_params.get("Configurations.last") is None:
            raise ValidationError("Configurations.last is required")


def register(subparsers):
    MresCommand().register(subparsers)
