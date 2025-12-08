"""Generate WIT input files via BaseCommand."""

from ..command import BaseCommand
from ...exceptions import ValidationError


class WitInputCommand(BaseCommand):
    name = "wit-input"
    aliases = ["wit"]
    help = "Generate WIT input file"
    job_type = None
    input_type = "wit_input"
    job_schema = []

    def custom_validation(self, input_params, job_params, ensemble):
        if input_params.get("Configurations.first") is None:
            raise ValidationError("Configurations.first is required")
        if input_params.get("Configurations.last") is None:
            raise ValidationError("Configurations.last is required")


def register(subparsers):
    WitInputCommand().register(subparsers)
