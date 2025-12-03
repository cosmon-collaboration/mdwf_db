"""mres-mq script command built on BaseCommand."""

from ..command import BaseCommand
from ..param_schemas import WIT_INPUT_SCHEMA
from ...exceptions import ValidationError


class MresMQCommand(BaseCommand):
    name = "mres-mq-script"
    help = "Generate WIT mres single-mass script"
    job_type = "mres_mq"
    input_type = "wit_input"
    input_schema = WIT_INPUT_SCHEMA
    default_variant = "charm"

    def custom_validation(self, input_params, job_params, ensemble):
        if input_params.get("Configurations.first") is None:
            raise ValidationError("Configurations.first is required")
        if input_params.get("Configurations.last") is None:
            raise ValidationError("Configurations.last is required")


def register(subparsers):
    MresMQCommand().register(subparsers)
