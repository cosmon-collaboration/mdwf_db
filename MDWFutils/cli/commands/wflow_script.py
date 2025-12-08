"""Wilson flow script command implemented via BaseCommand."""

from ..command import BaseCommand
from ...exceptions import ValidationError


class WFlowCommand(BaseCommand):
    name = "wflow-script"
    aliases = ["wflow"]
    help = "Generate gradient flow SLURM script"
    job_type = "wflow"
    input_type = "glu_input"
    default_variant = "default"

    def custom_validation(self, input_params, job_params, ensemble):
        if job_params.get("config_start") is None or job_params.get("config_end") is None:
            raise ValidationError("config_start/config_end are required for wflow jobs")


def register(subparsers):
    WFlowCommand().register(subparsers)
