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


def register(subparsers):
    WFlowCommand().register(subparsers)
