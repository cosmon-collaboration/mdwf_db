"""Wilson flow script command implemented via BaseCommand."""

from ..command import BaseCommand
from ...jobs.wflow import WflowContextBuilder
from ...jobs.glu import GluContextBuilder


class WFlowCommand(BaseCommand):
    name = "wflow-script"
    aliases = ["wflow"]
    help = "Generate gradient flow SLURM script"
    job_builder_class = WflowContextBuilder
    input_builder_class = GluContextBuilder
    default_variant = "default"


def register(subparsers):
    WFlowCommand().register(subparsers)
