"""Generate HMC XML input via BaseCommand."""

from ..command import BaseCommand
from ...jobs.hmc import HMCXMLContextBuilder


class HMCXMLCommand(BaseCommand):
    name = "hmc-xml"
    help = "Generate HMC XML input file"
    input_builder_class = HMCXMLContextBuilder


def register(subparsers):
    HMCXMLCommand().register(subparsers)
