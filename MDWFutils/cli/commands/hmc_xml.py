"""Generate HMC XML input via BaseCommand."""

from ..command import BaseCommand


class HMCXMLCommand(BaseCommand):
    name = "hmc-xml"
    help = "Generate HMC XML input file"
    job_type = None
    input_type = "hmc_xml"
    job_schema = []


def register(subparsers):
    HMCXMLCommand().register(subparsers)
