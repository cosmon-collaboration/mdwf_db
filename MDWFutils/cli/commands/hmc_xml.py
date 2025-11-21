"""Generate HMC XML input via BaseCommand."""

from ..command import BaseCommand
from ..param_schemas import HMC_INPUT_SCHEMA


class HMCXMLCommand(BaseCommand):
    name = "hmc-xml"
    help = "Generate HMC XML input file"
    job_type = None
    input_type = "hmc_xml"
    input_schema = HMC_INPUT_SCHEMA
    job_schema = []


def register(subparsers):
    HMCXMLCommand().register(subparsers)
