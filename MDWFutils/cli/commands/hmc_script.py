"""HMC script command with cpu/gpu variants."""

from ..command import BaseCommand
from ..param_schemas import HMC_INPUT_SCHEMA


class HMCGPUCommand(BaseCommand):
    help = "Generate GPU HMC script"
    job_type = "hmc_gpu"
    input_type = "hmc_xml"
    input_schema = HMC_INPUT_SCHEMA
    default_variant = "gpu"


class HMCCPUCommand(BaseCommand):
    help = "Generate CPU HMC script"
    job_type = "hmc_cpu"
    input_type = "hmc_xml"
    input_schema = HMC_INPUT_SCHEMA
    default_variant = "cpu"


class HMCCommand:
    name = "hmc-script"
    help = "Generate HMC SLURM scripts"

    def __init__(self):
        self.commands = {
            "gpu": HMCGPUCommand(),
            "cpu": HMCCPUCommand(),
        }

    def register(self, subparsers):
        import argparse

        parser = subparsers.add_parser(
            self.name,
            help=self.help,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="Select GPU or CPU subcommand",
        )
        variants = parser.add_subparsers(dest="variant", required=True)
        for variant, command in self.commands.items():
            variant_parser = variants.add_parser(
                variant,
                help=command.help,
                formatter_class=argparse.RawDescriptionHelpFormatter,
                description=command._build_description(),
            )
            command._add_arguments(variant_parser)
            variant_parser.set_defaults(func=command.execute)


def register(subparsers):
    HMCCommand().register(subparsers)
