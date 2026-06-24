"""HMC script command with cpu/gpu variants."""

import re
import sys
from pathlib import Path

from ..command import BaseCommand
from ...exceptions import ValidationError
from ...jobs.hmc import HMCGPUContextBuilder, HMCCPUContextBuilder, HMCXMLContextBuilder


def _extract_cfg_numbers(cnfg_dir: Path):
    """Extract config numbers from ckpoint_EODWF_lat.{number} files."""
    if not cnfg_dir.exists():
        return []
    pattern = re.compile(r'^ckpoint_EODWF_lat\.(\d+)$')
    numbers = []
    for child in cnfg_dir.iterdir():
        if not child.is_file():
            continue
        m = pattern.match(child.name)
        if m:
            numbers.append(int(m.group(1)))
    return sorted(set(numbers))


def _validate_hmc_mode_and_config_start(input_params, job_params, ensemble):
    """Validate mode/config_start against existing cnfg files."""
    directory = ensemble.get("directory", "")
    if not directory:
        return

    cnfg_dir = Path(directory) / "cnfg"
    cfg_numbers = _extract_cfg_numbers(cnfg_dir)
    mode = input_params.get("mode", "tepid")
    config_start = input_params.get("config_start")

    if not cfg_numbers:
        # No configs exist - mode must be tepid
        if mode != "tepid":
            raise ValidationError(
                f"No configuration files found in {cnfg_dir}. "
                f"Mode must be 'tepid' when starting from scratch."
            )
        # Auto-set config_start to 0 if not specified
        if config_start is None:
            input_params["config_start"] = 0
    else:
        last_cfg = max(cfg_numbers)
        # Auto-set config_start to last config if not specified
        if config_start is None:
            input_params["config_start"] = last_cfg
        # Warn if mode is tepid or reseed but configs exist
        if mode in ("tepid", "reseed") and last_cfg > 0:
            print(
                f"WARNING: Config files found up to {last_cfg}, but mode is '{mode}'. "
                f"This will overwrite existing progress.",
                file=sys.stderr,
            )
            # Only prompt for confirmation in interactive mode
            if sys.stdin.isatty():
                try:
                    resp = input("Continue anyway? [y/N] ").strip().lower()
                except EOFError:
                    resp = "n"
                if resp != "y":
                    raise ValidationError("Aborted by user.")
            # In non-interactive mode, allow but warn


class HMCGPUCommand(BaseCommand):
    name = "hmc-script"
    help = "Generate GPU HMC script"
    job_builder_class = HMCGPUContextBuilder
    input_builder_class = HMCXMLContextBuilder
    default_variant = "gpu"

    def custom_validation(self, input_params, job_params, ensemble):
        _validate_hmc_mode_and_config_start(input_params, job_params, ensemble)


class HMCCPUCommand(BaseCommand):
    name = "hmc-script"
    help = "Generate CPU HMC script"
    job_builder_class = HMCCPUContextBuilder
    input_builder_class = HMCXMLContextBuilder
    default_variant = "cpu"

    def custom_validation(self, input_params, job_params, ensemble):
        _validate_hmc_mode_and_config_start(input_params, job_params, ensemble)


class HMCCommand:
    name = "hmc-script"
    aliases = ["hmc"]
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
            aliases=getattr(self, "aliases", []),
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
