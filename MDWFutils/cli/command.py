"""Base CLI command scaffolding."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import List, Optional

from ..backends import get_backend
from ..exceptions import MDWFError
from .args import (
    add_default_params_group,
    add_ensemble_arg,
    add_input_params_arg,
    add_job_params_arg,
    add_output_file_arg,
)
from .components import EnsembleResolver, ParameterManager, ScriptGenerator
from .help_generator import HelpGenerator
from .param_schemas import ParamDef


def _load_default_backend():
    connection = os.getenv("MDWF_DB_URL")
    if not connection:
        connection = os.getenv("MDWF_DB", "mdwf_ensembles.db")
    return get_backend(connection)


class BaseCommand:
    """Template method implementation for CLI commands."""

    name: Optional[str] = None
    help: Optional[str] = None
    job_type: Optional[str] = None
    input_type: Optional[str] = None
    input_schema: List[ParamDef] = []
    job_schema: List[ParamDef] = []
    default_variant: str = "default"

    def __init__(self, backend=None):
        self._backend_override = backend
        self.help_gen = HelpGenerator()

    # ------------------------------------------------------------------
    # Argparse registration helpers
    # ------------------------------------------------------------------
    def register(self, subparsers):
        import argparse

        parser = subparsers.add_parser(
            self.name,
            help=self.help,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=self._build_description(),
        )
        self._add_arguments(parser)
        parser.set_defaults(func=self.execute)

    def _build_description(self) -> str:
        description = self.help or ""
        description += self.help_gen.generate_help(self.input_schema, "Input parameters")
        description += self.help_gen.generate_help(self.job_schema, "Job parameters")
        return description

    def _add_arguments(self, parser):
        add_ensemble_arg(parser)
        add_input_params_arg(parser)
        add_job_params_arg(parser)
        add_output_file_arg(parser)
        add_default_params_group(parser)
        self.add_custom_args(parser)

    # ------------------------------------------------------------------
    # Execution workflow
    # ------------------------------------------------------------------
    def execute(self, args):
        try:
            backend = self._resolve_backend(args)
            resolver = EnsembleResolver(backend)
            param_manager = ParameterManager(backend)
            generator = ScriptGenerator(backend)

            ensemble_id, ensemble = resolver.resolve(args.ensemble)

            defaults = {"input_params": "", "job_params": ""}
            if args.use_default_params:
                variant = args.params_variant or self.default_variant
                defaults = param_manager.load_defaults(ensemble_id, self.job_type, variant)

            default_input = param_manager.parse(defaults.get("input_params", ""))
            cli_input = param_manager.parse(args.input_params or "")
            merged_input = param_manager.merge(default_input, cli_input)

            default_job = param_manager.parse(defaults.get("job_params", ""))
            cli_job = param_manager.parse(args.job_params or "")
            merged_job = param_manager.merge(default_job, cli_job)

            typed_input = (
                self.help_gen.validate_and_cast(merged_input, self.input_schema)
                if self.input_schema
                else merged_input
            )
            # Merge validated job params with original to preserve extra parameters not in schema
            validated_job = (
                self.help_gen.validate_and_cast(merged_job, self.job_schema)
                if self.job_schema
                else {}
            )
            typed_job = {**merged_job, **validated_job}  # Preserve all params, with validated ones taking precedence

            self.custom_validation(typed_input, typed_job, ensemble)

            if self.input_type:
                input_content = generator.generate_input(
                    ensemble_id, self.input_type, typed_input
                )
                self._write_file(ensemble, input_content, args.output_file, suffix=".in")

            if self.job_type:
                script_content = generator.generate_slurm(
                    ensemble_id, self.job_type, typed_job, typed_input
                )
                script_path = self._write_file(
                    ensemble,
                    script_content,
                    args.output_file,
                    suffix=".sh",
                    executable=True,
                )
                print(f"Wrote SLURM script to {script_path}")

            if args.save_default_params:
                variant = args.params_variant or self.default_variant
                param_manager.save_defaults(
                    ensemble_id,
                    self.job_type,
                    variant,
                    args.input_params or "",
                    args.job_params or "",
                )

            return 0
        except MDWFError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------
    def add_custom_args(self, parser):
        """Override to add command-specific arguments."""

    def custom_validation(self, input_params, job_params, ensemble):
        """Override to implement job-specific validation."""

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _resolve_backend(self, args):
        if self._backend_override is not None:
            return self._backend_override
        return _load_default_backend()

    def _write_file(self, ensemble, content: str, output_file: str | None, suffix: str, executable: bool = False):
        if output_file:
            path = Path(output_file)
        else:
            target_dir = Path(ensemble["directory"]) / "cnfg" / "slurm"
            target_dir.mkdir(parents=True, exist_ok=True)
            prefix = self.job_type or "output"
            identifier = ensemble.get("id", ensemble.get("ensemble_id", ""))
            filename = f"{prefix}_{identifier}{suffix}" if identifier else f"{prefix}{suffix}"
            path = target_dir / filename
        path.write_text(content)
        if executable:
            path.chmod(0o755)
        return path


