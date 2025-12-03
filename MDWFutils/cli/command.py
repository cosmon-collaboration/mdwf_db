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

            # Try to get schemas from context builder (new system)
            builder_job_schema = None
            builder_input_schema = None
            if self.job_type:
                from ..jobs.registry import get_job_schema
                builder_job_schema, builder_input_schema = get_job_schema(self.job_type)
            
            # Handle input params: use builder schema with defaults if available, else command schema without defaults
            if builder_input_schema is not None:
                # New system: apply defaults from context builder schema
                typed_input = self.help_gen.apply_defaults_and_validate(merged_input, builder_input_schema, "input")
            elif self.input_schema:
                # Old system: validate only (no defaults from schema)
                typed_input = self.help_gen.validate_and_cast(merged_input, self.input_schema, "input")
            else:
                typed_input = merged_input
            
            # Use builder job schema if available (applies defaults), otherwise fall back to command's job_schema (no defaults)
            if builder_job_schema is not None:
                # New system: apply defaults from context builder schema
                typed_job = self.help_gen.apply_defaults_and_validate(merged_job, builder_job_schema, "job")
                # Merge with original to preserve any extra params not in schema
                typed_job = {**merged_job, **typed_job}
            elif self.job_schema:
                # Old system: no defaults, just validate and type-cast
                validated_job = self.help_gen.validate_and_cast(merged_job, self.job_schema, "job")
                typed_job = {**merged_job, **validated_job}
            else:
                typed_job = merged_job

            self.custom_validation(typed_input, typed_job, ensemble)

            # Build job context first (if exists) to get custom input file location
            job_context = None
            if self.job_type:
                from ..jobs.registry import get_job_builder
                import inspect
                
                job_builder = get_job_builder(self.job_type)
                # Check if it's a class (new system) or function (old system)
                if inspect.isclass(job_builder):
                    # New class-based builder: instantiate then call build()
                    builder_instance = job_builder()
                    job_context = builder_instance.build(backend, ensemble_id, typed_job, typed_input)
                else:
                    # Old function-based builder: call directly
                    job_context = job_builder(backend, ensemble_id, typed_job, typed_input)

            # Generate input file if needed
            if self.input_type:
                from ..jobs.registry import get_input_builder
                input_builder = get_input_builder(self.input_type)
                input_context = input_builder(backend, ensemble_id, typed_input)
                
                # Override input location if job context specifies it
                if job_context and "_input_output_dir" in job_context:
                    input_context["_output_dir"] = job_context["_input_output_dir"]
                    # Optionally override prefix too
                    if "_input_output_prefix" in job_context:
                        input_context["_output_prefix"] = job_context["_input_output_prefix"]
                
                input_content = generator.generate_input(
                    ensemble_id, self.input_type, typed_input
                )
                input_path = self._write_file(ensemble, input_content, args.output_file, suffix=".in", context=input_context)
                
                # Print friendly name
                input_names = {
                    "hmc_xml": "HMC XML",
                    "wit_input": "WIT",
                    "glu_input": "GLU"
                }
                display_name = input_names.get(self.input_type, self.input_type)
                print(f"Generated {display_name} input: {input_path}")

            # Generate job script if needed
            if self.job_type:
                script_content = generator.generate_slurm(
                    ensemble_id, self.job_type, typed_job, typed_input
                )
                script_path = self._write_file(
                    ensemble,
                    script_content,
                    args.output_file,
                    suffix=".sh",
                    context=job_context,
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

    def _write_file(self, ensemble, content: str, output_file: str | None, suffix: str, context: dict = None, executable: bool = False):
        if output_file:
            path = Path(output_file)
        elif context and "_output_dir" in context:
            # Use job-specific directory from context
            target_dir = Path(context["_output_dir"])
            prefix = context.get("_output_prefix", self.job_type or "output")
            path = target_dir / f"{prefix}{suffix}"
        else:
            # Fallback for commands without context
            target_dir = Path(ensemble["directory"]) / "cnfg" / "slurm"
            target_dir.mkdir(parents=True, exist_ok=True)
            prefix = self.job_type or "output"
            identifier = ensemble.get("id", ensemble.get("ensemble_id", ""))
            filename = f"{prefix}_{identifier}{suffix}" if identifier else f"{prefix}{suffix}"
            path = target_dir / filename
        
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        if executable:
            path.chmod(0o755)
        return path


