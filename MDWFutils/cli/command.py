"""Base CLI command scaffolding."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import List, Optional, Type

from ..backends import get_backend
from ..exceptions import ConnectionError, MDWFError
from .args import (
    add_default_params_group,
    add_ensemble_arg,
    add_input_params_arg,
    add_job_params_arg,
    add_output_file_arg,
    add_params_flag,
)
from .components import EnsembleResolver, ParameterManager, ScriptGenerator
from .help_generator import HelpGenerator
from .json_output import print_json
from ..jobs.schema import ContextParam, ContextBuilder, _deduplicate_schema


def _load_default_backend():
    connection = os.getenv("MDWF_DB_URL")
    if not connection:
        raise ConnectionError("MDWF_DB_URL environment variable not set")
    return get_backend(connection)


class BaseCommand:
    """Template method implementation for CLI commands.
    
    Commands can specify builders two ways:
    1. Direct class references (preferred): job_builder_class, input_builder_class
    2. String names (legacy): job_type, input_type
    
    Direct class references provide IDE support (autocomplete, go-to-definition)
    and eliminate magic naming.
    """

    name: Optional[str] = None
    help: Optional[str] = None
    aliases: list[str] = []
    
    # New: Direct builder class references (preferred)
    job_builder_class: Optional[Type[ContextBuilder]] = None
    input_builder_class: Optional[Type[ContextBuilder]] = None
    
    # Legacy: String-based type names (for backward compatibility)
    _job_type: Optional[str] = None
    _input_type: Optional[str] = None
    
    default_variant: str = "default"

    def __init__(self, backend=None):
        self._backend_override = backend
        self.help_gen = HelpGenerator()

    @property
    def job_type(self) -> Optional[str]:
        """Get job type name from builder class or legacy attribute."""
        if self.job_builder_class is not None:
            return self.job_builder_class.type_name
        return self._job_type
    
    @job_type.setter
    def job_type(self, value: Optional[str]):
        """Allow setting job_type for backward compatibility."""
        self._job_type = value

    @property
    def input_type(self) -> Optional[str]:
        """Get input type name from builder class or legacy attribute."""
        if self.input_builder_class is not None:
            return self.input_builder_class.type_name
        return self._input_type
    
    @input_type.setter
    def input_type(self, value: Optional[str]):
        """Allow setting input_type for backward compatibility."""
        self._input_type = value

    # ------------------------------------------------------------------
    # Argparse registration helpers
    # ------------------------------------------------------------------
    def register(self, subparsers):
        import argparse

        parser = subparsers.add_parser(
            self.name,
            aliases=getattr(self, "aliases", []),
            help=self.help,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=self._build_description(),
        )
        self._add_arguments(parser)
        parser.set_defaults(func=self.execute)

    def _build_description(self) -> str:
        """Build help description including parameter schemas from builder classes."""
        description = self.help or ""
        
        # Get schemas from builder classes (preferred) or fall back to registry
        input_schema = None
        job_schema = None
        
        if self.job_builder_class is not None:
            input_schema = _deduplicate_schema(self.job_builder_class.input_params_schema)
            job_schema = _deduplicate_schema(self.job_builder_class.job_params_schema)
        elif self.input_builder_class is not None:
            input_schema = _deduplicate_schema(self.input_builder_class.input_params_schema)
        elif self.job_type:
            # Legacy: fetch from registry by string name
            from ..jobs.registry import get_job_schema
            job_schema, input_schema = get_job_schema(self.job_type)
        elif self.input_type:
            from ..jobs.registry import get_input_schema
            input_schema = get_input_schema(self.input_type)
        
        if input_schema:
            description += self.help_gen.generate_help(input_schema, "Input parameters (-i)")
        if job_schema:
            description += self.help_gen.generate_help(job_schema, "Job parameters (-j)")
        
        return description

    def _add_arguments(self, parser):
        # -e is not required when --params is used (validated in execute)
        add_ensemble_arg(parser, required=False)
        add_input_params_arg(parser)
        add_job_params_arg(parser)
        add_output_file_arg(parser)
        add_default_params_group(parser)
        add_params_flag(parser)
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Validate and report planned outputs without writing files or saving defaults",
        )
        parser.add_argument(
            "--json",
            action="store_true",
            help="Print machine-readable JSON output",
        )
        self.add_custom_args(parser)

    # ------------------------------------------------------------------
    # Execution workflow
    # ------------------------------------------------------------------
    def execute(self, args):
        # Handle --params flag early (doesn't require ensemble or DB)
        if getattr(args, 'params', False):
            return self._print_params()
        
        # Validate that -e is provided for normal execution
        if not args.ensemble:
            print("ERROR: -e/--ensemble is required", file=sys.stderr)
            print("Hint: Use --params to see parameter documentation without an ensemble", file=sys.stderr)
            return 1
        
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

            # Get schemas from builder classes (preferred) or fall back to registry
            builder_job_schema = None
            builder_input_schema = None
            
            if self.job_builder_class is not None:
                builder_job_schema = _deduplicate_schema(self.job_builder_class.job_params_schema)
                builder_input_schema = _deduplicate_schema(self.job_builder_class.input_params_schema)
            elif self.input_builder_class is not None:
                builder_input_schema = _deduplicate_schema(self.input_builder_class.input_params_schema)
            elif self.job_type:
                # Legacy: fetch from registry by string name
                from ..jobs.registry import get_job_schema
                builder_job_schema, builder_input_schema = get_job_schema(self.job_type)
            elif self.input_type:
                from ..jobs.registry import get_input_schema
                builder_input_schema = get_input_schema(self.input_type)
            
            # Handle input params: use builder schema with defaults if available
            if builder_input_schema is not None:
                typed_input = self.help_gen.apply_defaults_and_validate(merged_input, builder_input_schema, "input")
            else:
                typed_input = merged_input
            
            # Handle job params: use builder schema with defaults if available
            if builder_job_schema is not None:
                typed_job = self.help_gen.apply_defaults_and_validate(merged_job, builder_job_schema, "job")
            else:
                typed_job = merged_job

            self.custom_validation(typed_input, typed_job, ensemble)
            if getattr(args, "dry_run", False):
                typed_input["_dry_run"] = True
                typed_job["_dry_run"] = True

            # Build job context first (if exists) to get custom input file location
            job_context = None
            planned_outputs = []
            if self.job_builder_class is not None:
                job_builder = self.job_builder_class()
                job_context = job_builder.build(backend, ensemble_id, typed_job, typed_input)
            elif self.job_type:
                # Legacy fallback
                from ..jobs.registry import get_job_builder
                job_builder = get_job_builder(self.job_type)
                job_context = job_builder.build(backend, ensemble_id, typed_job, typed_input)

            # Generate input file if needed
            if self.input_builder_class is not None or self.input_type:
                if self.input_builder_class is not None:
                    input_builder = self.input_builder_class()
                else:
                    from ..jobs.registry import get_input_builder
                    input_builder = get_input_builder(self.input_type)
                
                # Provide job_params as well so input builders can derive values like CONFNO
                input_context = input_builder.build(backend, ensemble_id, typed_job, typed_input)
                
                # Override input location if job context specifies it
                if job_context and "_input_output_dir" in job_context:
                    input_context["_output_dir"] = job_context["_input_output_dir"]
                    # Optionally override prefix too
                    if "_input_output_prefix" in job_context:
                        input_context["_output_prefix"] = job_context["_input_output_prefix"]
                
                input_content = generator.generate_input(
                    ensemble_id, self.input_type, typed_input, job_params=typed_job
                )
                input_path = self._resolve_output_path(ensemble, args.output_file, suffix=".in", context=input_context)
                planned_outputs.append({
                    "kind": "input",
                    "path": str(input_path),
                    "bytes": len(input_content.encode("utf-8")),
                    "would_write": bool(getattr(args, "dry_run", False)),
                })
                if not getattr(args, "dry_run", False):
                    input_path = self._write_file(ensemble, input_content, args.output_file, suffix=".in", context=input_context)
                
                # Print friendly name
                input_names = {
                    "hmc_xml": "HMC XML",
                    "wit_input": "WIT",
                    "glu_input": "GLU"
                }
                display_name = input_names.get(self.input_type, self.input_type)
                if not getattr(args, "json", False):
                    verb = "Would generate" if getattr(args, "dry_run", False) else "Generated"
                    print(f"{verb} {display_name} input: {input_path}")

            # Generate job script if needed
            if self.job_builder_class is not None or self.job_type:
                script_content = generator.generate_slurm(
                    ensemble_id, self.job_type, typed_job, typed_input
                )
                script_path = self._resolve_output_path(
                    ensemble,
                    args.output_file,
                    suffix=".sh",
                    context=job_context,
                )
                planned_outputs.append({
                    "kind": "slurm",
                    "path": str(script_path),
                    "bytes": len(script_content.encode("utf-8")),
                    "executable": True,
                    "would_write": bool(getattr(args, "dry_run", False)),
                })
                if not getattr(args, "dry_run", False):
                    script_path = self._write_file(
                        ensemble,
                        script_content,
                        args.output_file,
                        suffix=".sh",
                        context=job_context,
                        executable=True,
                    )
                if not getattr(args, "json", False):
                    verb = "Would write" if getattr(args, "dry_run", False) else "Wrote"
                    print(f"{verb} SLURM script to {script_path}")

            if args.save_default_params and not getattr(args, "dry_run", False):
                variant = args.params_variant or self.default_variant
                param_manager.save_defaults(
                    ensemble_id,
                    self.job_type,
                    variant,
                    args.input_params or "",
                    args.job_params or "",
                )
            elif args.save_default_params and getattr(args, "dry_run", False):
                planned_outputs.append({
                    "kind": "default_params",
                    "job_type": self.job_type,
                    "variant": args.params_variant or self.default_variant,
                    "would_write": True,
                })

            if getattr(args, "json", False):
                print_json({
                    "ok": True,
                    "status": "dry_run" if getattr(args, "dry_run", False) else "ok",
                    "command": self.name,
                    "ensemble_id": ensemble_id,
                    "job_type": self.job_type,
                    "input_type": self.input_type,
                    "input_params": typed_input,
                    "job_params": typed_job,
                    "outputs": planned_outputs,
                })

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
    def _print_params(self) -> int:
        """Print detailed parameter documentation and exit."""
        input_schema = None
        job_schema = None
        
        if self.job_builder_class is not None:
            input_schema = _deduplicate_schema(self.job_builder_class.input_params_schema)
            job_schema = _deduplicate_schema(self.job_builder_class.job_params_schema)
        elif self.input_builder_class is not None:
            input_schema = _deduplicate_schema(self.input_builder_class.input_params_schema)
        elif self.job_type:
            from ..jobs.registry import get_job_schema
            job_schema, input_schema = get_job_schema(self.job_type)
        elif self.input_type:
            from ..jobs.registry import get_input_schema
            input_schema = get_input_schema(self.input_type)
        
        output = self.help_gen.format_params_detailed(
            input_schema or [],
            job_schema or [],
            command_name=self.name or ""
        )
        print(output)
        return 0

    def _resolve_backend(self, args):
        if self._backend_override is not None:
            return self._backend_override
        return _load_default_backend()

    def _resolve_output_path(self, ensemble, output_file: str | None, suffix: str, context: dict = None) -> Path:
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
            prefix = self.job_type or "output"
            identifier = ensemble.get("id", ensemble.get("ensemble_id", ""))
            filename = f"{prefix}_{identifier}{suffix}" if identifier else f"{prefix}{suffix}"
            path = target_dir / filename
        return path

    def _write_file(self, ensemble, content: str, output_file: str | None, suffix: str, context: dict = None, executable: bool = False):
        path = self._resolve_output_path(ensemble, output_file, suffix, context)
        
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        if executable:
            path.chmod(0o755)
        return path
