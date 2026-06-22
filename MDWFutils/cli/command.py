"""Base CLI command scaffolding."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import List, Optional, Type

from ..backends import get_backend
from ..exceptions import ConnectionError, MDWFError
from ..jobs.schema import (
    ContextBuilder,
    ContextParam,
    _deduplicate_schema,
    collapse_schema_aliases,
    resolve_param_aliases,
    storable_params,
)
from .args import (
    add_default_params_group,
    add_dry_run_flag,
    add_ensemble_arg,
    add_input_params_arg,
    add_job_params_arg,
    add_output_file_arg,
    add_params_flag,
)
from .components import EnsembleResolver, ParameterManager, ScriptGenerator
from .help_generator import HelpGenerator


def _load_default_backend():
    connection = os.getenv("MDWF_DB_URL")
    if not connection:
        raise ConnectionError("MDWF_DB_URL environment variable not set")
    return get_backend(connection)


def resolve_command_schemas(cmd) -> tuple[Optional[List], Optional[List]]:
    """Resolve input and job parameter schemas for a command instance."""
    input_schema = None
    job_schema = None

    if cmd.job_builder_class is not None:
        job_schema = _deduplicate_schema(cmd.job_builder_class.job_params_schema)
        job_input = cmd.job_builder_class.input_params_schema or []
        if cmd.input_builder_class is not None:
            xml_input = cmd.input_builder_class.input_params_schema or []
            # Job-builder input params win on name collisions (e.g. HMC run settings).
            input_schema = _deduplicate_schema(xml_input + job_input)
            input_schema = collapse_schema_aliases(input_schema)
        else:
            input_schema = _deduplicate_schema(job_input)
    elif cmd.input_builder_class is not None:
        input_schema = _deduplicate_schema(cmd.input_builder_class.input_params_schema)
    elif cmd.job_type:
        from ..jobs.registry import get_job_schema

        job_schema, input_schema = get_job_schema(cmd.job_type)
    elif cmd.input_type:
        from ..jobs.registry import get_input_schema

        input_schema = get_input_schema(cmd.input_type)

    return input_schema, job_schema


def _resolve_input_schema(command: "BaseCommand") -> Optional[List[ContextParam]]:
    """Merge input schemas from job and input builders when both are present."""
    input_schema, _ = resolve_command_schemas(command)
    return input_schema


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
        input_schema, job_schema = resolve_command_schemas(self)

        if input_schema:
            description += self.help_gen.generate_help(
                input_schema, "Input parameters (-i)"
            )
        if job_schema:
            description += self.help_gen.generate_help(
                job_schema, "Job parameters (-j)"
            )

        return description

    def _add_arguments(self, parser):
        # -e is not required when --params is used (validated in execute)
        add_ensemble_arg(parser, required=False)
        add_input_params_arg(parser)
        add_job_params_arg(parser)
        add_output_file_arg(parser)
        add_default_params_group(parser)
        add_dry_run_flag(parser)
        add_params_flag(parser)
        self.add_custom_args(parser)

    # ------------------------------------------------------------------
    # Execution workflow
    def execute(self, args):
        # Handle --params flag early (doesn't require ensemble or DB)
        if getattr(args, "params", False):
            return self._print_params()

        # Validate that -e is provided for normal execution
        if not args.ensemble:
            print("ERROR: -e/--ensemble is required", file=sys.stderr)
            print(
                "Hint: Use --params to see parameter documentation without an ensemble",
                file=sys.stderr,
            )
            return 1

        try:
            backend = self._resolve_backend(args)
            resolver = EnsembleResolver(backend)
            param_manager = ParameterManager(backend)
            generator = ScriptGenerator(backend)

            ensemble_id, ensemble = resolver.resolve(args.ensemble)

            variant = args.params_variant or self.default_variant
            command_name = self.name or self.job_type or "unknown"

            # Load DB defaults (default behavior; opt-out with --no-defaults)
            load_defaults = not getattr(args, "no_defaults", False)
            if load_defaults:
                db_defaults = param_manager.load_ensemble_defaults(
                    ensemble_id, command_name, variant
                )
            else:
                db_defaults = {"input_params": {}, "job_params": {}}

            # Parse CLI params
            cli_input = param_manager.parse(args.input_params or "")
            cli_job = param_manager.parse(args.job_params or "")

            # Merge: DB defaults -> CLI overrides (CLI wins)
            merged_input = param_manager.merge(
                db_defaults.get("input_params", {}), cli_input
            )
            merged_job = param_manager.merge(db_defaults.get("job_params", {}), cli_job)

            # Resolve schemas
            builder_input_schema, builder_job_schema = resolve_command_schemas(self)

            if (
                self.job_builder_class is not None
                and self.input_builder_class is not None
            ):
                job_input = self.job_builder_class.input_params_schema or []
                xml_input = self.input_builder_class.input_params_schema or []
                full_input_schema = _deduplicate_schema(xml_input + job_input)
                merged_input = resolve_param_aliases(merged_input, full_input_schema)

            # Apply schema defaults and validate
            if builder_input_schema is not None:
                typed_input = self.help_gen.apply_defaults_and_validate(
                    merged_input, builder_input_schema, "input"
                )
            else:
                typed_input = merged_input

            if builder_job_schema is not None:
                typed_job = self.help_gen.apply_defaults_and_validate(
                    merged_job, builder_job_schema, "job"
                )
            else:
                typed_job = merged_job

            self.custom_validation(typed_input, typed_job, ensemble)

            # Determine source for each param (for dry-run and staleness)
            input_sources = self._param_sources(
                builder_input_schema,
                db_defaults.get("input_params", {}),
                cli_input,
            )
            job_sources = self._param_sources(
                builder_job_schema,
                db_defaults.get("job_params", {}),
                cli_job,
            )

            # Check for CLI overrides of DB defaults (staleness warning)
            self._check_staleness(
                args, db_defaults, cli_input, cli_job, command_name, variant
            )

            # Dry-run: print effective params and exit
            if getattr(args, "dry_run", False):
                self._print_dry_run(
                    command_name,
                    ensemble,
                    variant,
                    typed_input,
                    typed_job,
                    input_sources,
                    job_sources,
                )
                return 0

            # Build job context
            job_context = None
            if self.job_builder_class is not None:
                job_builder = self.job_builder_class()
                job_context = job_builder.build(
                    backend, ensemble_id, typed_job, typed_input
                )
            elif self.job_type:
                from ..jobs.registry import get_job_builder

                job_builder = get_job_builder(self.job_type)
                job_context = job_builder.build(
                    backend, ensemble_id, typed_job, typed_input
                )

            # Generate input file if needed
            input_path = None
            if self.input_builder_class is not None or self.input_type:
                if self.input_builder_class is not None:
                    input_builder = self.input_builder_class()
                else:
                    from ..jobs.registry import get_input_builder

                    input_builder = get_input_builder(self.input_type)

                input_context = input_builder.build(
                    backend, ensemble_id, typed_job, typed_input
                )

                if job_context and "_input_output_dir" in job_context:
                    input_context["_output_dir"] = job_context["_input_output_dir"]
                    if "_input_output_prefix" in job_context:
                        input_context["_output_prefix"] = job_context[
                            "_input_output_prefix"
                        ]

                if self.input_builder_class is not None:
                    input_content = generator.renderer.render(
                        f"input/{input_builder.type_name}.j2",
                        input_context,
                    )
                else:
                    input_content = generator.generate_input(
                        ensemble_id, self.input_type, typed_input, job_params=typed_job
                    )
                input_suffix = input_context.get("_output_suffix", ".in")
                input_path = self._write_file(
                    ensemble,
                    input_content,
                    args.output_file,
                    suffix=input_suffix,
                    context=input_context,
                )

                input_names = {
                    "hmc_xml": "HMC XML",
                    "wit_input": "WIT",
                    "glu_input": "GLU",
                }
                input_type_name = (
                    self.input_builder_class.type_name
                    if self.input_builder_class is not None
                    else self.input_type
                )
                display_name = input_names.get(input_type_name, input_type_name)
                print(f"Generated {display_name} input: {input_path}")

            # Generate job script if needed
            script_path = None
            if self.job_builder_class is not None or self.job_type:
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

            # Save merged defaults back if --update
            if getattr(args, "update", False):
                self._save_merged_defaults(
                    param_manager,
                    ensemble_id,
                    command_name,
                    variant,
                    typed_input,
                    typed_job,
                    builder_input_schema,
                    builder_job_schema,
                )

            return 0
        except MDWFError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1

    # ------------------------------------------------------------------
    # Parameter source tracking
    # ------------------------------------------------------------------
    def _param_sources(
        self,
        schema: Optional[List[ContextParam]],
        db_defaults: Dict[str, str],
        cli_overrides: Dict[str, str],
    ) -> Dict[str, str]:
        """Determine the source of each parameter value."""
        sources = {}
        if not schema:
            return sources
        for param in schema:
            if param.name in cli_overrides:
                sources[param.name] = "CLI override"
            elif param.name in db_defaults:
                sources[param.name] = "DB default"
            elif param.default is not None:
                sources[param.name] = "Schema default"
            else:
                sources[param.name] = "Required"
        return sources

    def _check_staleness(
        self,
        args,
        db_defaults: Dict[str, Dict[str, str]],
        cli_input: Dict[str, str],
        cli_job: Dict[str, str],
        command_name: str,
        variant: str,
    ):
        """Warn if CLI overrides differ from saved defaults."""
        if getattr(args, "force", False):
            return
        if getattr(args, "update", False):
            return

        db_input = db_defaults.get("input_params", {})
        db_job = db_defaults.get("job_params", {})

        overrides = []
        for name, value in cli_input.items():
            if name in db_input and db_input[name] != value:
                overrides.append(f"  {name}: CLI={value}, saved={db_input[name]}")
        for name, value in cli_job.items():
            if name in db_job and db_job[name] != value:
                overrides.append(f"  {name}: CLI={value}, saved={db_job[name]}")

        if overrides:
            print(
                f"WARNING: CLI overrides differ from saved defaults (variant: {variant}).",
                file=sys.stderr,
            )
            for line in overrides:
                print(line, file=sys.stderr)
            print(
                f"Use --update to persist these changes to defaults.",
                file=sys.stderr,
            )

    def _save_merged_defaults(
        self,
        param_manager: ParameterManager,
        ensemble_id: int,
        command_name: str,
        variant: str,
        typed_input: Dict,
        typed_job: Dict,
        input_schema: Optional[List[ContextParam]],
        job_schema: Optional[List[ContextParam]],
    ):
        """Save only storable params back as defaults, using merged values."""
        # Filter to storable params only
        storable_input = {}
        storable_job = {}

        if input_schema:
            for param in storable_params(input_schema):
                if param.name in typed_input:
                    storable_input[param.name] = str(typed_input[param.name])

        if job_schema:
            for param in storable_params(job_schema):
                if param.name in typed_job:
                    storable_job[param.name] = str(typed_job[param.name])

        param_manager.save_ensemble_defaults(
            ensemble_id, command_name, variant, storable_input, storable_job
        )
        print(f"Updated defaults for {command_name} (variant: {variant})")

    def _print_dry_run(
        self,
        command_name: str,
        ensemble: Dict,
        variant: str,
        typed_input: Dict,
        typed_job: Dict,
        input_sources: Dict[str, str],
        job_sources: Dict[str, str],
    ):
        """Print effective parameters and target files without writing."""
        ens_id = ensemble.get("ensemble_id", ensemble.get("id", "?"))
        ens_nick = ensemble.get("nickname", "")
        ens_label = f"{ens_id}" + (f" ({ens_nick})" if ens_nick else "")
        print(f"Command: {command_name}  Ensemble: {ens_label}  Variant: {variant}")
        print()

        # Input params table
        if typed_input:
            print("Input Parameters:")
            self._print_param_table(typed_input, input_sources)
            print()

        # Job params table
        if typed_job:
            print("Job Parameters:")
            self._print_param_table(typed_job, job_sources)
            print()

    def _print_param_table(self, params: Dict, sources: Dict[str, str]):
        """Print a formatted table of parameters with sources."""
        if not params:
            return
        max_name = max(len(str(k)) for k in params)
        max_val = max(len(str(v)) for v in params.values())
        header = f"{'Parameter':<{max_name}}  {'Value':<{max_val}}  Source"
        print(header)
        print(f"{'-' * max_name}  {'-' * max_val}  {'-' * 15}")
        for name, value in params.items():
            source = sources.get(name, "unknown")
            print(f"{str(name):<{max_name}}  {str(value):<{max_val}}  {source}")

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
        input_schema, job_schema = resolve_command_schemas(self)

        output = self.help_gen.format_params_detailed(
            input_schema or [], job_schema or [], command_name=self.name or ""
        )
        print(output)
        return 0

    def _resolve_backend(self, args):
        if self._backend_override is not None:
            return self._backend_override
        return _load_default_backend()

    def _write_file(
        self,
        ensemble,
        content: str,
        output_file: str | None,
        suffix: str,
        context: dict = None,
        executable: bool = False,
    ):
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
            filename = (
                f"{prefix}_{identifier}{suffix}" if identifier else f"{prefix}{suffix}"
            )
            path = target_dir / filename

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        if executable:
            path.chmod(0o755)
        return path
