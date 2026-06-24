"""Base CLI command scaffolding."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List, Optional, Type

from ..exceptions import MDWFError
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
from .runtime import load_default_backend


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

    return input_schema, job_schema


def _resolve_input_schema(command: "BaseCommand") -> Optional[List[ContextParam]]:
    """Merge input schemas from job and input builders when both are present."""
    input_schema, _ = resolve_command_schemas(command)
    return input_schema


class BaseCommand:
    """Template method implementation for CLI commands.

    Commands specify builders via direct class references:
    job_builder_class, input_builder_class
    """

    name: Optional[str] = None
    help: Optional[str] = None
    aliases: list[str] = []

    # Direct builder class references
    job_builder_class: Optional[Type[ContextBuilder]] = None
    input_builder_class: Optional[Type[ContextBuilder]] = None

    default_variant: str = "default"

    def __init__(self, backend=None):
        self._backend_override = backend
        self.help_gen = HelpGenerator()

    @property
    def job_type(self) -> Optional[str]:
        if self.job_builder_class is not None:
            return self.job_builder_class.type_name
        return None

    @property
    def input_type(self) -> Optional[str]:
        if self.input_builder_class is not None:
            return self.input_builder_class.type_name
        return None

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
                db_defaults = self._load_ensemble_defaults(
                    param_manager, ensemble_id, command_name, variant
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
            # Relax validation for --update and --dry-run (partial defaults)
            relaxed = getattr(args, "update", False) or getattr(args, "dry_run", False)
            if builder_input_schema is not None:
                typed_input = self.help_gen.apply_defaults_and_validate(
                    merged_input, builder_input_schema, "input", strict=not relaxed
                )
            else:
                typed_input = merged_input

            if builder_job_schema is not None:
                typed_job = self.help_gen.apply_defaults_and_validate(
                    merged_job, builder_job_schema, "job", strict=not relaxed
                )
            else:
                typed_job = merged_job

            missing_input = self._missing_required_params(
                builder_input_schema, typed_input
            )
            missing_job = self._missing_required_params(builder_job_schema, typed_job)

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
                args,
                db_defaults,
                cli_input,
                cli_job,
                command_name,
                variant,
                builder_input_schema,
                builder_job_schema,
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
                    builder_input_schema,
                    builder_job_schema,
                )
                return 0

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

            # --update can be used to store partial defaults. If required
            # generation params are still missing, skip file generation.
            if getattr(args, "update", False) and (missing_input or missing_job):
                self._print_update_skipped_generation(missing_input, missing_job)
                return 0

            # Build job context
            job_context = None
            if self.job_builder_class is not None:
                job_builder = self.job_builder_class()
                job_context = job_builder.build(
                    backend, ensemble_id, typed_job, typed_input
                )

            # Generate input file if needed
            input_path = None
            if self.input_builder_class is not None:
                input_builder = self.input_builder_class()

                input_context = input_builder.build(
                    backend, ensemble_id, typed_job, typed_input
                )

                if job_context and "_input_output_dir" in job_context:
                    input_context["_output_dir"] = job_context["_input_output_dir"]
                    if "_input_output_prefix" in job_context:
                        input_context["_output_prefix"] = job_context[
                            "_input_output_prefix"
                        ]

                input_content = generator.renderer.render(
                    f"input/{input_builder.type_name}.j2",
                    input_context,
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
                input_type_name = self.input_builder_class.type_name
                display_name = input_names.get(input_type_name, input_type_name)
                print(f"Generated {display_name} input: {input_path}")

            # Generate job script if needed
            script_path = None
            if self.job_builder_class is not None:
                script_content = generator.renderer.render(
                    f"slurm/{self.job_type}.j2", job_context
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

            return 0
        except MDWFError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1

    # ------------------------------------------------------------------
    # Parameter source tracking
    # ------------------------------------------------------------------
    def _load_ensemble_defaults(
        self,
        param_manager: ParameterManager,
        ensemble_id: int,
        command_name: str,
        variant: str,
    ) -> Dict[str, Dict[str, str]]:
        """Load defaults, falling back to legacy builder-key storage."""
        defaults = param_manager.load_ensemble_defaults(
            ensemble_id, command_name, variant
        )
        if self._has_saved_defaults(defaults):
            return defaults

        legacy_name = self._legacy_defaults_command_name(command_name)
        if not legacy_name:
            return defaults

        legacy_defaults = param_manager.load_ensemble_defaults(
            ensemble_id, legacy_name, variant
        )
        if self._has_saved_defaults(legacy_defaults):
            return legacy_defaults
        return defaults

    def _legacy_defaults_command_name(self, command_name: str) -> Optional[str]:
        """Return the old storage key for commands whose canonical name changed."""
        if self.job_type and self.job_type != command_name:
            return self.job_type
        return None

    @staticmethod
    def _has_saved_defaults(defaults: Dict[str, Dict[str, str]]) -> bool:
        return bool(
            defaults.get("input_params") or defaults.get("job_params")
        )

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
            if self._param_has_value(cli_overrides, param):
                sources[param.name] = "CLI override"
            elif self._param_has_value(db_defaults, param):
                sources[param.name] = "DB default"
            elif param.default is not None:
                sources[param.name] = "Schema default"
            else:
                sources[param.name] = "Required"
        return sources

    def _missing_required_params(
        self,
        schema: Optional[List[ContextParam]],
        typed_params: Dict,
    ) -> List[ContextParam]:
        """Return required params that are absent after merge/default handling."""
        if not schema:
            return []
        return [
            param
            for param in schema
            if param.required and param.name not in typed_params
        ]

    @staticmethod
    def _param_has_value(params: Dict, param: ContextParam) -> bool:
        if param.name in params and params[param.name] is not None:
            return True
        return any(
            alias in params and params[alias] is not None
            for alias in param.aliases
        )

    @staticmethod
    def _param_lookup(params: Dict, param: ContextParam):
        if param.name in params and params[param.name] is not None:
            return params[param.name]
        for alias in param.aliases:
            if alias in params and params[alias] is not None:
                return params[alias]
        return None

    def _check_staleness(
        self,
        args,
        db_defaults: Dict[str, Dict[str, str]],
        cli_input: Dict[str, str],
        cli_job: Dict[str, str],
        command_name: str,
        variant: str,
        input_schema: Optional[List[ContextParam]] = None,
        job_schema: Optional[List[ContextParam]] = None,
    ):
        """Warn if CLI overrides differ from saved defaults."""
        if getattr(args, "force", False):
            return
        if getattr(args, "update", False):
            return

        db_input = db_defaults.get("input_params", {})
        db_job = db_defaults.get("job_params", {})

        overrides = []
        overrides.extend(
            self._stale_param_overrides(db_input, cli_input, input_schema)
        )
        overrides.extend(self._stale_param_overrides(db_job, cli_job, job_schema))

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

    def _stale_param_overrides(
        self,
        db_defaults: Dict[str, str],
        cli_overrides: Dict[str, str],
        schema: Optional[List[ContextParam]],
    ) -> List[str]:
        """Return stale override warning lines, honoring parameter aliases."""
        warnings = []
        consumed_cli = set()

        for param in schema or []:
            cli_value = self._param_lookup(cli_overrides, param)
            db_value = self._param_lookup(db_defaults, param)
            if cli_value is None:
                continue
            consumed_cli.add(param.name)
            consumed_cli.update(param.aliases)
            if db_value is not None and str(db_value) != str(cli_value):
                warnings.append(
                    f"  {param.name}: CLI={cli_value}, saved={db_value}"
                )

        for name, value in cli_overrides.items():
            if name in consumed_cli:
                continue
            if name in db_defaults and str(db_defaults[name]) != str(value):
                warnings.append(f"  {name}: CLI={value}, saved={db_defaults[name]}")

        return warnings

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
        input_schema: Optional[List[ContextParam]] = None,
        job_schema: Optional[List[ContextParam]] = None,
    ):
        """Print effective parameters and target files without writing."""
        ens_id = ensemble.get("ensemble_id", ensemble.get("id", "?"))
        ens_nick = ensemble.get("nickname", "")
        ens_label = f"{ens_id}" + (f" ({ens_nick})" if ens_nick else "")
        print(f"Command: {command_name}  Ensemble: {ens_label}  Variant: {variant}")
        print()

        input_table, input_table_sources = self._dry_run_table_data(
            input_schema, typed_input, input_sources
        )
        job_table, job_table_sources = self._dry_run_table_data(
            job_schema, typed_job, job_sources
        )

        # Input params table
        if input_table:
            print("Input Parameters:")
            self._print_param_table(input_table, input_table_sources)
            print()

        # Job params table
        if job_table:
            print("Job Parameters:")
            self._print_param_table(job_table, job_table_sources)
            print()

    def _dry_run_table_data(
        self,
        schema: Optional[List[ContextParam]],
        typed_params: Dict,
        sources: Dict[str, str],
    ) -> tuple[Dict, Dict[str, str]]:
        """Build ordered dry-run rows from the full schema plus typed values."""
        table = {}
        table_sources = {}

        if schema:
            for param in schema:
                if param.name in typed_params:
                    table[param.name] = typed_params[param.name]
                    table_sources[param.name] = sources.get(param.name, "unknown")
                elif param.required:
                    table[param.name] = "<required>"
                    table_sources[param.name] = "Missing required"
                else:
                    table[param.name] = "<unset>"
                    table_sources[param.name] = "Unset optional"

        for name, value in typed_params.items():
            if name not in table:
                table[name] = value
                table_sources[name] = sources.get(name, "unknown")

        return table, table_sources

    def _print_update_skipped_generation(
        self,
        missing_input: List[ContextParam],
        missing_job: List[ContextParam],
    ) -> None:
        names = [p.name for p in missing_input + missing_job]
        joined = ", ".join(names)
        print(
            "Skipped file generation because required parameters are missing: "
            f"{joined}"
        )

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
        return load_default_backend()

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
