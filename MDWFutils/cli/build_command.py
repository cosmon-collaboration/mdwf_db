"""Build command scaffolding."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple

from ..backends import get_backend
from ..build.registry import get_build_builder, get_build_schema
from ..build.schema import parse_build_params
from ..build.site import resolve_site_profile
from ..exceptions import ConnectionError, MDWFError
from .args import add_output_file_arg
from .components import BuildScriptGenerator, EnsembleResolver
from .help_generator import HelpGenerator
from ..jobs.schema import _deduplicate_schema


def _load_backend():
    connection = os.getenv("MDWF_DB_URL")
    if not connection:
        raise ConnectionError("MDWF_DB_URL environment variable not set")
    return get_backend(connection)


def write_build_artifact(content: str, context: Dict, output_file: Optional[str] = None) -> Path:
    if output_file:
        path = Path(output_file)
    else:
        target_dir = Path(context["_output_dir"])
        prefix = context.get("_output_prefix", "build_output")
        suffix = context.get("_output_suffix", ".sh")
        path = target_dir / f"{prefix}{suffix}"

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    if context.get("_executable"):
        path.chmod(path.stat().st_mode | 0o111)
    return path


class BuildCommand:
    """Generate build scripts from build context builders."""

    type_name: str = ""
    help: str = ""
    requires_physics_ensemble: bool = False
    uses_site_ensemble_default: bool = False

    def __init__(self, backend=None):
        self._backend_override = backend
        self.help_gen = HelpGenerator()

    def register(self, subparsers):
        parser = subparsers.add_parser(
            self.type_name.replace("_", "-") if False else self._command_name(),
            help=self.help,
            formatter_class=__import__("argparse").RawDescriptionHelpFormatter,
        )
        self._add_arguments(parser)
        parser.set_defaults(func=self.execute)

    def _command_name(self) -> str:
        return self.type_name

    def _add_arguments(self, parser):
        if self.requires_physics_ensemble or not self.uses_site_ensemble_default:
            parser.add_argument("-e", "--ensemble", help="Ensemble identifier")
        else:
            parser.add_argument(
                "-e",
                "--ensemble",
                default="software",
                help="Ensemble identifier (default: site software ensemble nickname)",
            )
        parser.add_argument("-p", "--params", default="", help="Build params KEY=VALUE ...")
        add_output_file_arg(parser)
        parser.add_argument("--params", dest="show_params", action="store_true", help="Show parameter schema")
        self.add_custom_args(parser)

    def add_custom_args(self, parser):
        """Override in subclasses."""

    def execute(self, args):
        if getattr(args, "show_params", False):
            return self._print_params()
        try:
            backend = self._backend_override or _load_backend()
            build_params = parse_build_params(args.params or "")
            ensemble_id, ensemble = self._resolve_ensemble(backend, args)
            generator = BuildScriptGenerator(backend)
            cmd_line = " ".join(sys.argv)
            content, context = generator.generate(
                self.type_name,
                ensemble_id,
                build_params,
                ensemble=ensemble,
                command_line=cmd_line,
            )
            path = write_build_artifact(content, context, getattr(args, "output_file", None))
            print(f"Generated build artifact: {path}")
            self.after_generate(backend, ensemble_id, ensemble, context, args)
            return 0
        except MDWFError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1

    def after_generate(self, backend, ensemble_id, ensemble, context, args):
        """Hook for register-paths etc."""

    def _resolve_ensemble(self, backend, args) -> Tuple[int, Dict]:
        identifier = getattr(args, "ensemble", None)
        if not identifier and self.uses_site_ensemble_default:
            identifier = "software"
        if not identifier:
            raise MDWFError("Ensemble identifier required (-e)")
        resolver = EnsembleResolver(backend)
        return resolver.resolve(identifier)

    def _print_params(self) -> int:
        schema = get_build_schema(self.type_name) or []
        output = self.help_gen.format_params_detailed([], schema, command_name=self.type_name)
        print(output)
        return 0
