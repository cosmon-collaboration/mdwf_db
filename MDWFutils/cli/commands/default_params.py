#!/usr/bin/env python3
"""Manage default parameter variants stored in the database."""

import argparse
import sys

from ..components import ParameterManager
from ..ensemble_utils import get_backend_for_args, resolve_ensemble_from_args


def register(subparsers):
    p = subparsers.add_parser(
        "default_params",
        help="Manage default parameter variants for ensembles",
        description="Store and inspect default parameter variants directly in the database.",
    )
    sub = p.add_subparsers(dest="action", required=True)

    show = sub.add_parser("show", help="Show parameters for a variant")
    _add_common_args(show)

    set_cmd = sub.add_parser("set", help="Set parameters for a variant")
    _add_common_args(set_cmd)
    set_cmd.add_argument(
        "--input", default="", help="Input parameter string (key=val ...)"
    )
    set_cmd.add_argument("--job", default="", help="Job parameter string (key=val ...)")

    delete = sub.add_parser("delete", help="Delete a stored variant")
    _add_common_args(delete)
    delete.add_argument("--force", action="store_true", help="Skip confirmation prompt")

    list_cmd = sub.add_parser("list", help="List all variants for an ensemble")
    list_cmd.add_argument("-e", "--ensemble", required=True)
    list_cmd.add_argument("--command", help="Filter by command name")

    import_cmd = sub.add_parser(
        "import",
        help="Import defaults from mdwf_default_params.yaml file",
    )
    import_cmd.add_argument("-e", "--ensemble", required=True)
    import_cmd.add_argument(
        "--command", required=True, help="Command name (e.g. hmc-script)"
    )
    import_cmd.add_argument("--variant", required=True, help="Variant name (e.g. gpu)")

    p.set_defaults(func=do_default_params)


def _add_common_args(parser):
    parser.add_argument("-e", "--ensemble", required=True)
    parser.add_argument(
        "--command", required=True, help="Command name (e.g. hmc-script)"
    )
    parser.add_argument("--variant", required=True, help="Variant name (e.g. gpu)")


def do_default_params(args):
    backend = get_backend_for_args(args)
    ensemble_id, ensemble = resolve_ensemble_from_args(args)
    if not ensemble:
        return 1

    param_manager = ParameterManager(backend)

    if args.action == "list":
        defaults = param_manager.list_ensemble_defaults(ensemble_id, args.command)
        if not defaults:
            print(f"No saved defaults for ensemble {ensemble_id}")
            return 0
        print(f"Saved defaults for ensemble {ensemble_id}:")
        for entry in defaults:
            cmd = entry.get("command", "?")
            var = entry.get("variant", "?")
            inp = entry.get("input_params", {})
            job = entry.get("job_params", {})
            print(f"  {cmd} / {var}:")
            if inp:
                print(f"    input: {inp}")
            if job:
                print(f"    job: {job}")
        return 0

    command = args.command
    variant = args.variant

    if args.action == "show":
        defaults = param_manager.load_ensemble_defaults(ensemble_id, command, variant)
        input_params = defaults.get("input_params", {})
        job_params = defaults.get("job_params", {})
        if not input_params and not job_params:
            print(f"No saved defaults for {command} (variant: {variant})")
            return 0
        print(f"Defaults for {command} (variant: {variant}):")
        if input_params:
            print("  Input parameters:")
            for k, v in input_params.items():
                print(f"    {k} = {v}")
        if job_params:
            print("  Job parameters:")
            for k, v in job_params.items():
                print(f"    {k} = {v}")
        return 0

    if args.action == "delete":
        if not args.force:
            print(f"Delete defaults for {command} (variant: {variant})? [y/N]", end=" ")
            try:
                resp = input().strip().lower()
            except EOFError:
                return 0
            if resp not in ("y", "yes"):
                print("Aborted")
                return 0
        ok = param_manager.delete_ensemble_defaults(ensemble_id, command, variant)
        if ok:
            print(f"Deleted defaults for {command} (variant: {variant})")
        else:
            print(f"No defaults found for {command} (variant: {variant})")
        return 0

    if args.action == "set":
        input_params = ParameterManager.parse(args.input)
        job_params = ParameterManager.parse(args.job)
        param_manager.save_ensemble_defaults(
            ensemble_id, command, variant, input_params, job_params
        )
        print(f"Set defaults for {command} (variant: {variant})")
        return 0

    if args.action == "import":
        return _import_yaml(backend, ensemble_id, ensemble, command, variant)

    print(f"ERROR: Unknown action {args.action}", file=sys.stderr)
    return 1


def _import_yaml(backend, ensemble_id, ensemble, command, variant):
    """Import defaults from mdwf_default_params.yaml file."""
    from pathlib import Path

    import yaml

    ensemble_dir = Path(ensemble.get("directory", ""))
    config_path = ensemble_dir / "mdwf_default_params.yaml"

    if not config_path.exists():
        print(
            f"ERROR: No mdwf_default_params.yaml found in {ensemble_dir}",
            file=sys.stderr,
        )
        return 1

    with open(config_path) as f:
        config = yaml.safe_load(f)

    if not config:
        print("ERROR: YAML file is empty", file=sys.stderr)
        return 1

    # Map command name to operation type in YAML
    # The YAML uses operation_type keys like "HMC", "GLU", "WIT_MRES", etc.
    # We need to figure out which key matches the command.
    op_map = {
        "hmc-script": "HMC",
        "smear-script": "GLU",
        "wflow-script": "GLU",
        "mres-script": "WIT_MRES",
        "mres-mq-script": "WIT_MRES_MQ",
        "meson2pt-script": "WIT_MESON2PT",
        "zv-script": "WIT_Zv",
    }
    op_type = op_map.get(command, command.upper())

    # Try to find the matching operation in the YAML
    if op_type not in config:
        # Try partial match
        matching = [k for k in config.keys() if op_type in k or k in op_type]
        if matching:
            op_type = matching[0]
        else:
            print(
                f"ERROR: No operation type '{op_type}' found in YAML. Available: {list(config.keys())}",
                file=sys.stderr,
            )
            return 1

    op_config = config[op_type]

    # Extract params from the YAML operation config
    input_params = {}
    job_params = {}

    if isinstance(op_config, dict):
        # Check for 'params' or 'input_params'/'job_params' keys
        if "input_params" in op_config:
            inp = op_config["input_params"]
            if isinstance(inp, str):
                input_params = ParameterManager.parse(inp)
            elif isinstance(inp, dict):
                input_params = {k: str(v) for k, v in inp.items()}
        if "job_params" in op_config:
            job = op_config["job_params"]
            if isinstance(job, str):
                job_params = ParameterManager.parse(job)
            elif isinstance(job, dict):
                job_params = {k: str(v) for k, v in job.items()}
        if "params" in op_config:
            params = op_config["params"]
            if isinstance(params, str):
                parsed = ParameterManager.parse(params)
                # Heuristic: merge into input/job based on known param names
                input_params.update(parsed)
            elif isinstance(params, dict):
                input_params.update({k: str(v) for k, v in params.items()})
    elif isinstance(op_config, str):
        # Plain string of key=value pairs
        input_params = ParameterManager.parse(op_config)

    if not input_params and not job_params:
        print(
            f"ERROR: Could not extract parameters from '{op_type}' in YAML",
            file=sys.stderr,
        )
        return 1

    pm = ParameterManager(backend)
    pm.save_ensemble_defaults(ensemble_id, command, variant, input_params, job_params)
    print(f"Imported defaults from YAML for {command} (variant: {variant})")
    print(f"  Input params: {input_params}")
    print(f"  Job params: {job_params}")
    return 0
