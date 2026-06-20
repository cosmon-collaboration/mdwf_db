#!/usr/bin/env python3
"""Create or update operation records via the backend."""

import argparse
import sys
from getpass import getuser

from ..ensemble_utils import resolve_ensemble_from_args, get_backend_for_args


def register(subparsers):
    p = subparsers.add_parser(
        'update',
        help='Create or update an operation record, or update ensemble properties',
        description="""
Create or update an operation record (requires -o/--operation-type and -s/--status),
or update ensemble document fields (omit -o and -s).

ENSEMBLE UPDATES (omit -o and -s):
  mdwf_db update -e . -p C_therm=500
  mdwf_db update -e . -p configurations.thermalized=500

HMC paths (stored on the ensemble; shown by mdwf_db status -e .):
  These are not saved automatically by mdwf_db hmc-script; set them explicitly or
  use build grid ... --register-paths for exec_path after a Grid GPU/CPU build.

  mdwf_db update -e . -p hmc_paths.exec_path=/path/to/Nf2p1p1
  mdwf_db update -e . -p hmc_paths.bind_script_gpu=/path/to/bind.sh
  mdwf_db update -e . -p hmc_paths.bind_script_cpu=/path/to/bind_cpu.sh

  mdwf_db build grid gpu -e . --register-paths   # sets hmc_paths.exec_path

OPERATION UPDATES (require both -o and -s):
  mdwf_db update -e . -o HMC_tepid -s RUNNING -p slurm_job_id=12345
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument('-e', '--ensemble', required=True, help='Ensemble identifier')
    p.add_argument('--operation-type', '-o', help='Operation type label (required for operation updates)')
    p.add_argument('--status', '-s', help='Operation status (required for operation updates)')
    p.add_argument('--operation-id', '-i', type=int, help='Existing operation ID to update')
    p.add_argument(
        '-p', '--params', default='',
        help='Key=value pairs. Ensemble examples: C_therm=500, hmc_paths.exec_path=/path/to/Nf2p1p1',
    )
    p.add_argument('-u', '--user', default=None, help='Override username associated with the operation')
    p.set_defaults(func=do_update)


def do_update(args):
    backend = get_backend_for_args(args)
    ensemble_id, ensemble = resolve_ensemble_from_args(args)
    if ensemble_id is None:
        return 1

    param_dict = _parse_params(args.params)
    
    # If operation-type and status are not provided, treat as ensemble update
    if not args.operation_type and not args.status:
        return _update_ensemble(backend, ensemble_id, ensemble, param_dict)
    
    # Otherwise, proceed with operation update (require both)
    if not args.operation_type or not args.status:
        print("ERROR: Both --operation-type and --status are required for operation updates", file=sys.stderr)
        print("Hint: Omit both to update ensemble properties instead", file=sys.stderr)
        return 1
    
    status = args.status

    if args.operation_id:
        payload = _flatten_update_fields(param_dict)
        _apply_terminal_timing(payload, status, param_dict)
        updated = backend.update_operation_by_id(args.operation_id, status, **payload)
        if not updated:
            print(f"ERROR: Operation {args.operation_id} not found", file=sys.stderr)
            return 1
        print(f"Updated operation {args.operation_id}")
        return 0

    slurm_job_id = param_dict.pop('slurm_job_id', param_dict.pop('slurm_job', None))
    if slurm_job_id:
        payload = _flatten_update_fields(param_dict)
        payload['slurm.job_id'] = slurm_job_id
        updated = backend.update_operation_by_slurm_id(
            slurm_job_id, 
            status,
            ensemble_id,
            args.operation_type,
            **payload
        )
        if updated:
            print(f"Updated SLURM job {slurm_job_id}")
            return 0
        # fall through to creation if not found
        param_dict['slurm_job_id'] = slurm_job_id

    user = args.user or getuser()
    operation_id = backend.add_operation(
        ensemble_id,
        operation_type=args.operation_type,
        status=status,
        user=user,
        **_extract_operation_kwargs(param_dict),
    )
    print(f"Created operation entry operation_id={operation_id}")
    return 0


# Aliases for ensemble update keys (e.g. -p C_therm=500)
_ENSEMBLE_UPDATE_ALIASES = {"C_therm": "configurations.thermalized"}


def _update_ensemble(backend, ensemble_id, ensemble, param_dict):
    """Update ensemble properties."""
    if not param_dict:
        print("ERROR: No properties specified for ensemble update", file=sys.stderr)
        print("Hint: Use -p key=value (e.g. -p C_therm=500 or -p hmc_paths.exec_path=/path/to/Nf2p1p1)", file=sys.stderr)
        return 1
    
    updates = {}
    cfg_updates = {}  # Collect all configuration updates first
    
    for key, value in param_dict.items():
        key = _ENSEMBLE_UPDATE_ALIASES.get(key, key)
        # Handle nested keys like "configurations.thermalized"
        if '.' in key:
            parts = key.split('.')
            if parts[0] == 'configurations':
                # Special handling for configurations dict to preserve existing fields
                if len(parts) == 2:
                    # Try to convert to int if it's a numeric field
                    if parts[1] in ('thermalized', 'first', 'last', 'increment', 'total'):
                        if isinstance(value, str) and value.lower() == 'none':
                            value = None
                        else:
                            try:
                                value = int(value)
                            except (ValueError, TypeError):
                                pass
                    elif parts[1] == 'config_list':
                        # Parse comma-separated list
                        try:
                            value = [int(x.strip()) for x in value.split(',')]
                        except ValueError:
                            pass
                    cfg_updates[parts[1]] = value
                else:
                    updates[key] = value
            else:
                updates[key] = value
        else:
            updates[key] = value
    
    # Apply all configuration updates at once
    if cfg_updates:
        current_cfg = ensemble.get('configurations', {}).copy()
        current_cfg.update(cfg_updates)
        updates['configurations'] = current_cfg
    
    try:
        backend.update_ensemble(ensemble_id, **updates)
        print(f"Updated ensemble {ensemble_id}")
        for key in updates:
            print(f"  {key}: {updates[key]}")
        return 0
    except Exception as e:
        print(f"ERROR: Failed to update ensemble: {e}", file=sys.stderr)
        return 1


_TERMINAL_STATUSES = frozenset({"COMPLETED", "FAILED", "CANCELED", "TIMEOUT"})


def _apply_terminal_timing(payload: dict, status: str, param_dict: dict) -> None:
    if status not in _TERMINAL_STATUSES:
        return
    from datetime import datetime

    payload["timing.end_time"] = datetime.utcnow()
    runtime = param_dict.get("runtime")
    if runtime is not None:
        try:
            payload["timing.runtime_seconds"] = int(runtime)
        except (TypeError, ValueError):
            pass


def _parse_params(param_string):
    result = {}
    for token in param_string.split():
        if '=' not in token:
            print(f"WARNING: ignoring malformed token '{token}'", file=sys.stderr)
            continue
        key, value = token.split('=', 1)
        result[key.strip()] = value.strip()
    return result


def _flatten_update_fields(params):
    payload = {}
    for key, value in params.items():
        if key in ('config_start', 'config_end', 'config_increment'):
            try:
                value = int(value)
            except ValueError:
                pass
            payload[f"execution.{key if key != 'config_increment' else 'config_increment'}"] = value
        elif key in ('run_dir',):
            payload[f"execution.{key}"] = value
        elif key in ('host', 'batch_script', 'output_log', 'error_log', 'exit_code', 'slurm_status'):
            payload[f"slurm.{key}"] = value
        elif key == 'slurm_job_id':
            payload['slurm.job_id'] = value
        else:
            payload[f"params.{key}"] = value
    return payload


def _extract_operation_kwargs(params):
    kwargs = {}
    exec_fields = {}
    slurm_fields = {}
    extra = {}
    for key, value in params.items():
        if key == 'slurm_job_id' or key == 'slurm_job':
            slurm_fields['slurm_job_id'] = value
        elif key in ('host', 'batch_script', 'output_log', 'error_log', 'exit_code', 'slurm_status'):
            slurm_fields[key] = value
        elif key in ('run_dir', 'config_start', 'config_end', 'config_increment'):
            if key.startswith('config_'):
                try:
                    value = int(value)
                except ValueError:
                    pass
            exec_fields[key] = value
        else:
            extra[key] = value
    kwargs.update(slurm_fields)
    kwargs.update(exec_fields)
    kwargs.update(extra)
    return kwargs
