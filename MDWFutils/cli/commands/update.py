#!/usr/bin/env python3
"""Create or update operation records via the backend."""

import argparse
import sys
from getpass import getuser

from ..ensemble_utils import resolve_ensemble_from_args, get_backend_for_args


def register(subparsers):
    p = subparsers.add_parser(
        'update',
        help='Create or update an operation record',
        description='Create a new operation entry or update an existing one (by ID or SLURM job ID).'
    )
    p.add_argument('-e', '--ensemble', required=True, help='Ensemble identifier')
    p.add_argument('--operation-type', '-o', required=True, help='Operation type label')
    p.add_argument('--status', '-s', required=True, help='Operation status')
    p.add_argument('--operation-id', '-i', type=int, help='Existing operation ID to update')
    p.add_argument('-p', '--params', default='', help='Space separated key=val pairs for metadata')
    p.add_argument('-u', '--user', default=None, help='Override username associated with the operation')
    p.set_defaults(func=do_update)


def do_update(args):
    backend = get_backend_for_args(args)
    ensemble_id, _ = resolve_ensemble_from_args(args)
    if ensemble_id is None:
        return 1

    param_dict = _parse_params(args.params)
    status = args.status

    if args.operation_id:
        payload = _flatten_update_fields(param_dict)
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
    backend.add_operation(
        ensemble_id,
        operation_type=args.operation_type,
        status=status,
        user=user,
        **_extract_operation_kwargs(param_dict),
    )
    print("Created operation entry")
    return 0


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
