#!/usr/bin/env python3
"""Manage default parameter variants stored in the database."""

import argparse
import sys

from ..ensemble_utils import resolve_ensemble_from_args, get_backend_for_args
from ..components import ParameterManager


def register(subparsers):
    p = subparsers.add_parser(
        'default_params',
        help='Manage default parameter variants for ensembles',
        description='Store and inspect default parameter variants directly in the database.'
    )
    sub = p.add_subparsers(dest='action', required=True)

    show = sub.add_parser('show', help='Show parameters for a variant')
    _add_common_args(show)

    set_cmd = sub.add_parser('set', help='Set parameters for a variant')
    _add_common_args(set_cmd)
    set_cmd.add_argument('--input', default='', help='Input parameter string (key=val ...)')
    set_cmd.add_argument('--job', default='', help='Job parameter string (key=val ...)')

    delete = sub.add_parser('delete', help='Delete a stored variant')
    _add_common_args(delete)
    delete.add_argument('--force', action='store_true', help='Skip confirmation prompt')

    list_cmd = sub.add_parser('list', help='List all variants for an ensemble')
    list_cmd.add_argument('-e', '--ensemble', required=True)
    list_cmd.add_argument('--job-type', help='Filter by job type')

    p.set_defaults(func=do_default_params)


def _add_common_args(parser):
    parser.add_argument('-e', '--ensemble', required=True)
    parser.add_argument('--job-type', required=True)
    parser.add_argument('--variant', required=True)


def do_default_params(args):
    backend = get_backend_for_args(args)
    ensemble_id, ensemble = resolve_ensemble_from_args(args)
    if not ensemble:
        return 1

    param_manager = ParameterManager(backend)

    if args.action == 'list':
        defaults = param_manager.list_all_defaults(ensemble_id, args.job_type)
        if not defaults:
            print('No default parameter variants stored')
            return 0
        
        # Group by job_type
        by_job_type = {}
        for item in defaults:
            jt = item['job_type']
            if jt not in by_job_type:
                by_job_type[jt] = []
            by_job_type[jt].append(item)
        
        # Print formatted output
        ensemble_name = ensemble.get('nickname') or f"ID {ensemble_id}"
        print(f"\nDefault parameters for ensemble '{ensemble_name}' (ID: {ensemble_id}):\n")
        
        for job_type in sorted(by_job_type.keys()):
            print(f"{job_type}:")
            for item in sorted(by_job_type[job_type], key=lambda x: x['variant']):
                variant = item['variant']
                input_params = item['input_params']
                job_params = item['job_params']
                
                # Truncate long parameter strings for display
                input_display = input_params[:60] + "..." if len(input_params) > 60 else input_params
                job_display = job_params[:60] + "..." if len(job_params) > 60 else job_params
                
                print(f"  - {variant}")
                if input_params:
                    print(f"    Input:  {input_display}")
                if job_params:
                    print(f"    Job:    {job_display}")
            print()
        return 0

    job_type = args.job_type
    variant = args.variant

    if args.action == 'show':
        params = backend.get_default_params(ensemble_id, job_type, variant)
        if not params['input_params'] and not params['job_params']:
            print('No params stored')
            return 0
        
        print(f"\nDefault parameters: {job_type} (variant: {variant})\n")
        print("Input Parameters:")
        if params['input_params']:
            for param in params['input_params'].split():
                if '=' in param:
                    key, value = param.split('=', 1)
                    print(f"  {key}: {value}")
                else:
                    print(f"  {param}")
        else:
            print("  (none)")
        
        print("\nJob Parameters:")
        if params['job_params']:
            for param in params['job_params'].split():
                if '=' in param:
                    key, value = param.split('=', 1)
                    print(f"  {key}: {value}")
                else:
                    print(f"  {param}")
        else:
            print("  (none)")
        return 0

    if args.action == 'delete':
        if not args.force:
            response = input(f"Delete default parameters for {job_type}.{variant}? (y/N): ")
            if response.lower() != 'y':
                print("Cancelled")
                return 0
        
        if param_manager.delete_defaults(ensemble_id, job_type, variant):
            print(f'Deleted default parameters for {job_type}.{variant}')
            return 0
        print('Nothing to delete')
        return 0

    if args.action == 'set':
        input_params = args.input.strip()
        job_params = args.job.strip()
        backend.set_default_params(ensemble_id, job_type, variant, input_params, job_params)
        print(f"Stored defaults for {job_type}.{variant}")
        return 0

    print(f"ERROR: Unknown action {args.action}", file=sys.stderr)
    return 1
