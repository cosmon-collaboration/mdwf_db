#!/usr/bin/env python3
"""Manage default parameter variants stored in the database."""

import argparse

from ..ensemble_utils import resolve_ensemble_from_args, get_backend_for_args


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

    list_cmd = sub.add_parser('list', help='List all variants for an ensemble')
    list_cmd.add_argument('-e', '--ensemble', required=True)

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

    if args.action == 'list':
        defaults = ensemble.get('default_params', {})
        if not defaults:
            print('No default parameter variants stored')
            return 0
        for job_type, variants in sorted(defaults.items()):
            print(f"{job_type}:")
            for variant in sorted(variants.keys()):
                print(f"  - {variant}")
        return 0

    job_type = args.job_type
    variant = args.variant

    if args.action == 'show':
        params = backend.get_default_params(ensemble_id, job_type, variant)
        if not params['input_params'] and not params['job_params']:
            print('No params stored')
            return 0
        print('Input params:', params['input_params'] or '(none)')
        print('Job params:', params['job_params'] or '(none)')
        return 0

    if args.action == 'delete':
        if backend.delete_default_params(ensemble_id, job_type, variant):
            print('Deleted default parameters')
            return 0
        print('Nothing to delete')
        return 0

    if args.action == 'set':
        input_params = args.input.strip()
        job_params = args.job.strip()
        backend.set_default_params(ensemble_id, job_type, variant, input_params, job_params)
        print(f"Stored defaults for {job_type}.{variant}")
        return 0

    print(f"ERROR: Unknown action {args.action}")
    return 1
