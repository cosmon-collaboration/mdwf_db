#!/usr/bin/env python3
"""List ensembles or show details using the new backend."""

import argparse

from ..ensemble_utils import resolve_ensemble_from_args, get_backend_for_args


def register(subparsers):
    p = subparsers.add_parser(
        'query',
        help='List ensembles or show details for one ensemble',
        description='List ensembles (default) or show detailed information for a specific ensemble.'
    )
    p.add_argument('-e', '--ensemble', help='Ensemble identifier for detail view')
    p.add_argument('--detailed', action='store_true', help='Include physics/configuration fields in list view')
    p.add_argument('--sort-by-id', action='store_true', help='Sort list output by ensemble ID')
    p.add_argument('--dir', action='store_true', help='Only print directory path in detail view')
    p.set_defaults(func=do_query)


def do_query(args):
    backend = get_backend_for_args(args)

    if not args.ensemble:
        ensembles = backend.list_ensembles(detailed=args.detailed)
        if not ensembles:
            print('No ensembles found')
            return 0

        rows = []
        for ens in ensembles:
            row = [
                ens.get('ensemble_id') or ens.get('id'),
                ens.get('status'),
                ens.get('nickname'),
                ens.get('directory'),
            ]
            if args.detailed:
                physics = ens.get('physics', {})
                cfg = ens.get('configurations', {})
                row.extend([
                    physics.get('beta'),
                    physics.get('b'),
                    physics.get('Ls'),
                    cfg.get('first'),
                    cfg.get('last'),
                ])
            rows.append(row)

        if args.sort_by_id:
            rows.sort(key=lambda r: r[0])

        headers = ['ID', 'Status', 'Nickname', 'Directory']
        if args.detailed:
            headers.extend(['beta', 'b', 'Ls', 'cfg_first', 'cfg_last'])
        _print_table(headers, rows)
        return 0

    ensemble_id, ensemble = resolve_ensemble_from_args(args)
    if not ensemble:
        return 1

    if args.dir:
        print(ensemble['directory'])
        return 0

    print(f"ID: {ensemble_id}")
    print(f"Directory: {ensemble['directory']}")
    print(f"Status: {ensemble.get('status')}")
    if ensemble.get('nickname'):
        print(f"Nickname: {ensemble['nickname']}")
    if ensemble.get('description'):
        print(f"Description: {ensemble['description']}")

    physics = ensemble.get('physics', {})
    if physics:
        print('\nPhysics parameters:')
        for key in ['beta', 'b', 'Ls', 'ml', 'ms', 'mc', 'L', 'T']:
            if key in physics:
                print(f"  {key} = {physics[key]}")

    cfg = ensemble.get('configurations', {})
    if cfg:
        print('\nConfigurations:')
        for key in ['first', 'last', 'increment', 'total']:
            if key in cfg and cfg[key] is not None:
                print(f"  {key} = {cfg[key]}")

    print('\nOperations:')
    ops = backend.list_operations(ensemble_id)
    if not ops:
        print('  (none)')
    else:
        rows = []
        for op in ops:
            rows.append([
                op.get('operation_id'),
                op.get('operation_type'),
                op.get('status'),
                op.get('timing', {}).get('update_time'),
                op.get('slurm', {}).get('job_id'),
            ])
        _print_table(['ID', 'Type', 'Status', 'Updated', 'SLURM'], rows)

    return 0


def _print_table(headers, rows):
    widths = [len(str(h)) for h in headers]
    for row in rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(str(value)))
    header_line = "  ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers))
    print(header_line)
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(headers))))
