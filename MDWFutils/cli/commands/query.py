#!/usr/bin/env python3
"""
commands/query.py

‘mdwf_db query’:
  • with no -e/--ensemble-id: list all ensembles
  • with   -e/--ensemble-id: show details + history for one
"""
import sys
from MDWFutils.db import (
    list_ensembles,
    get_ensemble_details,
    print_history
)


def register(subparsers):
    p = subparsers.add_parser(
        'query',
        help='List ensembles or show details + history for one'
    )
    p.add_argument(
        '-e', '--ensemble-id',
        type=int,
        default=None,
        help='If given, show info and history for that ensemble'
    )
    p.add_argument(
        '--detailed',
        action='store_true',
        help='In list mode, also show parameters + op counts'
    )
    p.set_defaults(func=do_query)


def do_query(args):
    # ---- List mode ----
    if args.ensemble_id is None:
        ens_list = list_ensembles(args.db_file, detailed=args.detailed)
        if not ens_list:
            print("No ensembles found")
            return 0

        for e in ens_list:
            print(f"[{e['id']}] ({e['status']}) {e['directory']}")
            if args.detailed:
                for k, v in e.get('parameters', {}).items():
                    print(f"    {k} = {v}")
                print(f"    ops = {e.get('operation_count',0)}")
        return 0

    # ---- Detail mode ----
    ens = get_ensemble_details(args.db_file, args.ensemble_id)
    if not ens:
        print(f"ERROR: ensemble not found: {args.ensemble_id}", file=sys.stderr)
        return 1

    print(f"ID          = {ens['id']}")
    print(f"Directory   = {ens['directory']}")
    print(f"Status      = {ens['status']}")
    print(f"Created     = {ens['creation_time']}")
    if ens.get('description'):
        print(f"Description = {ens['description']}")
    print("Parameters:")
    for k, v in ens.get('parameters', {}).items():
        print(f"    {k} = {v}")

    print("\n=== Operation history ===")
    print_history(args.db_file, args.ensemble_id)
    return 0