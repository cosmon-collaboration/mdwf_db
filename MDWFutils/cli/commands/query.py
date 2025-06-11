#!/usr/bin/env python3
"""
commands/query.py

'mdwf_db query':
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
        help='List ensembles or show details + history for one',
        description="""
Query the MDWF database for ensemble information. This command has two modes:

1. List mode (no --ensemble-id):
   - Shows a summary of all ensembles
   - With --detailed, includes parameters and operation counts

2. Detail mode (with --ensemble-id):
   - Shows full ensemble parameters
   - Lists all operations and their status
   - Shows operation history and results

The output includes:
- Ensemble parameters (beta, masses, lattice size, etc.)
- Operation history (HMC, smearing, measurements)
- Job status and results
- Configuration ranges and counts
"""
    )
    p.add_argument(
        '-e', '--ensemble-id',
        type=int,
        default=None,
        help='Show detailed information and history for this ensemble ID'
    )
    p.add_argument(
        '--detailed',
        action='store_true',
        help='In list mode, show full parameters and operation counts for each ensemble'
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