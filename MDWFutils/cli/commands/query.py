#!/usr/bin/env python3
"""
commands/query.py

Query command with two modes:
• with no -e/--ensemble: list all ensembles
• with   -e/--ensemble: show details + history for one
"""
import sys
import argparse
from MDWFutils.db import (
    list_ensembles,
    print_history,
    resolve_ensemble_identifier
)
from MDWFutils.cli.ensemble_utils import add_ensemble_argument


def register(subparsers):
    p = subparsers.add_parser(
        'query',
        help='List ensembles or show detailed info for one ensemble',
        description="""
Query the MDWF database for ensemble information.

TWO MODES:

1. List mode (no --ensemble specified):
   Shows a summary of all ensembles with ID, status, and directory.
   Use --detailed to include physics parameters and operation counts.

2. Detail mode (with --ensemble specified):
   Shows complete information for one ensemble:
   - All physics parameters (beta, masses, lattice dimensions)
   - Full operation history with timestamps and parameters
   - Job status and configuration ranges

FLEXIBLE ENSEMBLE IDENTIFICATION:
The --ensemble parameter accepts multiple formats:
  • Ensemble ID: -e 1
  • Relative path: -e ./TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64
  • Absolute path: -e /full/path/to/ensemble
  • Current directory: -e . (when run from within ensemble directory)

EXAMPLES:
  mdwf_db query                    # List all ensembles
  mdwf_db query --detailed         # List all with full details
  mdwf_db query -e 1               # Show ensemble 1 details
  mdwf_db query -e .               # Show current ensemble (when in ensemble dir)
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        '-e', '--ensemble',
        help='Show detailed information for this ensemble (ID, directory path, or "." for current directory)'
    )
    p.add_argument(
        '--detailed',
        action='store_true',
        help='In list mode, show physics parameters and operation counts for each ensemble'
    )
    p.set_defaults(func=do_query)


def do_query(args):
    if not args.ensemble:
        # 1. List mode (no --ensemble):
        #    Show all ensembles, optionally with details
        ensembles = list_ensembles(args.db_file, detailed=args.detailed)
        if not ensembles:
            print("No ensembles found")
            return 0

        for ens in ensembles:
            status_str = f"({ens['status']})"
            print(f"[{ens['id']}] {status_str} {ens['directory']}")
            
            if args.detailed:
                if ens.get('parameters'):
                    params = ', '.join([f"{k}={v}" for k, v in sorted(ens['parameters'].items())])
                    print(f"    Parameters: {params}")
                if 'operation_count' in ens:
                    print(f"    Operations: {ens['operation_count']}")
                if ens.get('description'):
                    print(f"    Description: {ens['description']}")
                print()  # blank line between detailed entries

    else:
        # 2. Detail mode (with --ensemble):
        #    Show full details + history for one ensemble
        ensemble_id, ens = resolve_ensemble_identifier(args.db_file, args.ensemble)
        if ensemble_id is None:
            print(f"ERROR: Ensemble not found: {args.ensemble}")
            return 1

        # Print ensemble details
        print(f"ID          = {ens['id']}")
        print(f"Directory   = {ens['directory']}")
        print(f"Status      = {ens['status']}")
        print(f"Created     = {ens['creation_time']}")
        if ens['description']:
            print(f"Description = {ens['description']}")
        
        # Print parameters
        if ens['parameters']:
            print("Parameters:")
            for k, v in sorted(ens['parameters'].items()):
                print(f"    {k} = {v}")
        
        # Print operation history
        print("\n=== Operation history ===")
        print_history(args.db_file, ensemble_id)

    return 0