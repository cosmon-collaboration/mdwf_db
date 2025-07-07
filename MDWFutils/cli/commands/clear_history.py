#!/usr/bin/env python3
"""
commands/clear_history.py

Clear all operation history for a specific ensemble while preserving the ensemble record.
"""
import sys
from MDWFutils.db import clear_ensemble_history
from ..ensemble_utils import resolve_ensemble_from_args
import argparse

def register(subparsers):
    p = subparsers.add_parser(
        'clear-history',
        help='Clear operation history while preserving ensemble record',
        description="""
Clear all operation history for a specific ensemble.

WHAT THIS DOES:
• Removes all operations and their parameters from the database
• Preserves the ensemble record and its physics parameters
• Useful for cleaning up failed runs or resetting workflow state

WHAT IS PRESERVED:
• Ensemble record (ID, directory, status, creation time)
• Physics parameters (beta, masses, lattice dimensions)
• Description and other ensemble metadata

WHAT IS REMOVED:
• All operation records (HMC, smearing, measurements, etc.)
• Operation parameters (config ranges, job IDs, exit codes)
• Operation timestamps and status information

WARNING: This operation is irreversible. All job history and operation 
status will be permanently deleted.

FLEXIBLE ENSEMBLE IDENTIFICATION:
The --ensemble parameter accepts multiple formats:
  • Ensemble ID: -e 1
  • Relative path: -e ./TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64
  • Absolute path: -e /full/path/to/ensemble
  • Current directory: -e . (when run from within ensemble directory)

EXAMPLES:
  mdwf_db clear-history -e 1           # Clear history for ensemble 1
  mdwf_db clear-history -e . --force   # Clear current ensemble (no prompt)
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        '-e', '--ensemble',
        required=True,
        help='Ensemble to clear history for (ID, directory path, or "." for current directory)'
    )
    p.add_argument(
        '--force',
        action='store_true',
        help='Skip confirmation prompt and clear history immediately'
    )
    p.set_defaults(func=do_clear_history)

def do_clear_history(args):
    # Resolve ensemble identifier
    ensemble_id, ens = resolve_ensemble_from_args(args)
    if not ens:
        return 1
    
    # Check if there's any history to clear
    operation_count = ens.get('operation_count', 0)
    if operation_count == 0:
        print(f"Ensemble {ensemble_id} has no operation history to clear")
        return 0
    
    # Show what will be cleared
    print(f"Ensemble {ensemble_id}: {ens['directory']}")
    print(f"Found {operation_count} operation(s) to clear")
    
    # Confirmation prompt (unless --force)
    if not args.force:
        print("\nWARNING: This will permanently delete all operation history.")
        print("The ensemble record itself will be preserved.")
        response = input("Continue? (y/N): ")
        if response.lower() not in ('y', 'yes'):
            print("Operation cancelled")
            return 0
    
    # Clear the history
    deleted_count, success = clear_ensemble_history(args.db_file, ensemble_id)
    
    if success:
        print(f"Successfully cleared {deleted_count} operation(s) from ensemble {ensemble_id}")
        return 0
    else:
        print(f"ERROR: Failed to clear history for ensemble {ensemble_id}", file=sys.stderr)
        return 1 