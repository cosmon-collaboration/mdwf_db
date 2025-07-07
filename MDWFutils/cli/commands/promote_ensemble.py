#!/usr/bin/env python3
"""
commands/promote-ensemble.py

Move a TUNING ensemble to PRODUCTION status and directory.
"""
import shutil
import subprocess
import sys
import argparse
from pathlib import Path
from MDWFutils.db import update_ensemble
from ..ensemble_utils import resolve_ensemble_from_args

def register(subparsers):
    p = subparsers.add_parser(
        'promote-ensemble',
        help='Move ensemble from TUNING to PRODUCTION status',
        description="""
Move a TUNING ensemble to PRODUCTION status and directory.

WHAT THIS DOES:
• Moves ensemble directory from TUNING/ to ENSEMBLES/
• Updates ensemble status to PRODUCTION in database
• Records a PROMOTE_ENSEMBLE operation in the history
• Preserves all files and operation history

DIRECTORY MOVEMENT:
The ensemble directory is physically moved:
  FROM: TUNING/b{beta}/b{b}Ls{Ls}/mc{mc}/ms{ms}/ml{ml}/L{L}/T{T}/
  TO:   ENSEMBLES/b{beta}/b{b}Ls{Ls}/mc{mc}/ms{ms}/ml{ml}/L{L}/T{T}/

DATABASE UPDATES:
• Ensemble status: TUNING to PRODUCTION  
• Directory path: Updated to new ENSEMBLES/ location
• Operation history: PROMOTE_ENSEMBLE operation added

REQUIREMENTS:
• Ensemble must currently have TUNING status
• Target directory in ENSEMBLES/ must not already exist
• Source directory must be under TUNING/

FLEXIBLE ENSEMBLE IDENTIFICATION:
The --ensemble parameter accepts multiple formats:
  • Ensemble ID: -e 1
  • Relative path: -e ./TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64
  • Absolute path: -e /full/path/to/ensemble
  • Current directory: -e . (when run from within ensemble directory)

EXAMPLES:
  # Promote ensemble 1 to production
  mdwf_db promote-ensemble -e 1

  # Promote current ensemble (from within ensemble directory)
  mdwf_db promote-ensemble -e . --force

  # Promote with custom base directory
  mdwf_db promote-ensemble -e 1 --base-dir /scratch/lattice
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument('-e', '--ensemble', required=True,
                   help='Ensemble to promote (ID, directory path, or "." for current directory)')
    p.add_argument('--base-dir',
                   default='.',
                   help='Root directory containing TUNING/ and ENSEMBLES/ (default: current directory)')
    p.add_argument('--force', action='store_true',
                   help='Skip confirmation prompt and promote immediately')
    p.set_defaults(func=do_promote)


def do_promote(args):
    ensemble_id, info = resolve_ensemble_from_args(args)
    if not info:
        return 1
        
    if info['status'] == 'PRODUCTION':
        print("Already in PRODUCTION")
        return 0

    old_dir = Path(info['directory'])
    base    = Path(args.base_dir).resolve()
    tuning  = base / 'TUNING'
    prod    = base / 'ENSEMBLES'

    # compute the relative path under TUNING/
    try:
        rel = old_dir.relative_to(tuning)
    except Exception:
        print("ERROR: ensemble not under TUNING/")
        return 1

    new_dir = prod / rel
    print(f"Promote ensemble {ensemble_id}:")
    print(f"  from {old_dir}")
    print(f"    to {new_dir}")
    if new_dir.exists():
        print("ERROR: target path already exists:", new_dir)
        return 1

    if not args.force:
        resp = input("Proceed? (y/N) ")
        if resp.lower() not in ('y','yes'):
            print("Cancelled")
            return 0

    # do the move
    new_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(old_dir), str(new_dir))

    # update the ensembles table
    ok = update_ensemble(
        args.db_file,
        ensemble_id,
        status='PRODUCTION',
        directory=str(new_dir)
    )
    if not ok:
        print("Promotion FAILED")
        return 1

    # call existing CLI to record the PROMOTE_ENSEMBLE operation
    cmd = [
        'mdwf_db',               
        'update',
        '--db-file',    args.db_file,
        '--ensemble-id', str(ensemble_id),
        '--operation-type', 'PROMOTE_ENSEMBLE',
        '--status',        'COMPLETED',
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        return result.returncode

    sys.stdout.write(result.stdout)
    print("Promotion OK")
    return 0