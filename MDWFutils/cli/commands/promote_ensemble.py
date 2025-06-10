#!/usr/bin/env python3
"""
commands/promote-ensemble.py

Sub‐command “promote-ensemble”: move a TUNING ensemble under ENSEMBLES and update its status.
Default BASE_DIR is the current directory.
"""
import shutil
import subprocess
import sys
from pathlib import Path
from MDWFutils.db import get_ensemble_details, update_ensemble

def register(subparsers):
    p = subparsers.add_parser(
        'promote-ensemble',
        help='Move a TUNING ensemble into PRODUCTION (ENSEMBLES/)'
    )
    p.add_argument('--ensemble-id','-e', type=int, required=True,
                   help='Which ensemble to promote')
    p.add_argument('--base-dir',
                   default='.',
                   help='Root under which TUNING/ and ENSEMBLES/ live (default: CWD)')
    p.add_argument('--force', action='store_true',
                   help='Skip the “Proceed?” prompt')
    p.set_defaults(func=do_promote)


def do_promote(args):
    info = get_ensemble_details(args.db_file, args.ensemble_id)
    if not info:
        print(f"ERROR: no ensemble ID={args.ensemble_id}")
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
    print(f"Promote ensemble {args.ensemble_id}:")
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
        args.ensemble_id,
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
        '--ensemble-id', str(args.ensemble_id),
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