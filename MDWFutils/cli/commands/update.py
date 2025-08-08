#!/usr/bin/env python3
"""
commands/update.py

Sub‐command "update" for mdwf_db: insert a new operation or update an existing one
in the database's operations table.  Everything except ensemble-id, type, status,
and (optionally) operation-id is passed via --params.
"""

import sys
import sqlite3
from MDWFutils.db import update_operation, resolve_ensemble_identifier

def register(subparsers):
    p = subparsers.add_parser(
        'update',
        help='Create or update an operation in the database',
        description="""
Create or update an operation record in the database. This command:
1. Records operation status and parameters
2. Tracks job execution details
3. Maintains operation history

Common operation types:
- HMC_TUNE: HMC tuning run
- HMC_PRODUCTION: HMC production run
- GLU_SMEAR: Configuration smearing
- WIT_MESON2PT: Meson measurements
- PROMOTE_ENSEMBLE: Ensemble promotion

Common parameters:
- config_start: First configuration number
- config_end: Last configuration number
- exit_code: Job exit code
- runtime: Job runtime in seconds
- slurm_job: SLURM job ID
- host: Execution hostname

Example:
  mdwf_db update -e 1 -o HMC_TUNE -s RUNNING -p "config_start=0 config_end=100"
"""
    )
    # Backward compatible legacy integer-only option
    p.add_argument(
        '--ensemble-id',
        dest='ensemble_id',
        type=int,
        required=False,
        help='[DEPRECATED] ID of the ensemble (use -e/--ensemble for flexible ID or path)'
    )
    # New flexible identifier option: ID, path, or "."
    p.add_argument(
        '-e', '--ensemble',
        dest='ensemble',
        required=False,
        help='Ensemble identifier: ID, directory path, or "." for current directory'
    )
    p.add_argument(
        '--operation-type', '-o',
        dest='operation_type',
        required=True,
        help='Type of operation (e.g. HMC_TUNE, GLU_SMEAR, WIT_MESON2PT)'
    )
    p.add_argument(
        '--status', '-s',
        dest='status',
        required=True,
        choices=['RUNNING','COMPLETED','FAILED'],
        help='Operation status: RUNNING, COMPLETED, or FAILED'
    )
    p.add_argument(
        '--operation-id', '-i',
        dest='operation_id',
        type=int,
        default=None,
        help='(Optional) ID of existing operation to update'
    )
    p.add_argument(
        '--params', '-p',
        dest='params',
        default='',
        help=('Space-separated key=val pairs for operation details. '
              'Example: "config_start=0 config_end=100 exit_code=0 runtime=3600"')
    )
    p.set_defaults(func=do_update)


def do_update(args):
    #turn the single --params string into a dict[str,str]
    param_dict = {}
    for tok in args.params.strip().split():
        if '=' not in tok:
            print(f"ERROR: bad key=val pair: '{tok}'", file=sys.stderr)
            return 1
        k, v = tok.split('=', 1)
        param_dict[k] = v

    # Resolve ensemble id from flexible identifier first, fallback to legacy --ensemble-id
    ensemble_id = None
    if getattr(args, 'ensemble', None):
        eid, _ = resolve_ensemble_identifier(args.db_file, args.ensemble)
        if eid is None:
            print(f"ERROR: Ensemble not found: {args.ensemble}", file=sys.stderr)
            return 1
        ensemble_id = eid
    elif getattr(args, 'ensemble_id', None) is not None:
        ensemble_id = args.ensemble_id
    else:
        print("ERROR: Missing ensemble identifier. Use -e/--ensemble (ID or path) or --ensemble-id.", file=sys.stderr)
        return 1

    try:
        op_id, created, msg = update_operation(
            db_file       = args.db_file,
            ensemble_id   = ensemble_id,
            operation_type= args.operation_type,
            status        = args.status,
            operation_id  = args.operation_id,
            params        = param_dict or None
        )
    except sqlite3.OperationalError as e:
        print(f"ERROR: SQLite error: {e}", file=sys.stderr)
        return 1

    if op_id is None:
        # ensemble not found (or other error from helper)
        print(f"ERROR: {msg}", file=sys.stderr)
        return 1

    verb = "Created" if created else "Updated"
    print(f"{verb} operation {op_id}: {msg}")
    return 0