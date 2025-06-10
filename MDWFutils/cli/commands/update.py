#!/usr/bin/env python3
"""
commands/update.py

Sub‐command “update” for mdwf_db: insert a new operation or update an existing one
in the database’s operations table.  Everything except ensemble-id, type, status,
and (optionally) operation-id is passed via --params.
"""

import sys
import sqlite3
from MDWFutils.db import update_operation

def register(subparsers):
    p = subparsers.add_parser(
        'update',
        help='Create or update an operation in the DB'
    )
    p.add_argument(
        '--ensemble-id', '-e',
        dest='ensemble_id',
        type=int,
        required=True,
        help='ID of the ensemble this operation belongs to'
    )
    p.add_argument(
        '--operation-type', '-o',
        dest='operation_type',
        required=True,
        help='Operation type (e.g. HMC_TUNE, GLU_SMEAR, etc.)'
    )
    p.add_argument(
        '--status', '-s',
        dest='status',
        required=True,
        choices=['RUNNING','COMPLETED','FAILED'],
        help='New status of the operation'
    )
    p.add_argument(
        '--operation-id', '-i',
        dest='operation_id',
        type=int,
        default=None,
        help='(optional) existing operation ID to update in place'
    )
    p.add_argument(
        '--params', '-p',
        dest='params',
        default='',
        help=(
            'Space‐separated list of key=val pairs; all fields (e.g. '
            'config_start=0 config_end=100 exit_code=0 runtime=3600 '
            'slurm_job=123 host=foo) must live here.'
        )
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

    try:
        op_id, created, msg = update_operation(
            db_file       = args.db_file,
            ensemble_id   = args.ensemble_id,
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