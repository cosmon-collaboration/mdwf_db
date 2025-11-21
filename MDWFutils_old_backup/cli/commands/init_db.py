#!/usr/bin/env python3
"""
commands/init_db.py

Sub‐command "init-db": create fresh schema + disk tree
   BASE_DIR/TUNING
   BASE_DIR/ENSEMBLES
Default BASE_DIR is the current directory.
"""
import sys
from pathlib import Path
from MDWFutils.db import init_database

def register(subparsers):
    p = subparsers.add_parser(
        'init-db',
        help='Initialize the SQLite schema and create TUNING/ & ENSEMBLES/ under BASE_DIR',
        description="""
Initialize a new MDWF database and directory structure. This command:
1. Creates a new SQLite database with the required schema
2. Creates TUNING/ and ENSEMBLES/ directories under BASE_DIR
3. Sets up the initial directory structure for ensemble management

The database will track:
- Ensemble parameters and metadata
- Operation history and status
- Job parameters and results
"""
    )
    p.add_argument(
        '--base-dir',
        default='.',
        help='Root directory under which to create TUNING/ and ENSEMBLES/ (default: current directory)'
    )
    p.set_defaults(func=do_init)


def do_init(args):
    base = Path(args.base_dir).resolve()
    tuning = base / 'TUNING'
    ensembles = base / 'ENSEMBLES'

    for d in (base, tuning, ensembles):
        d.mkdir(parents=True, exist_ok=True)
        print(f"Ensured directory: {d}")

    ok = init_database(args.db_file)
    print(f"init_database returned: {ok}")
    return 0 if ok else 1