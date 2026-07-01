#!/usr/bin/env python3
"""Initialize directories and verify database connectivity."""

import argparse
from pathlib import Path

from ..runtime import load_default_backend


def register(subparsers):
    p = subparsers.add_parser(
        'init-db',
        help='Create TUNING/ and ENSEMBLES/ directories and verify DB connection',
        description='Ensure local directory structure exists and that the configured database is reachable.'
    )
    p.add_argument('--base-dir', default='.', help='Root directory for TUNING/ and ENSEMBLES/')
    p.set_defaults(func=do_init)


def do_init(args):
    base = Path(args.base_dir).resolve()
    for sub in ('TUNING', 'ENSEMBLES'):
        path = base / sub
        path.mkdir(parents=True, exist_ok=True)
        print(f"Ensured directory: {path}")

    backend = load_default_backend(validate_connection=True, ensure_indexes=True)
    # Trigger a simple call to ensure connection/indexes are ready
    backend.list_ensembles(detailed=False)
    print("Database connection OK")
    return 0
