#!/usr/bin/env python3
"""Move a TUNING ensemble to PRODUCTION status and directory."""

import argparse
import shutil
from pathlib import Path
from getpass import getuser

from ..ensemble_utils import resolve_ensemble_from_args, get_backend_for_args


def register(subparsers):
    p = subparsers.add_parser(
        'promote-ensemble',
        help='Move ensemble from TUNING to PRODUCTION status',
        description='Move a TUNING ensemble directory under ENSEMBLES/ and update DB status.'
    )
    p.add_argument('-e', '--ensemble', required=True,
                   help='Ensemble to promote (ID, directory path, or "." for current directory)')
    p.add_argument('--base-dir', default='.',
                   help='Root directory containing TUNING/ and ENSEMBLES/ (default: auto-discover)')
    p.add_argument('--force', action='store_true', help='Skip confirmation prompt')
    p.set_defaults(func=do_promote)


def do_promote(args):
    backend = get_backend_for_args(args)
    ensemble_id, ensemble = resolve_ensemble_from_args(args)
    if not ensemble:
        return 1

    status = ensemble.get('status')
    if status == 'PRODUCTION':
        print('Ensemble already in PRODUCTION')
        return 0
    if status != 'TUNING':
        print(f"ERROR: Ensemble must be in TUNING status, not {status}")
        return 1

    old_dir = Path(ensemble['directory']).resolve()
    base, tuning_dir, prod_dir = _discover_base_dirs(args.base_dir)
    if base is None:
        return 1

    try:
        rel = old_dir.relative_to(tuning_dir)
    except ValueError:
        print(f"ERROR: Ensemble directory {old_dir} is not under {tuning_dir}")
        return 1

    new_dir = prod_dir / rel
    print(f"Promoting ensemble {ensemble_id}\n  from {old_dir}\n    to {new_dir}")
    if new_dir.exists():
        print(f"ERROR: Target path already exists: {new_dir}")
        return 1

    if not args.force:
        resp = input('Proceed? (y/N) ')
        if resp.lower() not in ('y', 'yes'):
            print('Cancelled')
            return 0

    new_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(old_dir), str(new_dir))

    backend.update_ensemble(
        ensemble_id,
        status='PRODUCTION',
        directory=str(new_dir),
    )

    backend.add_operation(
        ensemble_id,
        operation_type='PROMOTE_ENSEMBLE',
        status='COMPLETED',
        user=getuser(),
        run_dir=str(new_dir),
    )
    print('Promotion OK')
    return 0


def _discover_base_dirs(base_arg):
    if base_arg == '.':
        current = Path.cwd()
        while current != current.parent:
            tuning = current / 'TUNING'
            prod = current / 'ENSEMBLES'
            if tuning.exists() and prod.exists():
                return current, tuning, prod
            current = current.parent
        print('ERROR: Could not auto-discover TUNING/ and ENSEMBLES/ roots')
        return None, None, None
    base = Path(base_arg).resolve()
    tuning = base / 'TUNING'
    prod = base / 'ENSEMBLES'
    if not tuning.exists() or not prod.exists():
        print(f'ERROR: Expected TUNING/ and ENSEMBLES/ under {base}')
        return None, None, None
    return base, tuning, prod
