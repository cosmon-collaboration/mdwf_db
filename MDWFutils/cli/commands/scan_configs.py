#!/usr/bin/env python3
"""Scan cnfg directories to update configuration ranges in the database."""

import argparse
import re
from pathlib import Path

from ..ensemble_utils import get_backend_for_args


def register(subparsers):
    p = subparsers.add_parser(
        'scan',
        help='Update configuration ranges based on cnfg/ files',
        description='Walk each ensemble cnfg/ directory and store first/last/inc/total in the database.'
    )
    p.add_argument('--force', action='store_true', help='Update all ensembles even if unchanged')
    p.set_defaults(func=do_scan)


def do_scan(args):
    backend = get_backend_for_args(args)
    ensembles = backend.list_ensembles(detailed=True)
    if not ensembles:
        print('No ensembles found')
        return 0

    updated = 0
    for ens in ensembles:
        ens_id = ens.get('ensemble_id') or ens.get('id')
        cnfg_dir = Path(ens['directory']) / 'cnfg'
        values = _extract_cfg_numbers(cnfg_dir)
        if not values:
            continue
        first, last = values[0], values[-1]
        increment = _infer_increment(values)
        total = len(values)
        cfg = ens.get('configurations') or {}
        if not args.force and cfg:
            if (
                cfg.get('first') == first
                and cfg.get('last') == last
                and cfg.get('increment') == increment
                and cfg.get('total') == total
            ):
                continue
        backend.update_ensemble(
            ens_id,
            configurations={
                'first': first,
                'last': last,
                'increment': increment,
                'total': total,
            },
        )
        updated += 1
        print(f"Updated ensemble {ens_id}: first={first} last={last} inc={increment} total={total}")

    print(f"Scan complete: {updated} ensemble(s) updated")
    return 0


def _extract_cfg_numbers(cnfg_dir: Path):
    if not cnfg_dir.exists():
        return []
    numbers = []
    for child in cnfg_dir.iterdir():
        if not child.is_file():
            continue
        m = list(re.finditer(r"(\d+)", child.name))
        if not m:
            continue
        try:
            numbers.append(int(m[-1].group(1)))
        except ValueError:
            pass
    return sorted(set(numbers))


def _infer_increment(values):
    if len(values) < 2:
        return None
    inc = values[1] - values[0]
    if inc <= 0:
        return None
    for idx in range(2, len(values)):
        if values[idx] - values[idx - 1] != inc:
            return None
    return inc
