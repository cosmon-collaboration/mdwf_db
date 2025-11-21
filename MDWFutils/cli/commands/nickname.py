#!/usr/bin/env python3
"""
commands/nickname.py

Get, set or clear a human-friendly nickname for an ensemble.

Examples:
  mdwf_db nickname -e 1                       # Print current nickname
  mdwf_db nickname -e 1 --set "test32a"      # Set nickname
  mdwf_db nickname -e ./TUNING/b6.0/... --set mynick
  mdwf_db nickname -e 1 --clear               # Clear nickname
"""

import sys
import argparse
from MDWFutils.cli.ensemble_utils import (
    add_ensemble_argument,
    resolve_ensemble_from_args,
    get_backend_for_args,
)


def register(subparsers: argparse._SubParsersAction):
    p = subparsers.add_parser(
        'nickname',
        help='Get, set or clear a human-friendly nickname for an ensemble',
        description='Attach a nickname to an ensemble so it can be referenced with -e <nickname>.',
    )

    add_ensemble_argument(p, help_text='Ensemble identifier: ID, directory path, or "." for current directory')

    g = p.add_mutually_exclusive_group(required=False)
    g.add_argument('--set', dest='nickname', help='Set nickname for the ensemble')
    g.add_argument('--clear', action='store_true', help='Clear existing nickname')

    p.add_argument('--force', action='store_true', help='Allow duplicate nickname (not recommended)')

    p.set_defaults(func=do_nickname)


def do_nickname(args):
    backend = get_backend_for_args(args)
    ensemble_id, ensemble = resolve_ensemble_from_args(args)
    if ensemble_id is None:
        return 1

    # If neither --set nor --clear is specified, just print the current nickname
    if not getattr(args, 'clear', False) and not getattr(args, 'nickname', None):
        nickname = ensemble.get('nickname')
        print(nickname or "None")
        return 0
    
    if getattr(args, 'clear', False):
        try:
            backend.update_ensemble(ensemble_id, nickname=None)
            print('Cleared nickname')
            return 0
        except Exception as e:
            print(f"ERROR: Failed to clear nickname: {e}", file=sys.stderr)
            return 1

    # Setting nickname
    nickname = (args.nickname or '').strip()
    if not nickname:
        print('ERROR: Nickname must be a non-empty string', file=sys.stderr)
        return 1

    if not getattr(args, 'force', False):
        for ens in backend.list_ensembles(detailed=False):
            if ens.get('nickname') == nickname and ens.get('ensemble_id') != ensemble_id:
                print(f"ERROR: Nickname already in use by ensemble {ens.get('ensemble_id')}. Use --force to override.", file=sys.stderr)
                return 1

    try:
        backend.update_ensemble(ensemble_id, nickname=nickname)
        print(f"Set nickname: {nickname}")
        return 0
    except Exception as e:
        print(f"ERROR: Failed to set nickname: {e}", file=sys.stderr)
        return 1


