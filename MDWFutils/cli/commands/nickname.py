#!/usr/bin/env python3
"""
commands/nickname.py

Set or clear a human-friendly nickname for an ensemble.

Examples:
  mdwf_db nickname -e 1 --set "test32a"
  mdwf_db nickname -e ./TUNING/b6.0/... --set mynick
  mdwf_db nickname -e 1 --clear
"""

import sys
import argparse
from MDWFutils.cli.ensemble_utils import add_ensemble_argument, resolve_ensemble_from_args
from MDWFutils.db import (
    set_ensemble_parameter,
    delete_ensemble_parameter,
    get_ensemble_id_by_nickname,
)


def register(subparsers: argparse._SubParsersAction):
    p = subparsers.add_parser(
        'nickname',
        help='Set or clear a human-friendly nickname for an ensemble',
        description='Attach a nickname to an ensemble so it can be referenced with -e <nickname>.',
    )

    add_ensemble_argument(p, help_text='Ensemble identifier: ID, directory path, or "." for current directory')

    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument('--set', dest='nickname', help='Set nickname for the ensemble')
    g.add_argument('--clear', action='store_true', help='Clear existing nickname')

    p.add_argument('--force', action='store_true', help='Allow duplicate nickname (not recommended)')

    p.set_defaults(func=do_nickname)


def do_nickname(args):
    ensemble_id, _ = resolve_ensemble_from_args(args)
    if ensemble_id is None:
        return 1

    if getattr(args, 'clear', False):
        try:
            delete_ensemble_parameter(args.db_file, ensemble_id, 'nickname')
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

    # Check uniqueness unless forced
    try:
        other_id = get_ensemble_id_by_nickname(args.db_file, nickname)
        if other_id is not None and other_id != ensemble_id and not getattr(args, 'force', False):
            print(f"ERROR: Nickname already in use by ensemble {other_id}. Use --force to override.", file=sys.stderr)
            return 1

        set_ensemble_parameter(args.db_file, ensemble_id, 'nickname', nickname)
        print(f"Set nickname: {nickname}")
        return 0
    except Exception as e:
        print(f"ERROR: Failed to set nickname: {e}", file=sys.stderr)
        return 1


