#!/usr/bin/env python3
"""
ensemble_utils.py

Common utilities for CLI commands that work with ensembles.
"""

import os
import sys

from MDWFutils.backends import get_backend
from MDWFutils.exceptions import EnsembleNotFoundError
from .components import EnsembleResolver


def add_ensemble_argument(parser, help_text=None):
    """
    Add ensemble identifier argument to a parser.
    
    Args:
        parser: ArgumentParser to add the argument to
        help_text: Custom help text (optional)
    """
    default_help = ('Ensemble identifier: either ensemble ID (integer) or path to ensemble directory')
    
    parser.add_argument(
        '-e', '--ensemble',
        required=True,
        help=help_text or default_help
    )


def resolve_ensemble_from_args(args):
    """
    Resolve ensemble from CLI arguments using the backend abstraction.
    """
    backend = _backend_from_args(args)
    resolver = EnsembleResolver(backend)
    try:
        return resolver.resolve(args.ensemble)
    except EnsembleNotFoundError:
        if isinstance(args.ensemble, str) and not args.ensemble.isdigit():
            print(f"ERROR: Ensemble not found at path: {args.ensemble}", file=sys.stderr)
        else:
            print(f"ERROR: Ensemble not found with ID: {args.ensemble}", file=sys.stderr)
        return None, None


def migrate_ensemble_id_argument(parser):
    """
    Update existing --ensemble-id arguments to use the new flexible --ensemble argument.
    This is for backward compatibility during migration.
    
    Args:
        parser: ArgumentParser to update
    """
    # Remove the old ensemble-id argument if it exists and add the new one
    for action in parser._actions[:]:
        if hasattr(action, 'dest') and action.dest == 'ensemble_id':
            parser._remove_action(action)
            break
    
    add_ensemble_argument(parser) 


def get_backend_for_args(args):
    connection = getattr(args, "db_file", None)
    if not connection:
        connection = os.getenv("MDWF_DB_URL") or os.getenv("MDWF_DB_FILE", "mdwf_db.sqlite")
    return get_backend(connection)


def _backend_from_args(args):
    return get_backend_for_args(args)