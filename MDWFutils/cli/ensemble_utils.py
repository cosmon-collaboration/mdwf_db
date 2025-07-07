#!/usr/bin/env python3
"""
ensemble_utils.py

Common utilities for CLI commands that work with ensembles.
"""

import sys
from MDWFutils.db import resolve_ensemble_identifier


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
    Resolve ensemble from CLI arguments using the new flexible identifier system.
    
    Args:
        args: Parsed command line arguments containing 'ensemble' and 'db_file'
    
    Returns:
        tuple: (ensemble_id, ensemble_details) or exits on error
    """
    ensemble_id, ensemble_details = resolve_ensemble_identifier(args.db_file, args.ensemble)
    
    if ensemble_id is None:
        if isinstance(args.ensemble, str) and not args.ensemble.isdigit():
            print(f"ERROR: Ensemble not found at path: {args.ensemble}", file=sys.stderr)
        else:
            print(f"ERROR: Ensemble not found with ID: {args.ensemble}", file=sys.stderr)
        sys.exit(1)
    
    return ensemble_id, ensemble_details


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