#!/usr/bin/env python3
"""
ensemble_utils.py

Common utilities for CLI commands that work with ensembles.
"""

import sys
from .components import EnsembleResolver
from ..exceptions import EnsembleNotFoundError
from .runtime import load_default_backend


def add_ensemble_argument(parser, help_text=None, required=True):
    """
    Add ensemble identifier argument to a parser.
    
    Args:
        parser: ArgumentParser to add the argument to
        help_text: Custom help text (optional)
        required: Whether the argument is required (default: True)
    """
    from .args import add_ensemble_arg
    add_ensemble_arg(parser, required=required, help_text=help_text)


def resolve_ensemble_from_args(args, backend=None):
    """
    Resolve ensemble from CLI arguments using the backend abstraction.
    """
    backend = backend or _backend_from_args(args)
    resolver = EnsembleResolver(backend)
    try:
        return resolver.resolve(args.ensemble)
    except EnsembleNotFoundError:
        if isinstance(args.ensemble, str) and not args.ensemble.isdigit():
            print(f"ERROR: Ensemble not found at path: {args.ensemble}", file=sys.stderr)
        else:
            print(f"ERROR: Ensemble not found with ID: {args.ensemble}", file=sys.stderr)
        return None, None


def get_backend_for_args(args):
    """Resolve backend connection using environment variables only (MongoDB required)."""
    return load_default_backend()


def _backend_from_args(args):
    return get_backend_for_args(args)
