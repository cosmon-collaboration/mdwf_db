#!/usr/bin/env python3
"""
commands/config.py

Manage ensemble configuration files containing operation parameters.
"""

import sys
import argparse 
from pathlib import Path
from MDWFutils.db import resolve_ensemble_identifier, get_ensemble_details
from MDWFutils.config import (
    load_ensemble_config, save_ensemble_config, create_default_config,
    print_config_summary, validate_config, get_config_path
)

def register(subparsers):
    p = subparsers.add_parser(
        'default_params',
        help='Manage ensemble default parameter files for operation parameters',
        description="""
Manage default parameter files that store operation parameters for ensembles.
These files allow you to save "recipes" of parameters that work well for
specific ensembles and reuse them in script generation commands.

DEFAULT PARAMETER FILE FORMAT:
The default parameters are stored as YAML (default) or JSON and contain
parameters for different operation types and modes:

Example mdwf_default_params.yaml:
---
hmc:
  tepid:
    xml_params: "StartTrajectory=0 Trajectories=100 MDsteps=2 trajL=0.75"
    job_params: "cfg_max=100 time_limit=12:00:00 nodes=1"
  continue:
    xml_params: "Trajectories=50 MDsteps=2"
    job_params: "cfg_max=500 time_limit=6:00:00"
    
smearing:
  stout8:
    params: "nsteps=8 rho=0.1"
    job_params: "time_limit=2:00:00"

meson_2pt:
  default:
    params: "source_type=point sink_type=point"
    job_params: "time_limit=4:00:00"

USAGE WITH SCRIPT COMMANDS:
When using script generation commands (hmc-script, smear-script, etc.),
you can use the --use-default-params flag to load parameters from the file.
CLI parameters will override default parameter file parameters.

Examples:
  mdwf_db hmc-script -e 1 -a m2986 -m tepid --use-default-params
  mdwf_db hmc-script -e 1 -a m2986 -m continue --use-default-params -j "nodes=2"
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subcommands = p.add_subparsers(dest='config_action', help='Configuration actions')
    
    # Generate template
    gen_p = subcommands.add_parser(
        'generate',
        help='Generate a template default parameter file',
        description='Create a template default parameter file with common parameter sets'
    )
    gen_p.add_argument('-e', '--ensemble', required=True,
                       help='Ensemble ID, directory path, or "." for current directory')
    gen_p.add_argument('-f', '--format', choices=['yaml', 'json'], default='yaml',
                       help='Default parameter file format (default: yaml)')
    gen_p.add_argument('--overwrite', action='store_true',
                       help='Overwrite existing default parameter file')
    
    # Show default parameters
    show_p = subcommands.add_parser(
        'show',
        help='Display current default parameters',
        description='Show the current default parameters for an ensemble'
    )
    show_p.add_argument('-e', '--ensemble', required=True,
                        help='Ensemble ID, directory path, or "." for current directory')
    
    # Edit default parameters (placeholder for future)
    edit_p = subcommands.add_parser(
        'edit',
        help='Edit default parameter file',
        description='Open default parameter file in editor (uses $EDITOR environment variable)'
    )
    edit_p.add_argument('-e', '--ensemble', required=True,
                        help='Ensemble ID, directory path, or "." for current directory')
    
    # Validate default parameters
    validate_p = subcommands.add_parser(
        'validate',
        help='Validate default parameter file',
        description='Check default parameter file for syntax and structure errors'
    )
    validate_p.add_argument('-e', '--ensemble', required=True,
                           help='Ensemble ID, directory path, or "." for current directory')
    
    p.set_defaults(func=do_config)


def do_config(args):
    if not args.config_action:
        print("Error: No config action specified. Use 'generate', 'show', 'edit', or 'validate'", 
              file=sys.stderr)
        return 1
    
    # Resolve ensemble identifier to get directory
    ensemble_id, ens = resolve_ensemble_identifier(args.db_file, args.ensemble)
    if ensemble_id is None:
        print(f"ERROR: Ensemble not found: {args.ensemble}", file=sys.stderr)
        return 1
    
    ensemble_dir = Path(ens['directory'])
    
    if args.config_action == 'generate':
        return do_generate_config(ensemble_dir, args)
    elif args.config_action == 'show':
        return do_show_config(ensemble_dir, args)
    elif args.config_action == 'edit':
        return do_edit_config(ensemble_dir, args)
    elif args.config_action == 'validate':
        return do_validate_config(ensemble_dir, args)
    else:
        print(f"Unknown config action: {args.config_action}", file=sys.stderr)
        return 1


def do_generate_config(ensemble_dir: Path, args):
    """Generate a template configuration file."""
    config_path = get_config_path(ensemble_dir)
    
    if config_path.exists() and not args.overwrite:
        print(f"Configuration file already exists: {config_path}")
        print("Use --overwrite to replace it")
        return 1
    
    # Create default configuration
    config = create_default_config()
    
    # Save configuration
    success = save_ensemble_config(ensemble_dir, config, args.format)
    
    if success:
        print(f"Generated configuration template: {config_path}")
        print(f"Edit this file to customize parameters for your ensemble")
        return 0
    else:
        print(f"Failed to generate configuration file", file=sys.stderr)
        return 1


def do_show_config(ensemble_dir: Path, args):
    """Show current configuration."""
    print_config_summary(ensemble_dir)
    return 0


def do_edit_config(ensemble_dir: Path, args):
    """Edit configuration file in external editor."""
    import os
    import subprocess
    
    config_path = get_config_path(ensemble_dir)
    
    if not config_path.exists():
        print(f"No configuration file found: {config_path}")
        print("Use 'mdwf_db config generate' to create one first")
        return 1
    
    editor = os.environ.get('EDITOR', 'vi')
    
    try:
        subprocess.run([editor, str(config_path)], check=True)
        print(f"Configuration file edited: {config_path}")
        return 0
    except subprocess.CalledProcessError:
        print(f"Error opening editor: {editor}", file=sys.stderr)
        return 1
    except FileNotFoundError:
        print(f"Editor not found: {editor}", file=sys.stderr)
        print("Set the EDITOR environment variable to your preferred editor")
        return 1


def do_validate_config(ensemble_dir: Path, args):
    """Validate configuration file."""
    config_path = get_config_path(ensemble_dir)
    
    if not config_path.exists():
        print(f"No configuration file found: {config_path}")
        return 1
    
    try:
        config = load_ensemble_config(ensemble_dir)
        if validate_config(config):
            print(f"Configuration file is valid: {config_path}")
            return 0
        else:
            print(f"Configuration file has errors: {config_path}", file=sys.stderr)
            return 1
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        return 1