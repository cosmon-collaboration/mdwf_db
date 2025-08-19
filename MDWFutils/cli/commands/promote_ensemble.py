#!/usr/bin/env python3
"""
commands/promote-ensemble.py

Move a TUNING ensemble to PRODUCTION status and directory.
"""
import shutil
import subprocess
import sys
import argparse
from pathlib import Path
from MDWFutils.db import update_ensemble, get_ensemble_id_by_directory
from ..ensemble_utils import resolve_ensemble_from_args

def register(subparsers):
    p = subparsers.add_parser(
        'promote-ensemble',
        help='Move ensemble from TUNING to PRODUCTION status',
        description="""
Move a TUNING ensemble to PRODUCTION status and directory.

WHAT THIS DOES:
• Moves ensemble directory from TUNING/ to ENSEMBLES/
• Updates ensemble status to PRODUCTION in database
• Records a PROMOTE_ENSEMBLE operation in the history
• Preserves all files and operation history

DIRECTORY MOVEMENT:
The ensemble directory is physically moved:
  FROM: TUNING/b{beta}/b{b}Ls{Ls}/mc{mc}/ms{ms}/ml{ml}/L{L}/T{T}/
  TO:   ENSEMBLES/b{beta}/b{b}Ls{Ls}/mc{mc}/ms{ms}/ml{ml}/L{L}/T{T}/

DATABASE UPDATES:
• Ensemble status: TUNING to PRODUCTION  
• Directory path: Updated to new ENSEMBLES/ location
• Operation history: PROMOTE_ENSEMBLE operation added

REQUIREMENTS:
• Ensemble must currently have TUNING status
• Target directory in ENSEMBLES/ must not already exist
• Source directory must be under TUNING/

FLEXIBLE ENSEMBLE IDENTIFICATION:
The --ensemble parameter accepts multiple formats:
  • Ensemble ID: -e 1
  • Relative path: -e ./TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64
  • Absolute path: -e /full/path/to/ensemble
  • Current directory: -e . (when run from within ensemble directory)

EXAMPLES:
  # Promote ensemble 1 to production
  mdwf_db promote-ensemble -e 1

  # Promote current ensemble (from within ensemble directory)
  mdwf_db promote-ensemble -e . --force

  # Promote with custom base directory
  mdwf_db promote-ensemble -e 1 --base-dir /scratch/lattice
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument('-e', '--ensemble', required=True,
                   help='Ensemble to promote (ID, directory path, or "." for current directory)')
    p.add_argument('--base-dir',
                   default='.',
                   help='Root directory containing TUNING/ and ENSEMBLES/ (default: current directory)')
    p.add_argument('--force', action='store_true',
                   help='Skip confirmation prompt and promote immediately')
    p.set_defaults(func=do_promote)


def resolve_ensemble_directory(ensemble_arg, base_dir, db_file):
    """
    Resolve ensemble directory from various input formats.
    
    Args:
        ensemble_arg: String argument from -e/--ensemble
        base_dir: Base directory containing TUNING/ and ENSEMBLES/
        db_file: Database file path
    
    Returns:
        tuple: (ensemble_id, ensemble_info, resolved_directory) or (None, None, None)
    """
    # If base_dir is not specified (defaults to '.'), try to auto-discover it
    if base_dir == '.':
        # Walk up the directory tree to find the root containing TUNING/ and ENSEMBLES/
        current = Path.cwd()
        while current != current.parent:
            if (current / 'TUNING').exists() and (current / 'ENSEMBLES').exists():
                base = current
                print(f"Auto-discovered base directory: {base}")
                break
            current = current.parent
        else:
            print("ERROR: Could not find root directory containing TUNING/ and ENSEMBLES/")
            print("Please specify --base-dir or run from a directory above TUNING/ and ENSEMBLES/")
            return None, None, None
    else:
        base = Path(base_dir).resolve()
    
    tuning = base / 'TUNING'
    prod = base / 'ENSEMBLES'
    
    # Check if base directory has the expected structure
    if not tuning.exists():
        print(f"ERROR: TUNING directory not found at {tuning}")
        return None, None, None
    if not prod.exists():
        print(f"ERROR: ENSEMBLES directory not found at {prod}")
        return None, None, None
    
    # Handle current directory case (-e .)
    if ensemble_arg == '.':
        current_dir = Path.cwd().resolve()
        print(f"Looking for ensemble in current directory: {current_dir}")
        
        # Try to find ensemble by current directory path
        ensemble_id = get_ensemble_id_by_directory(db_file, str(current_dir))
        if ensemble_id is None:
            print("ERROR: Current directory is not a registered ensemble")
            return None, None, None
        
        return ensemble_id, None, current_dir
    
    # Handle numeric ensemble ID
    if ensemble_arg.isdigit():
        ensemble_id = int(ensemble_arg)
        # Get ensemble details from database
        from MDWFutils.db import get_ensemble_details
        ensemble_info = get_ensemble_details(db_file, ensemble_id)
        if ensemble_info is None:
            print(f"ERROR: Ensemble {ensemble_id} not found in database")
            return None, None, None
        
        resolved_dir = Path(ensemble_info['directory'])
        return ensemble_id, ensemble_info, resolved_dir
    
    # Handle directory path
    try:
        # Try to resolve as absolute or relative path
        if Path(ensemble_arg).is_absolute():
            resolved_dir = Path(ensemble_arg).resolve()
        else:
            resolved_dir = (base / ensemble_arg).resolve()
        
        # Check if this directory exists
        if not resolved_dir.exists():
            print(f"ERROR: Directory does not exist: {resolved_dir}")
            return None, None, None
        
        # Try to find ensemble by directory path
        ensemble_id = get_ensemble_id_by_directory(db_file, str(resolved_dir))
        if ensemble_id is None:
            print(f"ERROR: Directory {resolved_dir} is not a registered ensemble")
            return None, None, None
        
        # Get ensemble details
        from MDWFutils.db import get_ensemble_details
        ensemble_info = get_ensemble_details(db_file, ensemble_id)
        return ensemble_id, ensemble_info, resolved_dir
        
    except Exception as e:
        print(f"ERROR: Could not resolve directory path '{ensemble_arg}': {e}")
        return None, None, None


def do_promote(args):
    # Resolve ensemble directory using improved logic
    ensemble_id, ensemble_info, old_dir = resolve_ensemble_directory(args.ensemble, args.base_dir, args.db_file)
    if ensemble_id is None:
        return 1
    
    # If we don't have ensemble_info yet, get it now
    if ensemble_info is None:
        from MDWFutils.db import get_ensemble_details
        ensemble_info = get_ensemble_details(args.db_file, ensemble_id)
        if ensemble_info is None:
            print(f"ERROR: Could not retrieve ensemble {ensemble_id} details")
            return 1
    
    if ensemble_info['status'] == 'PRODUCTION':
        print("Already in PRODUCTION")
        return 0
    
    if ensemble_info['status'] != 'TUNING':
        print(f"ERROR: Ensemble must be in TUNING status, not {ensemble_info['status']}")
        return 1
    
    # Get the base directory and tuning/prod paths
    if args.base_dir == '.':
        # Use the auto-discovered base directory
        current = Path.cwd()
        while current != current.parent:
            if (current / 'TUNING').exists() and (current / 'ENSEMBLES').exists():
                base = current
                break
            current = current.parent
    else:
        base = Path(args.base_dir).resolve()
    
    tuning = base / 'TUNING'
    prod = base / 'ENSEMBLES'
    
    # Verify the ensemble is actually under TUNING/
    try:
        rel = old_dir.relative_to(tuning)
    except ValueError:
        print(f"ERROR: Ensemble directory {old_dir} is not under TUNING/")
        print(f"Expected to be under: {tuning}")
        return 1
    
    new_dir = prod / rel
    print(f"Promote ensemble {ensemble_id}:")
    print(f"  from {old_dir}")
    print(f"    to {new_dir}")
    
    if new_dir.exists():
        print("ERROR: target path already exists:", new_dir)
        return 1
    
    if not args.force:
        resp = input("Proceed? (y/N) ")
        if resp.lower() not in ('y','yes'):
            print("Cancelled")
            return 0
    
    # do the move
    new_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(old_dir), str(new_dir))
    
    # update the ensembles table
    ok = update_ensemble(
        args.db_file,
        ensemble_id,
        status='PRODUCTION',
        directory=str(new_dir)
    )
    if not ok:
        print("Promotion FAILED")
        return 1
    
    # call existing CLI to record the PROMOTE_ENSEMBLE operation
    import sys
    cmd = [
        sys.executable, '-m', 'MDWFutils.cli.main',              
        'update',
        '--db-file',    args.db_file,
        '--ensemble-id', str(ensemble_id),
        '--operation-type', 'PROMOTE_ENSEMBLE',
        '--status',        'COMPLETED',
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        return result.returncode
    
    sys.stdout.write(result.stdout)
    print("Promotion OK")
    return 0