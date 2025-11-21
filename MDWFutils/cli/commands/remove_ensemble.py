import shutil
import argparse

from ..ensemble_utils import resolve_ensemble_from_args, get_backend_for_args

def register(subparsers):
    p = subparsers.add_parser(
        'remove-ensemble',
        help='Remove ensemble and all its operations from database',
        description="""
Remove an ensemble and all its operations from the database.

WHAT THIS DOES:
• Removes the ensemble record from the database
• Deletes all associated operations and their parameters
• Optionally deletes the on-disk directory tree

WHAT IS REMOVED FROM DATABASE:
• Ensemble record (ID, directory, status, creation time)
• Physics parameters (beta, masses, lattice dimensions)
• All operation records (HMC, smearing, measurements, etc.)
• Operation parameters (config ranges, job IDs, exit codes)
• Operation timestamps and status information

FILESYSTEM OPERATIONS:
By default, only the database records are removed. The ensemble
directory and all files remain on disk.

Use --remove-directory to also delete the on-disk ensemble tree.

WARNING: Database removal is irreversible. All ensemble and operation
data will be permanently deleted from the database.

FLEXIBLE ENSEMBLE IDENTIFICATION:
The --ensemble parameter accepts multiple formats:
  • Ensemble ID: -e 1
  • Relative path: -e ./TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64
  • Absolute path: -e /full/path/to/ensemble
  • Current directory: -e . (when run from within ensemble directory)

EXAMPLES:
  # Remove from database only (preserve files)
  mdwf_db remove-ensemble -e 1

  # Remove database record and delete files
  mdwf_db remove-ensemble -e 1 --remove-directory --force

  # Remove current ensemble (from within ensemble directory)
  mdwf_db remove-ensemble -e . --force
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument('-e', '--ensemble', required=True,
                   help='Ensemble to remove (ID, directory path, or "." for current directory)')
    p.add_argument(
        '--force',
        action='store_true',
        help='Skip confirmation prompt and remove immediately'
    )
    p.add_argument(
        '--remove-directory',
        action='store_true',
        help='Also delete the ensemble directory and all files on disk'
    )
    p.set_defaults(func=do_remove)

def do_remove(args):
    backend = get_backend_for_args(args)
    ensemble_id, ens = resolve_ensemble_from_args(args)
    if not ens:
        return 1

    display_id = ens.get('ensemble_id', ens.get('id'))
    print(f"Removing ensemble {display_id} @ {ens['directory']}")
    if not args.force:
        if input("Proceed? (y/N) ").lower() not in ('y','yes'):
            print("Aborted")
            return 0

    ok = backend.delete_ensemble(ensemble_id)
    print("DB removal:", "OK" if ok else "FAILED")

    if ok and args.remove_directory:
        try:
            shutil.rmtree(ens['directory'])
            print("Removed on-disk tree")
        except Exception as e:
            print("Error removing directory:", e)
            return 1

    return 0