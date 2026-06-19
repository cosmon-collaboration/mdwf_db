import argparse
import shutil
import sys

from ..ensemble_utils import resolve_ensemble_from_args, get_backend_for_args
from ...build.operations import SITE_ENSEMBLE_NICKNAME

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
    if ens.get('nickname') == SITE_ENSEMBLE_NICKNAME and not args.force:
        print(
            f"ERROR: Refusing to remove site ensemble '{SITE_ENSEMBLE_NICKNAME}' without --force",
            file=sys.stderr,
        )
        return 1
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