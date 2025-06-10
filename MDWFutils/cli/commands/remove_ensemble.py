import shutil
from MDWFutils.db import get_ensemble_details, remove_ensemble

def register(subparsers):
    p = subparsers.add_parser(
        'remove-ensemble',
        help='Delete an ensemble and its operations'
    )
    p.add_argument('--ensemble-id', type=int, required=True)
    p.add_argument(
        '--force',
        action='store_true',
        help='Skip confirmation prompt'
    )
    p.add_argument(
        '--remove-directory',
        action='store_true',
        help='Also delete the on-disk tree'
    )
    p.set_defaults(func=do_remove)

def do_remove(args):
    ens = get_ensemble_details(args.db_file, args.ensemble_id)
    if not ens:
        print(f"Ensemble not found: {args.ensemble_id}")
        return 1

    print(f"Removing ensemble {ens['id']} @ {ens['directory']}")
    if not args.force:
        if input("Proceed? (y/N) ").lower() not in ('y','yes'):
            print("Aborted")
            return 0

    ok = remove_ensemble(args.db_file, args.ensemble_id)
    print("DB removal:", "OK" if ok else "FAILED")

    if ok and args.remove_directory:
        try:
            shutil.rmtree(ens['directory'])
            print("Removed on-disk tree")
        except Exception as e:
            print("Error removing directory:", e)
            return 1

    return 0