from MDWFutils.db import list_ensembles

def register(subparsers):
    p = subparsers.add_parser(
        'list-ensembles',
        help='List all ensembles'
    )
    p.add_argument(
        '--detailed',
        action='store_true',
        help='Show parameters and operation count'
    )
    p.set_defaults(func=do_list)

def do_list(args):
    ens_list = list_ensembles(args.db_file, detailed=args.detailed)
    if not ens_list:
        print("No ensembles found")
        return 0

    for e in ens_list:
        print(f"[{e['id']}] ({e['status']}) {e['directory']}")
        if args.detailed:
            for k, v in e.get('parameters', {}).items():
                print(f"  {k} = {v}")
            print(f"  ops = {e.get('operation_count',0)}")
    return 0