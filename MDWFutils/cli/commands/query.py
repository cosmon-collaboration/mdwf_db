from MDWFutils.db import get_ensemble_details, print_history

def register(subparsers):
    p = subparsers.add_parser(
        'query-ensemble',
        help='Show ensemble details and operation history'
    )
    p.add_argument('--ensemble-id', type=int, required=True)
    p.set_defaults(func=do_query)

def do_query(args):
    ens = get_ensemble_details(args.db_file, args.ensemble_id)
    if not ens:
        print(f"Ensemble not found: {args.ensemble_id}")
        return 1

    print(f"ID         = {ens['id']}")
    print(f"Directory  = {ens['directory']}")
    print(f"Status     = {ens['status']}")
    print(f"Created    = {ens['creation_time']}")
    if ens.get('description'):
        print(f"Description= {ens['description']}")
    print("Parameters:")
    for k, v in ens['parameters'].items():
        print(f"  {k} = {v}")

    print("\n=== Operation history ===")
    print_history(args.db_file, args.ensemble_id)
    return 0