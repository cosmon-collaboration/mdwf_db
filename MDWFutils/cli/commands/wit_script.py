import argparse, sys
from MDWFutils.db    import get_ensemble_details
from MDWFutils.jobs.wit import generate_wit_sbatch

def register(subparsers):
    p = subparsers.add_parser('wit-script', help='Generate WIT SLURM script')
    p.add_argument('--ensemble-id', type=int, required=True)
    p.add_argument('--account',     default='m2986_g')
    p.add_argument('--constraint',  default='gpu')
    p.add_argument('--gpus',        type=int, default=4)
    p.add_argument('--nodes',       type=int, default=1)
    p.add_argument('--cpus-per-task', type=int, default=16)
    p.add_argument('--time',        default='04:00:00')
    p.add_argument('--qos',         default='regular')
    p.add_argument('--ranks',       type=int, default=4)
    p.add_argument('--bind-sh',     default='bind.sh')
    p.add_argument('--exec-path',   required=True,
                   help='Full path to Meson executable')
    p.add_argument('--first',       type=int, required=True)
    p.add_argument('--last',        type=int, required=True)
    p.add_argument('--step',        type=int, default=4)
    p.set_defaults(func=do_wit_script)

def do_wit_script(args):
    ens = get_ensemble_details(args.db_file, args.ensemble_id)
    if not ens:
        print(f"ERROR: ensemble {args.ensemble_id} not found", file=sys.stderr)
        return 1

    # generate into ENSEMBLE_DIR/meson2pt/
    out = generate_wit_sbatch(
        output_file    = None,
        db_file        = args.db_file,
        ensemble_id    = args.ensemble_id,
        ensemble_dir   = ens['directory'],
        account        = args.account,
        constraint     = args.constraint,
        gpus           = args.gpus,
        nodes          = args.nodes,
        cpus_per_task  = args.cpus_per_task,
        time           = args.time,
        qos            = args.qos,
        ranks          = args.ranks,
        bind_sh        = args.bind_sh,
        exec_path      = args.exec_path,
        first          = args.first,
        last           = args.last,
        step           = args.step
    )
    print("Wrote WIT SBATCH script →", out)
    return 0