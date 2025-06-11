import argparse, sys, os, ast
from MDWFutils.db    import get_ensemble_details
from MDWFutils.jobs.wit import generate_wit_sbatch

REQUIRED_JOB_PARAMS = ['queue', 'time_limit', 'nodes', 'cpus_per_task']
DEFAULT_JOB_PARAMS = {
    'account'       : 'm2986_g',
    'constraint'    : 'gpu',
    'gpus'          : '4',
    'gpu_bind'      : 'none',
    'mail_user'     : os.getenv('USER',''),
}

def register(subparsers):
    p = subparsers.add_parser('meson-2pt', help='Generate WIT SLURM script for meson 2pt measurements')
    p.add_argument('-e','--ensemble-id', type=int, required=True,
                   help='ID of the ensemble')
    p.add_argument('-j','--job-params', default='',
                   help=('Space-separated key=val for SBATCH; '
                         f'required: {REQUIRED_JOB_PARAMS}'))
    p.add_argument('-w','--wit-params', default='',
                   help=('Space-separated key=val for WIT input; '
                         'must include Configurations.first,Configurations.last; '
                         'nest with dots: SECTION.KEY=val'))
    p.set_defaults(func=do_meson_2pt)

def do_meson_2pt(args):
    ens = get_ensemble_details(args.db_file, args.ensemble_id)
    if not ens:
        print(f"ERROR: ensemble {args.ensemble_id} not found", file=sys.stderr)
        return 1

    # Parse job parameters
    jdict = {}
    for tok in args.job_params.split():
        if '=' not in tok:
            print(f"ERROR: bad job-param {tok}", file=sys.stderr)
            return 1
        k,v = tok.split('=',1)
        jdict[k] = v

    # Warn for unused job parameters
    valid_job_params = {'account','constraint','gpus','gpu_bind','mail_user','queue','time_limit','nodes','cpus_per_task','output_file'}
    unused = [k for k in jdict if k not in valid_job_params]
    for k in unused:
        print(f"WARNING: job parameter '{k}' was provided but is not used by the script generator.", file=sys.stderr)
    # Remove unused keys
    for k in unused:
        jdict.pop(k)

    # Check required job parameters
    missing = [k for k in REQUIRED_JOB_PARAMS if k not in jdict]
    if missing:
        print("ERROR: missing job parameters:", missing, file=sys.stderr)
        return 1

    # Fill in defaults
    for k,v in DEFAULT_JOB_PARAMS.items():
        jdict.setdefault(k,v)

    # Parse WIT parameters into nested dict
    wdict = {}
    for tok in args.wit_params.split():
        if '=' not in tok:
            print(f"ERROR: bad WIT-param {tok}", file=sys.stderr)
            return 1
        key, raw = tok.split('=',1)
        try:
            val = ast.literal_eval(raw)
        except:
            val = raw
        parts = key.split('.')
        d = wdict
        for p in parts[:-1]:
            d = d.setdefault(p,{})
        d[parts[-1]] = val

    # Warn for unused WIT parameters (top-level keys)
    valid_wit_params = {'Run name','Directories','Configurations','Random number generator','Lattice parameters','Boundary conditions','Witness','Solver','Exact Deflation','Propagators','AMA'}
    unused_wit = [k for k in wdict if k not in valid_wit_params]
    for k in unused_wit:
        print(f"WARNING: WIT parameter '{k}' was provided but is not used by the script generator.", file=sys.stderr)
    # Remove unused keys
    for k in unused_wit:
        wdict.pop(k)

    # Require first, last
    if 'Configurations' not in wdict or 'first' not in wdict['Configurations'] or 'last' not in wdict['Configurations']:
        print("ERROR: must supply Configurations.first and Configurations.last", file=sys.stderr)
        return 1

    # Validate step parameter
    if 'Configurations.step' in wdict:
        try:
            step = int(wdict['Configurations']['step'])
            if step <= 0:
                print("ERROR: Configurations.step must be positive", file=sys.stderr)
                return 1
        except ValueError:
            print("ERROR: Configurations.step must be an integer", file=sys.stderr)
            return 1
    else:
        wdict.setdefault('Configurations', {})['step'] = '4'

    # Generate the script
    out = generate_wit_sbatch(
        output_file    = None,
        db_file        = args.db_file,
        ensemble_id    = args.ensemble_id,
        ensemble_dir   = ens['directory'],
        custom_changes = wdict,
        **jdict
    )
    return 0