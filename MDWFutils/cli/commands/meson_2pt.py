import argparse, sys, os, ast
from pathlib import Path
from MDWFutils.db    import get_ensemble_details
from MDWFutils.jobs.wit import generate_wit_sbatch
from MDWFutils.config import get_operation_config, merge_params, get_config_path, save_operation_config

REQUIRED_JOB_PARAMS = ['queue', 'time_limit', 'nodes', 'cpus_per_task']
DEFAULT_JOB_PARAMS = {
    'account'       : 'm2986_g',
    'constraint'    : 'gpu',
    'gpus'          : '4',
    'gpu_bind'      : 'none',
    'mail_user'     : os.getenv('USER',''),
}

def register(subparsers):
    p = subparsers.add_parser(
        'meson-2pt', 
        help='Generate WIT meson correlator measurement script',
        description="""
Generate SLURM script for meson 2-point correlator measurements using WIT.

WHAT THIS DOES:
• Creates WIT input file (DWF.in) with measurement parameters
• Generates SLURM batch script for GPU execution
• Sets up proper directory structure for correlator output
• Configures job parameters for HPC submission

WIT PROGRAM:
WIT (Wilson Improved Twisted mass) is used for computing meson correlators
from Domain Wall Fermion propagators on smeared gauge configurations.

JOB PARAMETERS (via -j/--job-params):
Required parameters:
  queue:         SLURM queue (e.g., regular)
  time_limit:    Job time limit (e.g., 06:00:00)
  nodes:         Number of nodes (e.g., 1)
  cpus_per_task: CPUs per task (e.g., 16)
  mail_user:     Email for job notifications

Optional parameters (with defaults):
  account: m2986_g          # SLURM account
  constraint: gpu           # Node constraint
  gpus: 4                   # GPUs per node
  gpu_bind: none            # GPU binding

WIT PARAMETERS (via -w/--wit-params):
WIT parameters use dot notation (SECTION.KEY=value) and can be overridden:

Required parameters:
  Configurations.first:     First configuration number
  Configurations.last:      Last configuration number

Common parameters (with defaults):
  name: u_stout8                    # Run name
  cnfg_dir: ../cnfg_stout8/         # Configuration directory
  Configurations.step: 4            # Step between configurations
  Witness.no_prop: 3                # Number of propagators (light, strange, charm)
  Witness.no_solver: 2              # Number of solvers

Lattice parameters (auto-set from ensemble):
  Ls: (from ensemble)               # Domain wall extent
  b: (from ensemble)                # Domain wall height
  c: (from ensemble)                # Domain wall parameter

Solver parameters:
  Solver 0.solver: CG               # Conjugate gradient solver
  Solver 0.nmx: 8000                # Maximum iterations
  Solver 1.exact_deflation: false   # Use exact deflation

Propagator parameters:
  Propagator 0.kappa: (auto from ml)  # Light quark kappa
  Propagator 1.kappa: (auto from ms)  # Strange quark kappa  
  Propagator 2.kappa: (auto from mc)  # Charm quark kappa

Note: Kappa values are automatically calculated from quark masses
stored in the ensemble parameters. Do not set these manually.

EXAMPLES:
  # Basic meson correlator measurement
  mdwf_db meson-2pt -e 1 \\
    -j "queue=regular time_limit=06:00:00 nodes=1 cpus_per_task=16 mail_user=user@example.com" \\
    -w "Configurations.first=100 Configurations.last=200"

  # Custom solver settings
  mdwf_db meson-2pt -e 1 \\
    -j "queue=regular time_limit=12:00:00 nodes=2 cpus_per_task=32 mail_user=user@example.com" \\
    -w "Configurations.first=0 Configurations.last=50 Solver 0.nmx=10000 Configurations.step=2"

  # Point source measurement
  mdwf_db meson-2pt -e 1 \\
    -j "queue=regular time_limit=06:00:00 nodes=1 cpus_per_task=16 mail_user=user@example.com" \\
    -w "Configurations.first=100 Configurations.last=150 Propagator 0.Source=Point"

  # Use stored default parameters
  mdwf_db meson-2pt -e 1 --use-default-params

  # Use default params with CLI overrides
  mdwf_db meson-2pt -e 1 --use-default-params -w "Configurations.first=150" -j "nodes=2"

  # Save current parameters for later reuse
  mdwf_db meson-2pt -e 1 -j "queue=regular time_limit=6:00:00 nodes=1" -w "Configurations.first=100" --save-default-params

  # Save under custom variant name
  mdwf_db meson-2pt -e 1 -w "Propagator 0.Source=Wall" -j "nodes=2" --save-params-as "wall"

  # Use specific parameter variant
  mdwf_db meson-2pt -e 1 --use-default-params --params-variant wall

DEFAULT PARAMETER FILES:
Use 'mdwf_db default_params generate -e <ensemble>' to create a default parameter template.
The --use-default-params flag loads parameters from mdwf_default_params.yaml in the ensemble directory.
The --save-default-params flag saves current parameters to the default params file for later reuse.
CLI parameters override default parameter file parameters.

For complete parameter documentation, see the WIT manual or examine
generated DWF.in files for all available options.
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument('-e','--ensemble-id', type=int, required=True,
                   help='ID of the ensemble to generate meson correlator script for')
    p.add_argument('-j','--job-params', default='',
                   help=f'Space-separated key=val for SLURM job parameters. Required: {REQUIRED_JOB_PARAMS}')
    p.add_argument('-w','--wit-params', default='',
                   help='Space-separated key=val for WIT parameters using dot notation. Required: Configurations.first, Configurations.last')
    p.add_argument('--use-default-params', action='store_true',
                   help='Load parameters from ensemble default parameter file (mdwf_default_params.yaml)')
    p.add_argument('--params-variant',
                   help='Specify which parameter variant to use (e.g., default, wall, point)')
    p.add_argument('--save-default-params', action='store_true',
                   help='Save current command parameters to default parameter file for later reuse')
    p.add_argument('--save-params-as',
                   help='Save current parameters under specific variant name (default: default)')
    p.set_defaults(func=do_meson_2pt)

def do_meson_2pt(args):
    # Get ensemble details first for config loading
    ens = get_ensemble_details(args.db_file, args.ensemble_id)
    if not ens:
        print(f"ERROR: ensemble {args.ensemble_id} not found", file=sys.stderr)
        return 1
    ens_dir = Path(ens['directory']).resolve()

    # Load parameters from config file if requested
    config_job_params = ""
    config_wit_params = ""
    
    if args.use_default_params:
        if args.params_variant:
            # Use specified variant
            config = get_operation_config(ens_dir, 'meson_2pt', args.params_variant)
            if config:
                config_job_params = config.get('job_params', '')
                config_wit_params = config.get('params', '')
                print(f"Loaded meson_2pt.{args.params_variant} default parameters from {get_config_path(ens_dir)}")
            else:
                config_path = get_config_path(ens_dir)
                print(f"Warning: No meson_2pt.{args.params_variant} default parameters found in {config_path}")
        else:
            # Try different parameter variants for meson_2pt (fallback behavior)
            config = None
            for meson_type in ['default', 'wall', 'point']:
                config = get_operation_config(ens_dir, 'meson_2pt', meson_type)
                if config:
                    config_job_params = config.get('job_params', '')
                    config_wit_params = config.get('params', '')
                    print(f"Loaded meson_2pt.{meson_type} default parameters from {get_config_path(ens_dir)}")
                    break
            
            if not config:
                config_path = get_config_path(ens_dir)
                if config_path.exists():
                    print(f"Warning: No meson_2pt default parameters found in {config_path}")
                else:
                    print(f"Warning: No default parameter file found at {config_path}")
                    print("Use 'mdwf_db default_params generate' to create one")

    # Merge config parameters with CLI parameters (CLI takes precedence)
    merged_job_params = merge_params(config_job_params, args.job_params)
    merged_wit_params = merge_params(config_wit_params, args.wit_params)

    # Parse merged job parameters
    job_dict = DEFAULT_JOB_PARAMS.copy()
    if merged_job_params:
        for param in merged_job_params.split():
            if '=' in param:
                key, val = param.split('=', 1)
                job_dict[key] = val

    # Check required parameters
    missing = [k for k in REQUIRED_JOB_PARAMS if k not in job_dict]
    if missing:
        if args.use_default_params:
            print(f"ERROR: missing required job parameters: {missing}. Add them to your default parameter file or use -j", file=sys.stderr)
        else:
            print("ERROR: missing required job parameters:", missing, file=sys.stderr)
        return 1

    # Parse merged WIT parameters into nested dict - only parameters that go into DWF.in
    wdict = {}
    for tok in merged_wit_params.split():
        if not tok.strip():  # Skip empty tokens
            continue
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
            if p not in d or not isinstance(d[p], dict):
                d[p] = {}
            d = d[p]
        d[parts[-1]] = val

    # List of valid WIT parameters (only those that go into DWF.in)
    valid_wit_params = {
        'name', 'cnfg_dir', 'Configurations.first', 'Configurations.last', 'Configurations.step',
        'Random number generator.level', 'Random number generator.seed', 'Ls', 'M5', 'b', 'c',
        'type', 'Witness.no_prop', 'Witness.no_solver', 'Solver 0.solver', 'Solver 0.nkv',
        'Solver 0.isolv', 'Solver 0.nmr', 'Solver 0.ncy', 'Solver 0.nmx', 'Solver 0.exact_deflation',
        'Solver 1.solver', 'Solver 1.nkv', 'Solver 1.isolv', 'Solver 1.nmr', 'Solver 1.ncy',
        'Solver 1.nmx', 'Solver 1.exact_deflation', 'Exact Deflation.Cheby_fine',
        'Exact Deflation.Cheby_smooth', 'Exact Deflation.Cheby_coarse', 'Exact Deflation.kappa',
        'Exact Deflation.res', 'Exact Deflation.nmx', 'Exact Deflation.Ns', 'Propagator 0.Noise',
        'Propagator 0.Source', 'Propagator 0.Dilution', 'Propagator 0.pos', 'Propagator 0.mom',
        'Propagator 0.twist', 'Propagator 0.kappa', 'Propagator 0.mu', 'Propagator 1.Noise',
        'Propagator 1.Source', 'Propagator 1.Dilution', 'Propagator 1.pos', 'Propagator 1.mom',
        'Propagator 1.twist', 'Propagator 1.kappa', 'Propagator 1.mu', 'Propagator 2.Noise',
        'Propagator 2.Source', 'Propagator 2.Dilution', 'Propagator 2.pos', 'Propagator 2.mom',
        'Propagator 2.twist', 'Propagator 2.kappa', 'Propagator 2.mu'
    }

    # Helper to recursively check for unused WIT parameters
    def find_unused_keys(d, valid_keys, prefix=""):
        unused = []
        for k, v in d.items():
            full_key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                unused.extend(find_unused_keys(v, valid_keys, full_key))
            elif full_key not in valid_keys:
                unused.append(full_key)
        return unused

    # Helper to recursively remove unused keys from nested dict
    def remove_unused_keys(d, unused, prefix=""):
        keys_to_remove = []
        for k, v in d.items():
            full_key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                remove_unused_keys(v, unused, full_key)
                if not v:  # Remove empty dicts
                    keys_to_remove.append(k)
            elif full_key in unused:
                keys_to_remove.append(k)
        for k in keys_to_remove:
            d.pop(k)

    # Check for unused WIT parameters and warn
    unused_w = find_unused_keys(wdict, valid_wit_params)
    for param in unused_w:
        print(f"WARNING: WIT parameter '{param}' was provided but is not used in DWF.in", file=sys.stderr)
    
    # Remove unused keys from wdict
    remove_unused_keys(wdict, unused_w)

    # Use ensemble directory from earlier

    # Generate the script
    sbatch = generate_wit_sbatch(
        db_file       = args.db_file,
        ensemble_id   = args.ensemble_id,
        ensemble_dir  = str(ens_dir),
        custom_changes = wdict,
        **job_dict
    )
    print("Wrote WIT SBATCH script to", sbatch)
    
    # Save parameters to default params if requested
    if args.save_default_params:
        save_variant = args.save_params_as if args.save_params_as else 'default'
        success = save_operation_config(
            ens_dir, 'meson_2pt', save_variant,
            job_params=merged_job_params,
            params=merged_wit_params
        )
        if success:
            print(f"Saved parameters to default params: meson_2pt.{save_variant}")
        else:
            print(f"Warning: Failed to save parameters to default params", file=sys.stderr)
    
    return 0