import argparse, sys, os, ast
from pathlib import Path
from MDWFutils.db    import get_ensemble_details, resolve_ensemble_identifier
from MDWFutils.jobs.zv import generate_zv_sbatch
from MDWFutils.config import get_operation_config, merge_params, get_config_path, save_operation_config

# Required job params: time_limit, nodes, mail_user. Config range lives in WIT params.
REQUIRED_JOB_PARAMS = ['mail_user', 'time_limit', 'nodes']
DEFAULT_JOB_PARAMS = {
    'account'    : 'm2986_g',
    'constraint' : 'gpu',
    'queue'      : 'regular',
    'time_limit' : '06:00:00',
    'nodes'      : '1',
    'gpus'       : '4',
    'gpu_bind'   : 'none',
    'ranks'      : '4',
    'ogeom'      : '1,1,1,4',
}

def register(subparsers):
    p = subparsers.add_parser(
        'zv-script',
        aliases=['zv'], 
        help='Generate Zv measurement script',
        description="""
Generate SLURM script for Zv correlator measurements using WIT.

JOB PARAMETERS (via -j/--job-params):
Required parameters:
          queue:         SLURM queue (e.g., regular)
          time_limit:    Job time limit (e.g., 06:00:00)
          nodes:         Number of nodes (e.g., 1)
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
  Witness.no_prop: 1                # Number of propagators (light only)
  Witness.no_solver: 1              # Number of solvers

Lattice parameters (auto-set from ensemble):
  Ls: (from ensemble)               # Domain wall extent
  b: (from ensemble)                # Domain wall height
  c: (from ensemble)                # Domain wall parameter

Solver parameters:
  Solver 0.solver: CG               # Conjugate gradient solver
  Solver 0.nmx: 8000                # Maximum iterations

Propagator parameters:
  Propagator 0.kappa: (auto from ml)  # Light quark kappa

Note: Kappa value is automatically calculated from the light quark mass
stored in the ensemble parameters. Do not set this manually.

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
    # Flexible identifier (preferred)
    p.add_argument('-e','--ensemble', required=False,
                   help='Ensemble ID, directory path, or "." for current directory')
    # Legacy integer-only option for backward compatibility
    p.add_argument('--ensemble-id', dest='ensemble_id', type=int, required=False,
                   help='[DEPRECATED] Ensemble ID (use -e/--ensemble for flexible ID or path)')
    p.add_argument('-j','--job-params', default='',
                   help=f'Space-separated key=val for SLURM job parameters. Required: {REQUIRED_JOB_PARAMS}')
    p.add_argument('-w','--wit-params', default='',
                   help='Space-separated key=val for WIT parameters using dot notation. Required: Configurations.first, Configurations.last')
    p.add_argument('--use-default-params', action='store_true',
                   help='Load parameters from ensemble default parameter file (mdwf_default_params.yaml)')
    p.add_argument('--params-variant',
                   help='Specify which parameter variant to use (e.g., default, wall, point)')
    p.add_argument('-o','--output-file', help='Output SBATCH script path (auto-generated if not specified)')
    p.add_argument('--save-default-params', action='store_true',
                   help='Save current command parameters to default parameter file for later reuse')
    p.add_argument('--save-params-as',
                   help='Save current parameters under specific variant name (default: default)')
    p.set_defaults(func=do_zv)

def do_zv(args):
    # Resolve ensemble from flexible identifier first, then legacy --ensemble-id
    ensemble_id = None
    ens = None
    if getattr(args, 'ensemble', None):
        eid, info = resolve_ensemble_identifier(args.db_file, args.ensemble)
        if eid is None:
            print(f"ERROR: Ensemble not found: {args.ensemble}", file=sys.stderr)
            return 1
        ensemble_id, ens = eid, info
    elif getattr(args, 'ensemble_id', None) is not None:
        ens = get_ensemble_details(args.db_file, args.ensemble_id)
        if not ens:
            print(f"ERROR: ensemble {args.ensemble_id} not found", file=sys.stderr)
            return 1
        ensemble_id = args.ensemble_id
    else:
        print("ERROR: Missing ensemble identifier. Use -e/--ensemble (ID or path) or --ensemble-id.", file=sys.stderr)
        return 1
    ens_dir = Path(ens['directory']).resolve()

    # Load parameters from config file if requested
    config_job_params = ""
    config_wit_params = ""
    
    if args.use_default_params:
        if args.params_variant:
            # Use specified variant
            config = get_operation_config(ens_dir, 'zv', args.params_variant)
            if config:
                config_job_params = config.get('job_params', '')
                config_wit_params = config.get('params', '')
                print(f"Loaded zv.{args.params_variant} default parameters from {get_config_path(ens_dir)}")
            else:
                config_path = get_config_path(ens_dir)
                print(f"Warning: No zv.{args.params_variant} default parameters found in {config_path}")
        else:
            # Try different parameter variants for zv (fallback behavior)
            config = None
            for meson_type in ['default', 'wall', 'point']:
                config = get_operation_config(ens_dir, 'zv', meson_type)
                if config:
                    config_job_params = config.get('job_params', '')
                    config_wit_params = config.get('params', '')
                    print(f"Loaded zv.{meson_type} default parameters from {get_config_path(ens_dir)}")
                    break
            
            if not config:
                config_path = get_config_path(ens_dir)
                if config_path.exists():
                    print(f"Warning: No zv default parameters found in {config_path}")
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

    # Require essential job parameters
    missing = [k for k in REQUIRED_JOB_PARAMS if k not in job_dict]
    if missing:
        if args.use_default_params:
            print(f"ERROR: missing required job parameters: {missing}. Add them to your default parameter file or use -j", file=sys.stderr)
        else:
            print(f"ERROR: missing required job parameters: {missing}", file=sys.stderr)
        return 1

    # Parse merged WIT parameters into nested dict - only parameters that go into DWF.in
    wdict = {}
    # Key alias normalization to allow shorthand without spaces in section names
    # These aliases are kept for backward compatibility and map to CLI underscore format
    section_alias = {
        'RNG': 'Random_number_generator',
        'Random number generator': 'Random_number_generator', # backward compatibility
        'Run name': 'Run_name', # backward compatibility
        'Exact Deflation': 'Exact_Deflation', # backward compatibility
        'Boundary conditions': 'Boundary_conditions', # backward compatibility
        'Propagator0': 'Propagator_0',
        'Solver0': 'Solver_0',
        'Lattice parameters': 'Lattice_parameters', # backward compatibility
    }
    def normalize_keypath(key: str) -> list:
        parts = key.split('.')
        if not parts:
            return parts
        # Normalize first section name if aliased
        head = section_alias.get(parts[0], parts[0])
        return [head] + parts[1:]
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
        parts = normalize_keypath(key)
        d = wdict
        for p in parts[:-1]:
            if p not in d or not isinstance(d[p], dict):
                d[p] = {}
            d = d[p]
        d[parts[-1]] = val

    # List of valid WIT parameters (only those that go into DWF.in) - CLI underscore format
    valid_wit_params = {
        'Run_name.name', 'Directories.cnfg_dir', 'Configurations.first', 'Configurations.last', 'Configurations.step',
        'Random_number_generator.level', 'Random_number_generator.seed', 'Lattice_parameters.Ls', 
        'Lattice_parameters.M5', 'Lattice_parameters.b', 'Lattice_parameters.c', 'Boundary_conditions.type',
        'Witness.no_prop', 'Witness.no_solver', 'Solver_0.solver', 'Solver_0.nkv', 'Solver_0.isolv', 
        'Solver_0.nmr', 'Solver_0.ncy', 'Solver_0.nmx', 'Solver_0.exact_deflation','Exact_Deflation.Cheby_fine', 'Exact_Deflation.Cheby_smooth', 
        'Exact_Deflation.Cheby_coarse', 'Exact_Deflation.kappa', 'Exact_Deflation.res', 
        'Exact_Deflation.nmx', 'Exact_Deflation.Ns', 'Propagator_0.Noise', 'Propagator_0.Source', 
        'Propagator_0.Dilution', 'Propagator_0.pos', 'Propagator_0.mom', 'Propagator_0.twist', 
        'Propagator_0.kappa', 'Propagator_0.mu', 'Propagator_0.Seed', 'Propagator_0.idx_solver', 
        'Propagator_0.res', 'Propagator_0.sloppy_res', 'AMA.NEXACT', 'AMA.SLOPPY_PREC', 'AMA.NHITS', 'AMA.NT',
        'Propagator.Seed'  # Special parameter that applies to all propagators
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

    # Enforce required WIT parameters for zv
    # Note: Propagator.Seed applies to all propagators, RNG seed is optional
    required_wit_params = {'Configurations.first', 'Configurations.last', 'Configurations.step', 'Propagator.Seed'}
    present = set()
    def collect_keys(d, prefix=""):
        for k, v in d.items():
            full_key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                collect_keys(v, full_key)
            else:
                present.add(full_key)
    collect_keys(wdict)
    missing_wit = [k for k in required_wit_params if k not in present]
    if missing_wit:
        print(f"ERROR: missing required WIT parameters: {missing_wit}", file=sys.stderr)
        return 1

    # Handle Propagator.Seed parameter - apply to Propagator_0 (only propagator for Zv)
    if 'Propagator' in wdict and 'Seed' in wdict['Propagator']:
        propagator_seed = wdict['Propagator']['Seed']
        print(f"Applying Propagator.Seed={propagator_seed} to Propagator_0")
        
        # Apply to Propagator_0 section
        if 'Propagator_0' not in wdict:
            wdict['Propagator_0'] = {}
        wdict['Propagator_0']['Seed'] = propagator_seed
        
        # Remove the Propagator.Seed entry since it's not a real WIT parameter
        del wdict['Propagator']['Seed']
        # Remove empty Propagator section if it has no other parameters
        if not wdict['Propagator']:
            del wdict['Propagator']

    # Check for unused WIT parameters and warn
    unused_w = find_unused_keys(wdict, valid_wit_params)
    for param in unused_w:
        print(f"\n ERROR: WIT parameter '{param}' was provided but is not used in DWF.in \n", file=sys.stderr)
        sys.exit(1)
    
    # Remove unused keys from wdict
    remove_unused_keys(wdict, unused_w)

    # Use ensemble directory from earlier

    # Generate the script
    sbatch = generate_zv_sbatch(
        db_file        = args.db_file,
        ensemble_id    = ensemble_id,
        ensemble_dir   = str(ens_dir),
        custom_changes = wdict,
        output_file    = args.output_file,
        account        = job_dict.get('account'),
        constraint     = job_dict.get('constraint'),
        queue          = job_dict.get('queue'),
        time_limit     = job_dict.get('time_limit'),
        nodes          = int(job_dict.get('nodes')),
        gpus           = int(job_dict.get('gpus')),
        gpu_bind       = job_dict.get('gpu_bind'),
        mail_user      = job_dict.get('mail_user'),
        ranks          = int(job_dict.get('ranks', 4)),
        ogeom          = job_dict.get('ogeom'),
    )
    
    # Save parameters to default params if requested
    if args.save_default_params:
        save_variant = args.save_params_as if args.save_params_as else 'default'
        success = save_operation_config(
            ens_dir, 'zv', save_variant,
            job_params=merged_job_params,
            params=merged_wit_params
        )
        if success:
            print(f"Saved parameters to {get_config_path(ens_dir)}: zv.{save_variant}")
        else:
            print(f"Warning: Failed to save parameters to default params", file=sys.stderr)
    
    return 0