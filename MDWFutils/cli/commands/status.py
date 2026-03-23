#!/usr/bin/env python3
"""Display ensemble status and details."""

import argparse
import sys

from ..ensemble_utils import add_ensemble_argument, resolve_ensemble_from_args, get_backend_for_args
from ..formatting import print_table, format_float, safe_float, safe_int


def register(subparsers):
    p = subparsers.add_parser(
        'status',
        help='List ensembles or show details for one ensemble',
        description="""
Display ensemble status information.

MODES:

1. List mode (no --ensemble specified):
   Shows a table of all ensembles with columns:
   EID, NICK, beta, b, Ls, mc, ms, ml, L, T, N_CFG, C_therm, LAST_OP, LAST_USER, STATUS
   
   Sorting options:
   • Default: Ensembles are sorted numerically by physics parameters
   • --sort-by-id: Ensembles are sorted by EID in numerical order

2. Detail mode (with --ensemble specified):
   Shows complete information for one ensemble including physics parameters,
   configuration details, HMC paths, measurement summaries, and operation history.

3. Operation detail mode (with --ensemble and --op specified):
   Shows complete information for a specific operation including all timing,
   SLURM details, execution context, chain information, and parameters.

4. Missing measurements mode (with --ensemble and --missing TYPE):
   Lists configuration numbers in config_list that lack TYPE in the database.
   By default, only configs >= configurations.thermalized are considered expected
   (or all configs in config_list if thermalized is not set).
   With --cfg-range START END, expected configs are those in config_list with
   START <= c <= END (inclusive); thermalization is not applied in that case.

5. Measured configs mode (with --ensemble and --measured TYPE):
   Prints config numbers that already have TYPE stored in the database, one per line
   (for scripting: wc -l, xargs, etc.). Cannot be combined with --missing.

6. Measurements table mode (with --measurements specified):
   Shows a table of measurement counts for ensembles.
   Use "all" to show all measurement types, or specify: gauge_obs mres meson2pt

EXAMPLES:
  mdwf_db status                    # List all ensembles
  mdwf_db status --sort-by-id       # List sorted by EID
  mdwf_db status -e 5               # Show ensemble 5 details
  mdwf_db status -e .               # Show current directory's ensemble
  mdwf_db status -e 5 --dir         # Print only the directory path
  mdwf_db status -e 5 --op 147      # Show operation 147 details
  mdwf_db status -e 5 --missing gauge_obs  # Configs missing gauge_obs
  mdwf_db status -e 5 --measured meson2pt  # Config numbers with meson2pt in DB (one per line)
  mdwf_db status -e 5 --missing meson2pt --cfg-range 100 500  # Missing in [100,500] ∩ config_list
  mdwf_db status --measurements all  # Show all measurement types for all ensembles
  mdwf_db status --measurements gauge_obs mres  # Show only gauge_obs and mres
  mdwf_db status -e 5 7 --measurements all  # Show measurements for ensembles 5 and 7
  mdwf_db status -e PRODUCTION --measurements all  # Show measurements for all PRODUCTION ensembles
  mdwf_db status -e TUNING --measurements all  # Show measurements for all TUNING ensembles
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument('-e', '--ensemble', nargs='*', metavar='ENSEMBLE',
                   help='Ensemble identifier(s): ID, path, ".", "all", "PRODUCTION", or "TUNING". Can specify multiple.')
    p.add_argument('--sort-by-id', action='store_true', help='Sort list output by ensemble ID')
    p.add_argument('--dir', action='store_true', help='Only print directory path in detail view')
    p.add_argument('--op', '--operation', type=int, metavar='OP_ID',
                   help='Show detailed information for a specific operation (requires -e)')
    p.add_argument('--missing', metavar='TYPE',
                   choices=['gauge_obs', 'mres', 'meson2pt'],
                   help='List configs in config_list missing TYPE in the DB (requires -e). '
                        'Default: expected = thermalized configs (or all if C_therm unset). '
                        'Use --cfg-range to restrict expected set to [START,END] ∩ config_list.')
    p.add_argument('--measured', metavar='TYPE',
                   choices=['gauge_obs', 'mres', 'meson2pt'], default=None,
                   help='Print config numbers that have TYPE in the DB, one per line (requires -e). '
                        'Not combinable with --missing.')
    p.add_argument('--cfg-range', nargs=2, type=int, metavar=('START', 'END'), default=None,
                   help='With --missing only: expected configs are config_list ∩ [START,END] inclusive. '
                        'Does not apply thermalization filter.')
    p.add_argument('--measurements', nargs='*', metavar='TYPE',
                   choices=['all', 'gauge_obs', 'mres', 'meson2pt'],
                   help='Show measurement counts table. Use "all" for all types, or specify: gauge_obs mres meson2pt. Use with -e to filter by ensemble(s).')
    p.set_defaults(func=do_status)


def do_status(args):
    backend = get_backend_for_args(args)

    # Measurements mode - show measurement counts table (supports multiple ensembles)
    if args.measurements is not None:
        return _show_measurements_table(backend, args)

    # Check if --op is used without -e
    if args.op and not args.ensemble:
        print("ERROR: --op requires -e/--ensemble")
        return 1
    
    # Check if --missing is used without -e
    if args.missing and not args.ensemble:
        print("ERROR: --missing requires -e/--ensemble")
        return 1

    if getattr(args, 'measured', None) and not args.ensemble:
        print("ERROR: --measured requires -e/--ensemble", file=sys.stderr)
        return 1

    if args.cfg_range is not None and not args.missing:
        print("ERROR: --cfg-range is only valid with --missing", file=sys.stderr)
        return 1

    if getattr(args, 'measured', None) and args.missing:
        print("ERROR: --measured and --missing cannot be used together", file=sys.stderr)
        return 1

    if not args.ensemble:
        return _list_ensembles(backend, args)

    # Detail/missing/op modes - require single ensemble
    # If multiple provided, use first one
    ensemble_arg = args.ensemble[0] if isinstance(args.ensemble, list) else args.ensemble
    if isinstance(args.ensemble, list) and len(args.ensemble) > 1:
        print(f"WARNING: Multiple ensembles specified. Using first: {ensemble_arg}", file=sys.stderr)
    
    # Create a temporary args-like object with single ensemble for resolve_ensemble_from_args
    class SingleEnsembleArgs:
        def __init__(self, ensemble_value):
            self.ensemble = ensemble_value
    
    temp_args = SingleEnsembleArgs(ensemble_arg)
    ensemble_id, ensemble = resolve_ensemble_from_args(temp_args)
    if not ensemble:
        return 1

    if args.dir:
        print(ensemble['directory'])
        return 0

    # Measured configs in DB (one per line)
    if getattr(args, 'measured', None):
        return _show_measured_configs(backend, ensemble_id, args.measured)

    # Missing measurements mode - show configs missing a measurement type
    if args.missing:
        cfg_range = tuple(args.cfg_range) if args.cfg_range is not None else None
        return _show_missing_measurements(
            backend, ensemble_id, ensemble, args.missing, cfg_range=cfg_range,
        )

    # Operation detail mode - show specific operation
    if args.op:
        operation = backend.get_operation(ensemble_id, args.op)
        if not operation:
            print(f"ERROR: Operation {args.op} not found for ensemble {ensemble_id}")
            return 1
        
        _print_operation_details(operation)
        return 0

    return _print_ensemble_details(backend, ensemble_id, ensemble)


def _show_measured_configs(backend, ensemble_id, measurement_type):
    """Print config numbers that have measurements in the DB, one per line."""
    measured = sorted(backend.get_measured_configs(ensemble_id, measurement_type))
    for c in measured:
        print(c)
    return 0


def _show_missing_measurements(
    backend,
    ensemble_id,
    ensemble,
    measurement_type,
    cfg_range=None,
):
    """Show configs missing a specific measurement type.

    cfg_range: if (c_i, c_f), expected = config_list ∩ [c_i, c_f] (no thermal filter).
    Otherwise expected = thermalized configs in config_list (or all if thermalized unset).
    """
    cfg = ensemble.get('configurations', {})
    config_list = cfg.get('config_list', [])
    
    if not config_list:
        print(f"ERROR: No config list for ensemble {ensemble_id} (run 'mdwf scan' first)")
        return 1
    
    config_set = set(config_list)
    therm_cfg = cfg.get('thermalized')

    if cfg_range is not None:
        c_i, c_f = cfg_range[0], cfg_range[1]
        if c_i > c_f:
            print("ERROR: --cfg-range START must be <= END", file=sys.stderr)
            return 1
        expected = {c for c in config_set if c_i <= c <= c_f}
        range_note = f"config_list ∩ [{c_i}, {c_f}]"
    elif therm_cfg is not None:
        expected = {c for c in config_set if c >= therm_cfg}
        range_note = None
    else:
        expected = config_set
        range_note = None
    
    measured = set(backend.get_measured_configs(ensemble_id, measurement_type))
    missing = sorted(expected - measured)
    
    if missing:
        print(f"Configs missing {measurement_type} for ensemble {ensemble_id}:")
        if cfg_range is not None:
            print(f"(Expected set: {range_note}, {len(expected)} configs)")
        elif therm_cfg is not None:
            print(f"(Only showing thermalized configs >= {therm_cfg})")
        else:
            print("(Note: thermalization threshold not set, showing all configs in config_list)")
        # Print in groups
        for i in range(0, len(missing), 15):
            chunk = missing[i:i+15]
            print("  " + ", ".join(str(c) for c in chunk))
        if cfg_range is not None:
            print(f"\nTotal: {len(missing)} missing out of {len(expected)} configs in range")
        elif therm_cfg is not None:
            print(f"\nTotal: {len(missing)} missing out of {len(expected)} thermalized configs")
        else:
            print(f"\nTotal: {len(missing)} missing out of {len(expected)} configs")
    else:
        if cfg_range is not None:
            print(f"All configs in {range_note} have {measurement_type} measurements for ensemble {ensemble_id}")
        elif therm_cfg is not None:
            print(f"All thermalized configs have {measurement_type} measurements for ensemble {ensemble_id}")
        else:
            print(f"All configs have {measurement_type} measurements for ensemble {ensemble_id}")
    
    return 0


def _list_ensembles(backend, args):
    """List all ensembles in a table."""
    ensembles = backend.list_ensembles(detailed=True)
    if not ensembles:
        print('No ensembles found')
        return 0

    # Build table data
    rows = []
    for ens in ensembles:
        ensemble_id = ens.get('ensemble_id', '')
        physics = ens.get('physics', {})
        cfg = ens.get('configurations', {})
        config_set = set(cfg.get('config_list', []))
        therm_cfg = cfg.get('thermalized')

        if therm_cfg is not None:
            thermalized_config_set = {c for c in config_set if c >= therm_cfg}
            c_therm = therm_cfg
        else:
            # When thermalization is not set, show all configs but indicate it's unknown
            thermalized_config_set = config_set
            c_therm = '?'  # Indicates thermalization threshold not set
        n_cfg = len(thermalized_config_set)


        # Get last operation info
        last_op = ''
        last_user = ''
        try:
            ops = backend.list_operations(ensemble_id)
            if ops:
                # Get the most recent operation by update_time
                latest = max(ops, key=lambda o: o.get('timing', {}).get('update_time') or '')
                last_op = latest.get('operation_type', '')
                last_user = latest.get('slurm', {}).get('user', '')
        except:
            pass
        
        row = {
            'EID': ensemble_id,
            'NICK': ens.get('nickname', ''),
            'beta': physics.get('beta', ''),
            'b': physics.get('b', ''),
            'Ls': physics.get('Ls', ''),
            'mc': physics.get('mc', ''),
            'ms': physics.get('ms', ''),
            'ml': physics.get('ml', ''),
            'L': physics.get('L', ''),
            'T': physics.get('T', ''),
            'N_CFG': n_cfg,
            'C_therm': c_therm,
            'LAST_OP': last_op,
            'LAST_USER': last_user,
            'STATUS': ens.get('status', ''),
        }
        rows.append(row)

    # Sort ensembles
    if args.sort_by_id:
        rows.sort(key=lambda r: r['EID'])
    else:
        # Sort by physics parameters
        def sort_key(row):
            return (
                safe_float(row['beta']),
                safe_float(row['b']),
                safe_int(row['Ls']),
                safe_float(row['mc']),
                safe_float(row['ms']),
                safe_float(row['ml']),
                safe_int(row['L']),
                safe_int(row['T']),
            )
        rows.sort(key=sort_key)

    # Print table
    headers = ['EID', 'NICK', 'beta', 'b', 'Ls', 'mc', 'ms', 'ml', 'L', 'T', 'N_CFG', 'C_therm', 'LAST_OP', 'LAST_USER', 'STATUS']
    print_table(headers, rows)
    return 0


def _print_ensemble_details(backend, ensemble_id, ensemble):
    """Print detailed information about an ensemble."""
    # Print ensemble header
    print(f"ID        = {ensemble_id}")
    print(f"Directory = {ensemble['directory']}")
    print(f"Status    = {ensemble.get('status', 'UNKNOWN')}")
    if ensemble.get('nickname'):
        print(f"Nickname  = {ensemble['nickname']}")
    if ensemble.get('description'):
        print(f"Description = {ensemble['description']}")

    # Physics parameters
    physics = ensemble.get('physics', {})
    if physics:
        print('\nPhysics parameters:')
        for key in ['beta', 'b', 'Ls', 'ml', 'ms', 'mc', 'L', 'T']:
            if key in physics:
                print(f"  {key} = {physics[key]}")

    # Configuration range
    cfg = ensemble.get('configurations', {})
    if cfg and any(cfg.values()):
        parts = []
        if cfg.get('first') is not None and cfg.get('last') is not None:
            parts.append(f"range: {cfg['first']}-{cfg['last']}")
        if cfg.get('increment'):
            parts.append(f"step: {cfg['increment']}")
        if cfg.get('total'):
            parts.append(f"total: {cfg['total']}")
        if parts:
            print("Config    = " + ", ".join(parts))

    # HMC paths
    hmc_paths = ensemble.get('hmc_paths', {})
    if hmc_paths and any(hmc_paths.values()):
        print("\nHMC paths:")
        if hmc_paths.get('exec_path'):
            print(f"  exec_path       = {hmc_paths['exec_path']}")
        if hmc_paths.get('bind_script_gpu'):
            print(f"  bind_script_gpu = {hmc_paths['bind_script_gpu']}")
        if hmc_paths.get('bind_script_cpu'):
            print(f"  bind_script_cpu = {hmc_paths['bind_script_cpu']}")

    # Measurements summary
    config_set = set(cfg.get('config_list', []))
    therm_cfg = cfg.get('thermalized')
    
    # Filter to thermalized configs
    if therm_cfg is not None:
        thermalized_config_set = {c for c in config_set if c >= therm_cfg}
    else:
        thermalized_config_set = config_set
    
    print("\nMeasurements:")
    if therm_cfg is not None:
        print(f"  C_therm: {therm_cfg}")
        if thermalized_config_set:
            print(f"  Total thermalized configs: {len(thermalized_config_set)}")
    else:
        print(f"  C_therm: UNKNOWN (showing all configs)")
        if thermalized_config_set:
            print(f"  Total configs: {len(thermalized_config_set)}")
    print()
    
    # Gauge observables
    gauge_configs = set(backend.get_measured_configs(ensemble_id, 'gauge_obs'))
    print("  gauge_obs:")
    _print_measurement_summary(gauge_configs, config_set, therm_cfg)
    
    # Mres
    mres_configs = set(backend.get_measured_configs(ensemble_id, 'mres'))
    print("  mres:")
    _print_measurement_summary(mres_configs, config_set, therm_cfg)
    
    # Meson 2pt
    meson2pt_configs = set(backend.get_measured_configs(ensemble_id, 'meson2pt'))
    print("  meson2pt:")
    _print_measurement_summary(meson2pt_configs, config_set, therm_cfg)

    # Operations summary table
    print('\nOperations:')
    ops = backend.list_operations(ensemble_id)
    if not ops:
        print('  (none)')
    else:
        op_rows = []
        for op in ops:
            timing = op.get('timing', {})
            slurm = op.get('slurm', {})
            execution = op.get('execution', {})
            
            # Build config range string
            cfg_range = ''
            if execution.get('config_start') is not None and execution.get('config_end') is not None:
                cfg_range = f"{execution['config_start']}-{execution['config_end']}"
                if execution.get('config_increment'):
                    cfg_range += f"({execution['config_increment']})"
            
            op_rows.append({
                'ID': op.get('operation_id', ''),
                'TYPE': op.get('operation_type', ''),
                'STATUS': op.get('status', ''),
                'UPDATED': str(timing.get('update_time', ''))[:19] if timing.get('update_time') else '',
                'USER': slurm.get('user', ''),
                'RANGE': cfg_range,
                'JOB': slurm.get('job_id', ''),
                'EXIT': slurm.get('exit_code', ''),
            })
        
        # Sort by operation ID descending (most recent first)
        op_rows.sort(key=lambda r: safe_int(r['ID']) if r['ID'] else 0, reverse=True)
        
        op_headers = ['ID', 'TYPE', 'STATUS', 'UPDATED', 'USER', 'RANGE', 'JOB', 'EXIT']
        print_table(op_headers, op_rows)

    return 0


def _print_measurement_summary(measured_configs, config_set, therm_cfg=None):
    """Print summary for a measurement type.
    
    Args:
        measured_configs: Set of config numbers that have measurements
        config_set: Set of all config numbers in the ensemble
        therm_cfg: Thermalized config number (only count >= this)
    """
    if measured_configs or config_set:
        # Filter to thermalized configs
        if therm_cfg is not None:
            thermalized_config_set = {c for c in config_set if c >= therm_cfg}
        else:
            thermalized_config_set = config_set
        
        # Configs that exist AND have measurements (filtered to thermalized if set)
        have_both = thermalized_config_set & measured_configs
        missing = thermalized_config_set - measured_configs
        
        # Measurements for configs that no longer exist
        orphaned = measured_configs - config_set
        
        if thermalized_config_set:
            print(f"    Measured: {len(have_both)}/{len(thermalized_config_set)} configs")
        else:
            print(f"    Measured: {len(measured_configs)} configs")
        
        if measured_configs:
            print(f"    Range:    {min(measured_configs)}-{max(measured_configs)}")
        
        if missing:
            if therm_cfg is not None:
                print(f"    Missing:  {len(missing)} configs (>= {therm_cfg})")
            else:
                print(f"    Missing:  {len(missing)} configs")
        
        if orphaned:
            print(f"    Orphaned: {len(orphaned)} (configs no longer exist)")
    else:
        print("    (none)")


def _print_operation_details(operation):
    """Print detailed information about a single operation."""
    print(f"Operation ID: {operation.get('operation_id')}")
    print(f"Ensemble ID:  {operation.get('ensemble_id')}")
    print(f"Type:         {operation.get('operation_type')}")
    print(f"Status:       {operation.get('status')}")
    print(f"Directory:    {operation.get('ensemble_directory', 'N/A')}")
    
    # Timing section
    timing = operation.get('timing', {})
    if timing:
        print("\nTiming:")
        if timing.get('creation_time'):
            print(f"  Created:  {str(timing['creation_time'])[:19]}")
        if timing.get('start_time'):
            print(f"  Started:  {str(timing['start_time'])[:19]}")
        if timing.get('update_time'):
            print(f"  Updated:  {str(timing['update_time'])[:19]}")
        if timing.get('end_time'):
            print(f"  Ended:    {str(timing['end_time'])[:19]}")
        if timing.get('runtime_seconds') is not None:
            runtime = timing['runtime_seconds']
            hours = runtime // 3600
            minutes = (runtime % 3600) // 60
            seconds = runtime % 60
            print(f"  Runtime:  {runtime}s ({hours}h {minutes}m {seconds}s)")
    
    # SLURM section
    slurm = operation.get('slurm', {})
    if slurm and any(slurm.values()):
        print("\nSLURM:")
        if slurm.get('job_id'):
            print(f"  Job ID:      {slurm['job_id']}")
        if slurm.get('user'):
            print(f"  User:        {slurm['user']}")
        if slurm.get('host'):
            print(f"  Host:        {slurm['host']}")
        if slurm.get('exit_code') is not None:
            print(f"  Exit code:   {slurm['exit_code']}")
        if slurm.get('slurm_status'):
            print(f"  SLURM status: {slurm['slurm_status']}")
        if slurm.get('batch_script'):
            print(f"  Batch script: {slurm['batch_script']}")
        if slurm.get('output_log'):
            print(f"  Output log:   {slurm['output_log']}")
        if slurm.get('error_log'):
            print(f"  Error log:    {slurm['error_log']}")
    
    # Execution section
    execution = operation.get('execution', {})
    if execution and any(execution.values()):
        print("\nExecution:")
        if execution.get('run_dir'):
            print(f"  Run dir:    {execution['run_dir']}")
        if execution.get('config_start') is not None:
            print(f"  Config start: {execution['config_start']}")
        if execution.get('config_end') is not None:
            print(f"  Config end:   {execution['config_end']}")
        if execution.get('config_increment') is not None:
            print(f"  Config step:  {execution['config_increment']}")
    
    # Chain section
    chain = operation.get('chain', {})
    if chain and (chain.get('is_chain_member') or chain.get('parent_operation_id')):
        print("\nChain:")
        print(f"  Is chain member: {chain.get('is_chain_member', False)}")
        if chain.get('parent_operation_id'):
            print(f"  Parent op ID:    {chain['parent_operation_id']}")
        print(f"  Attempt number:  {chain.get('attempt_number', 1)}")
    
    # Additional parameters
    params = operation.get('params', {})
    if params:
        print("\nAdditional parameters:")
        for key, value in sorted(params.items()):
            print(f"  {key} = {value}")


def _resolve_ensembles(backend, ensemble_args):
    """Resolve ensemble arguments to list of (ensemble_id, ensemble_doc) tuples.
    
    Special keywords (only work when used alone):
    - "all": all ensembles
    - "PRODUCTION": all ensembles with status='PRODUCTION'
    - "TUNING": all ensembles with status='TUNING'
    """
    results = []
    
    # Handle special keywords (only when used alone)
    if len(ensemble_args) == 1:
        keyword = ensemble_args[0].upper()
        if keyword == 'ALL':
            all_ensembles = backend.list_ensembles(detailed=True)
            for ens in all_ensembles:
                results.append((ens['ensemble_id'], ens))
            return results
        elif keyword == 'PRODUCTION':
            all_ensembles = backend.list_ensembles(detailed=True)
            for ens in all_ensembles:
                # Check both status field and directory path
                status = ens.get('status', '').upper()
                directory = ens.get('directory', '').upper()
                if status == 'PRODUCTION' or 'ENSEMBLES' in directory:
                    results.append((ens['ensemble_id'], ens))
            return results
        elif keyword == 'TUNING':
            all_ensembles = backend.list_ensembles(detailed=True)
            for ens in all_ensembles:
                # Check both status field and directory path
                status = ens.get('status', '').upper()
                directory = ens.get('directory', '').upper()
                if status == 'TUNING' or 'TUNING' in directory:
                    results.append((ens['ensemble_id'], ens))
            return results
    
    # Handle regular ensemble identifiers (keywords not allowed with multiple values)
    for identifier in ensemble_args:
        keyword = identifier.upper()
        if keyword in ('ALL', 'PRODUCTION', 'TUNING'):
            print(f"WARNING: Keyword '{identifier}' ignored when used with other identifiers. Use alone or use ensemble IDs.", file=sys.stderr)
            continue
        try:
            ens_id, ens = backend.resolve_ensemble_identifier(identifier)
            results.append((ens_id, ens))
        except Exception as e:
            print(f"WARNING: Could not resolve ensemble '{identifier}': {e}", file=sys.stderr)
    
    return results


def _show_measurements_table(backend, args):
    """Print measurement counts table for ensembles."""
    # Determine which measurement types to show
    if not args.measurements or 'all' in args.measurements:
        measurement_types = ['gauge_obs', 'mres', 'meson2pt']
    else:
        measurement_types = [m for m in args.measurements if m != 'all']
    
    # Get ensembles to show
    if args.ensemble:
        # Use specified ensemble(s) - args.ensemble is a list when nargs='*'
        ensembles = _resolve_ensembles(backend, args.ensemble)
        if not ensembles:
            print("No ensembles found", file=sys.stderr)
            return 1
    else:
        # Use all ensembles
        all_ensembles = backend.list_ensembles(detailed=True)
        ensembles = [(ens['ensemble_id'], ens) for ens in all_ensembles]
    
    if not ensembles:
        print('No ensembles found')
        return 0
    
    # Build table rows - one row per ensemble
    rows = []
    for ensemble_id, ensemble in ensembles:
        physics = ensemble.get('physics', {})
        cfg = ensemble.get('configurations', {})
        config_set = set(cfg.get('config_list', []))
        therm_cfg = cfg.get('thermalized')
        nickname = ensemble.get('nickname', '')
        
        # Filter config_set to only thermalized configs
        if therm_cfg is not None:
            thermalized_config_set = {c for c in config_set if c >= therm_cfg}
        else:
            thermalized_config_set = config_set
        
        # Start building row with ensemble info
        # Include physics params for sorting but they won't be displayed
        row = {
            'EID': ensemble_id,
            'NICK': nickname,
            'N_CFG': len(thermalized_config_set),
            # Physics params for sorting (not displayed)
            '_beta': physics.get('beta', ''),
            '_b': physics.get('b', ''),
            '_Ls': physics.get('Ls', ''),
            '_mc': physics.get('mc', ''),
            '_ms': physics.get('ms', ''),
            '_ml': physics.get('ml', ''),
            '_L': physics.get('L', ''),
            '_T': physics.get('T', ''),
        }
        
        # Add columns for each measurement type
        for mtype in measurement_types:
            measured_configs = set(backend.get_measured_configs(ensemble_id, mtype))
            # Count only thermalized measured configs
            thermalized_measured = measured_configs & thermalized_config_set
            
            # Add column for this measurement type
            row[f'{mtype}_MEASURED'] = len(thermalized_measured)
        
        rows.append(row)
    
    # Sort ensembles
    if args.sort_by_id:
        rows.sort(key=lambda r: r['EID'])
    else:
        # Sort by physics parameters (same as default status table)
        def sort_key(row):
            return (
                safe_float(row['_beta']),
                safe_float(row['_b']),
                safe_int(row['_Ls']),
                safe_float(row['_mc']),
                safe_float(row['_ms']),
                safe_float(row['_ml']),
                safe_int(row['_L']),
                safe_int(row['_T']),
            )
        rows.sort(key=sort_key)
    
    # Build headers dynamically based on measurement types (exclude physics params)
    headers = ['EID', 'NICK', 'N_CFG']
    for mtype in measurement_types:
        headers.append(f'{mtype}_MEASURED')
    
    # Filter rows to only include displayed columns
    display_rows = [{k: v for k, v in row.items() if not k.startswith('_')} for row in rows]
    
    print_table(headers, display_rows)
    return 0