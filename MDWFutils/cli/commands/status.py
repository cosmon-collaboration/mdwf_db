#!/usr/bin/env python3
"""Display ensemble status and details."""

import argparse

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
   EID, NICK, beta, b, Ls, mc, ms, ml, L, T, N_CFG, LAST_OP, LAST_USER, STATUS
   
   Sorting options:
   • Default: Ensembles are sorted numerically by physics parameters
   • --sort-by-id: Ensembles are sorted by EID in numerical order

2. Detail mode (with --ensemble specified):
   Shows complete information for one ensemble including physics parameters,
   configuration details, HMC paths, measurement summaries, and operation history.

3. Operation detail mode (with --ensemble and --op specified):
   Shows complete information for a specific operation including all timing,
   SLURM details, execution context, chain information, and parameters.

4. Missing measurements mode (with --ensemble and --missing specified):
   Lists configuration numbers missing a specific measurement type.

EXAMPLES:
  mdwf_db status                    # List all ensembles
  mdwf_db status --sort-by-id       # List sorted by EID
  mdwf_db status -e 5               # Show ensemble 5 details
  mdwf_db status -e .               # Show current directory's ensemble
  mdwf_db status -e 5 --dir         # Print only the directory path
  mdwf_db status -e 5 --op 147      # Show operation 147 details
  mdwf_db status -e 5 --missing gauge_obs  # Configs missing gauge_obs
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    add_ensemble_argument(p, required=False)
    p.add_argument('--sort-by-id', action='store_true', help='Sort list output by ensemble ID')
    p.add_argument('--dir', action='store_true', help='Only print directory path in detail view')
    p.add_argument('--op', '--operation', type=int, metavar='OP_ID',
                   help='Show detailed information for a specific operation (requires -e)')
    p.add_argument('--missing', metavar='TYPE',
                   choices=['gauge_obs', 'mres', 'meson2pt'],
                   help='List config numbers missing measurements of TYPE (requires -e)')
    p.set_defaults(func=do_status)


def do_status(args):
    backend = get_backend_for_args(args)

    # Check if --op is used without -e
    if args.op and not args.ensemble:
        print("ERROR: --op requires -e/--ensemble")
        return 1
    
    # Check if --missing is used without -e
    if args.missing and not args.ensemble:
        print("ERROR: --missing requires -e/--ensemble")
        return 1

    if not args.ensemble:
        return _list_ensembles(backend, args)

    # Detail mode - show specific ensemble
    ensemble_id, ensemble = resolve_ensemble_from_args(args)
    if not ensemble:
        return 1

    if args.dir:
        print(ensemble['directory'])
        return 0

    # Missing measurements mode - show configs missing a measurement type
    if args.missing:
        return _show_missing_measurements(backend, ensemble_id, ensemble, args.missing)

    # Operation detail mode - show specific operation
    if args.op:
        operation = backend.get_operation(ensemble_id, args.op)
        if not operation:
            print(f"ERROR: Operation {args.op} not found for ensemble {ensemble_id}")
            return 1
        
        _print_operation_details(operation)
        return 0

    return _print_ensemble_details(backend, ensemble_id, ensemble)


def _show_missing_measurements(backend, ensemble_id, ensemble, measurement_type):
    """Show configs missing a specific measurement type."""
    cfg = ensemble.get('configurations', {})
    config_list = cfg.get('config_list', [])
    
    if not config_list:
        print(f"ERROR: No config list for ensemble {ensemble_id} (run 'mdwf scan' first)")
        return 1
    
    expected = set(config_list)
    measured = set(backend.get_measured_configs(ensemble_id, measurement_type))
    missing = sorted(expected - measured)
    
    if missing:
        print(f"Configs missing {measurement_type} for ensemble {ensemble_id}:")
        # Print in groups
        for i in range(0, len(missing), 15):
            chunk = missing[i:i+15]
            print("  " + ", ".join(str(c) for c in chunk))
        print(f"\nTotal: {len(missing)} missing out of {len(expected)}")
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
        
        # Calculate N_CFG if possible
        n_cfg = ''
        if cfg.get('total'):
            n_cfg = str(cfg['total'])
        elif cfg.get('first') is not None and cfg.get('last') is not None and cfg.get('increment'):
            try:
                n_cfg = str((cfg['last'] - cfg['first']) // cfg['increment'] + 1)
            except:
                pass
        
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
    headers = ['EID', 'NICK', 'beta', 'b', 'Ls', 'mc', 'ms', 'ml', 'L', 'T', 'N_CFG', 'LAST_OP', 'LAST_USER', 'STATUS']
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
    
    # Thermalization info
    cfg = ensemble.get('configurations', {})
    therm_cfg = cfg.get('thermalized')
    if therm_cfg is not None:
        print(f"Thermalized = config >= {therm_cfg}")

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
    
    # Gauge observables
    gauge_configs = set(backend.get_measured_configs(ensemble_id, 'gauge_obs'))
    print("\nGauge observables:")
    _print_measurement_summary(gauge_configs, config_set)
    
    # Mres
    mres_configs = set(backend.get_measured_configs(ensemble_id, 'mres'))
    print("\nMres (residual mass):")
    _print_measurement_summary(mres_configs, config_set)
    
    # Meson 2pt
    meson2pt_configs = set(backend.get_measured_configs(ensemble_id, 'meson2pt'))
    print("\nMeson 2pt correlators:")
    _print_measurement_summary(meson2pt_configs, config_set)

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
        
        op_headers = ['ID', 'TYPE', 'STATUS', 'UPDATED', 'USER', 'RANGE', 'JOB', 'EXIT']
        print_table(op_headers, op_rows)

    return 0


def _print_measurement_summary(measured_configs, config_set):
    """Print summary for a measurement type."""
    if measured_configs or config_set:
        # Configs that exist AND have measurements
        have_both = config_set & measured_configs
        # Configs that exist but are missing measurements
        missing = config_set - measured_configs
        # Measurements for configs that no longer exist
        orphaned = measured_configs - config_set
        
        if config_set:
            print(f"  Measured: {len(have_both)}/{len(config_set)} configs")
        else:
            print(f"  Measured: {len(measured_configs)} configs")
        
        if measured_configs:
            print(f"  Range:    {min(measured_configs)}-{max(measured_configs)}")
        
        if missing:
            print(f"  Missing:  {len(missing)} configs")
        
        if orphaned:
            print(f"  Orphaned: {len(orphaned)} measurements (configs no longer exist)")
    else:
        print("  (none)")


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
