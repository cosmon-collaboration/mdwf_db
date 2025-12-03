#!/usr/bin/env python3
"""List ensembles or show details using the new backend."""

import argparse

from ..ensemble_utils import resolve_ensemble_from_args, get_backend_for_args


def register(subparsers):
    p = subparsers.add_parser(
        'query',
        help='List ensembles or show details for one ensemble',
        description="""
Query the MDWF database for ensemble information.

TWO MODES:

1. List mode (no --ensemble specified):
   Shows a spreadsheet-like table of all ensembles with columns:
   EID, NICK, beta, b, Ls, mc, ms, ml, L, T, N_CFG, STATUS
   
   Sorting options:
   • Default: Ensembles are sorted numerically by physics parameters
   • --sort-by-id: Ensembles are sorted by EID in numerical order

2. Detail mode (with --ensemble specified):
   Shows complete information for one ensemble including physics parameters,
   configuration details, HMC paths, and operation history.

EXAMPLES:
  mdwf_db query                    # List all ensembles (sorted by parameters)
  mdwf_db query --sort-by-id       # List all ensembles sorted by EID
  mdwf_db query -e 1               # Show ensemble 1 details
  mdwf_db query -e .               # Show current ensemble
  mdwf_db query -e 1 --dir         # Show only the directory path
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument('-e', '--ensemble', help='Ensemble identifier for detail view')
    p.add_argument('--detailed', action='store_true', help='Show extended operation details in detail view')
    p.add_argument('--sort-by-id', action='store_true', help='Sort list output by ensemble ID')
    p.add_argument('--dir', action='store_true', help='Only print directory path in detail view')
    p.set_defaults(func=do_query)


def do_query(args):
    backend = get_backend_for_args(args)

    if not args.ensemble:
        ensembles = backend.list_ensembles(detailed=True)
        if not ensembles:
            print('No ensembles found')
            return 0

        # Build table data
        rows = []
        for ens in ensembles:
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
            
            row = {
                'EID': ens.get('ensemble_id', ''),
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
                    float(row['beta']) if row['beta'] else 999,
                    float(row['b']) if row['b'] else 999,
                    int(row['Ls']) if row['Ls'] else 999,
                    float(row['mc']) if row['mc'] else 999,
                    float(row['ms']) if row['ms'] else 999,
                    float(row['ml']) if row['ml'] else 999,
                    int(row['L']) if row['L'] else 999,
                    int(row['T']) if row['T'] else 999,
                )
            rows.sort(key=sort_key)

        # Print table
        headers = ['EID', 'NICK', 'beta', 'b', 'Ls', 'mc', 'ms', 'ml', 'L', 'T', 'N_CFG', 'STATUS']
        _print_table(headers, rows)
        return 0

    # Detail mode - show specific ensemble
    ensemble_id, ensemble = resolve_ensemble_from_args(args)
    if not ensemble:
        return 1

    if args.dir:
        print(ensemble['directory'])
        return 0

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
        _print_table(op_headers, op_rows)

    return 0


def _print_table(headers, rows):
    """Print a formatted table with proper column alignment."""
    if isinstance(rows[0], dict):
        # rows are dictionaries
        widths = {h: len(str(h)) for h in headers}
        for row in rows:
            for h in headers:
                widths[h] = max(widths[h], len(str(row.get(h, ''))))
        
        header_line = "  ".join(str(h).ljust(widths[h]) for h in headers)
        print(header_line)
        print("  ".join("-" * widths[h] for h in headers))
        for row in rows:
            print("  ".join(str(row.get(h, '')).ljust(widths[h]) for h in headers))
    else:
        # rows are lists
        widths = [len(str(h)) for h in headers]
        for row in rows:
            for idx, value in enumerate(row):
                widths[idx] = max(widths[idx], len(str(value)))
        
        header_line = "  ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers))
        print(header_line)
        print("  ".join("-" * w for w in widths))
        for row in rows:
            print("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(headers))))
