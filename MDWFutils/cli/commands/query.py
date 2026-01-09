#!/usr/bin/env python3
"""List ensembles or show details using the new backend."""

import argparse
import math

from ..ensemble_utils import resolve_ensemble_from_args, get_backend_for_args


def register(subparsers):
    p = subparsers.add_parser(
        'query',
        help='List ensembles or show details for one ensemble',
        description="""
Query the MDWF database for ensemble information.

MODES:

1. List mode (no --ensemble specified):
   Shows a spreadsheet-like table of all ensembles with columns:
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
   Lists configuration numbers that are missing a specific measurement type.

5. Measurements display mode (with --ensemble and --measurements specified):
   Shows stored measurement data. Behavior varies by measurement type:
   
   gauge_obs: Shows table with plaquette, Q, t0, w0 for all configs
              Use --cfg-range START END to filter config range
   
   mres/meson2pt: Without --cfg, lists available configs
                  With --cfg CFG_NUM, shows full correlator data for that config
                  With --cfg CFG_NUM -t T1 T2 ..., shows specific timeslices only

MEASUREMENT TYPES:
  gauge_obs  - Gauge observables (plaquette, Q, t0, w0)
  mres       - Residual mass correlators (PP, MP for light/strange/charm)
  meson2pt   - Meson 2pt correlators (PP, AP for pion/kaon/eta_s/D/Ds/eta_c)

EXAMPLES:
  # List ensembles
  mdwf_db query                              # List all (sorted by parameters)
  mdwf_db query --sort-by-id                 # List all (sorted by EID)
  
  # Ensemble details
  mdwf_db query -e 1                         # Show ensemble 1 details
  mdwf_db query -e .                         # Show current directory's ensemble
  mdwf_db query -e 1 --dir                   # Print only the directory path
  mdwf_db query -e 21 --op 147               # Show operation 147 details
  
  # Missing measurements
  mdwf_db query -e 5 --missing gauge_obs     # Configs missing gauge_obs
  mdwf_db query -e 5 --missing mres          # Configs missing mres
  
  # Gauge observables (scalar values per config)
  mdwf_db query -e 5 --measurements gauge_obs                    # All configs
  mdwf_db query -e 5 --measurements gauge_obs --cfg-range 100 200
  
  # Mres correlators (arrays per config)
  mdwf_db query -e 5 --measurements mres                 # List available configs
  mdwf_db query -e 5 --measurements mres --cfg 124       # Full correlators for cfg 124
  mdwf_db query -e 5 --measurements mres --cfg 124 -t 0 16 32 48  # Specific timeslices
  
  # Meson 2pt correlators (arrays per config)
  mdwf_db query -e 5 --measurements meson2pt             # List available configs
  mdwf_db query -e 5 --measurements meson2pt --cfg 124   # Full correlators for cfg 124
  mdwf_db query -e 5 --measurements meson2pt --cfg 124 -t 0 16 32  # Specific timeslices
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument('-e', '--ensemble', help='Ensemble identifier for detail view')
    p.add_argument('--detailed', action='store_true', help='Show extended operation details in detail view')
    p.add_argument('--sort-by-id', action='store_true', help='Sort list output by ensemble ID')
    p.add_argument('--dir', action='store_true', help='Only print directory path in detail view')
    p.add_argument('--op', '--operation', type=int, metavar='OP_ID',
                   help='Show detailed information for a specific operation (requires -e)')
    p.add_argument('--missing', metavar='TYPE',
                   help='List config numbers missing measurements of TYPE (requires -e)')
    p.add_argument('--measurements', metavar='TYPE',
                   help='Show measurements of TYPE: gauge_obs, mres, or meson2pt (requires -e)')
    p.add_argument('--cfg-range', nargs=2, type=int, metavar=('START', 'END'),
                   help='Filter to config range START-END inclusive (use with --measurements gauge_obs)')
    p.add_argument('--cfg', type=int, metavar='CFG_NUM',
                   help='Show full correlator arrays for a single config (use with --measurements mres/meson2pt)')
    p.add_argument('-t', '--timeslices', nargs='+', type=int, metavar='T',
                   help='Show only specific timeslices, e.g. -t 0 16 32 48 (use with --cfg)')
    p.set_defaults(func=do_query)


def _safe_float(val, default=999.0):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _safe_int(val, default=999):
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def do_query(args):
    backend = get_backend_for_args(args)

    # Check if --op is used without -e
    if args.op and not args.ensemble:
        print("ERROR: --op requires -e/--ensemble")
        return 1
    
    # Check if --missing is used without -e
    if args.missing and not args.ensemble:
        print("ERROR: --missing requires -e/--ensemble")
        return 1
    
    # Check if --measurements is used without -e
    if args.measurements and not args.ensemble:
        print("ERROR: --measurements requires -e/--ensemble")
        return 1
    
    # Check if --cfg-range is used without --measurements
    if args.cfg_range and not args.measurements:
        print("ERROR: --cfg-range requires --measurements")
        return 1
    
    # Check if --cfg is used without --measurements
    if args.cfg and not args.measurements:
        print("ERROR: --cfg requires --measurements")
        return 1
    
    # Check if --timeslices is used without --cfg
    if args.timeslices and not args.cfg:
        print("ERROR: --timeslices (-t) requires --cfg")
        return 1

    if not args.ensemble:
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
                    _safe_float(row['beta']),
                    _safe_float(row['b']),
                    _safe_int(row['Ls']),
                    _safe_float(row['mc']),
                    _safe_float(row['ms']),
                    _safe_float(row['ml']),
                    _safe_int(row['L']),
                    _safe_int(row['T']),
                )
            rows.sort(key=sort_key)

        # Print table
        headers = ['EID', 'NICK', 'beta', 'b', 'Ls', 'mc', 'ms', 'ml', 'L', 'T', 'N_CFG', 'LAST_OP', 'LAST_USER', 'STATUS']
        _print_table(headers, rows)
        return 0

    # Detail mode - show specific ensemble
    ensemble_id, ensemble = resolve_ensemble_from_args(args)
    if not ensemble:
        return 1

    if args.dir:
        print(ensemble['directory'])
        return 0

    # Missing measurements mode - show configs missing a measurement type
    if args.missing:
        cfg = ensemble.get('configurations', {})
        config_list = cfg.get('config_list', [])
        if not config_list:
            print(f"ERROR: No config list for ensemble {ensemble_id} (run 'mdwf scan' first)")
            return 1
        
        expected = set(config_list)
        measured = set(backend.get_measured_configs(ensemble_id, args.missing))
        missing = sorted(expected - measured)
        
        if missing:
            print(f"Configs missing {args.missing} for ensemble {ensemble_id}: {', '.join(map(str, missing))}")
        else:
            print(f"All configs have {args.missing} measurements for ensemble {ensemble_id}")
        return 0
    
    # Measurements display mode - show stored measurements in table format
    if args.measurements:
        config_start = None
        config_end = None
        if args.cfg_range:
            config_start, config_end = args.cfg_range
        
        # If --cfg is specified, filter to just that config
        if args.cfg:
            config_start = args.cfg
            config_end = args.cfg
        
        measurements = backend.query_measurements(
            ensemble_id,
            args.measurements,
            config_start=config_start,
            config_end=config_end
        )
        
        if not measurements:
            print(f"No {args.measurements} measurements found for ensemble {ensemble_id}")
            if args.cfg:
                print(f"  (no data for config {args.cfg})")
            elif args.cfg_range:
                print(f"  (filtered to config range {config_start}-{config_end})")
            return 0
        
        # Format measurements as table
        _print_measurements_table(
            measurements,
            args.measurements,
            ensemble_id,
            single_cfg=args.cfg,
            timeslices=args.timeslices,
        )
        return 0
    
    # Operation detail mode - show specific operation
    if args.op:
        operation = backend.get_operation(ensemble_id, args.op)
        if not operation:
            print(f"ERROR: Operation {args.op} not found for ensemble {ensemble_id}")
            return 1
        
        _print_operation_details(operation)
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
        _print_table(op_headers, op_rows)

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


def _print_measurements_table(measurements, measurement_type, ensemble_id, single_cfg=None, timeslices=None):
    """Print measurements in a formatted table."""
    if not measurements:
        return
    
    # Special formatting for gauge_obs
    if measurement_type == 'gauge_obs':
        headers = ['CFG', 'PLAQ', 'Q', 'SQRT(T0_CLOV)', 'SQRT(T0_PLAQ)', 'W0_CLOV', 'W0_PLAQ']
        rows = []
        for m in measurements:
            data = m.get('data', {})
            row = {
                'CFG': m.get('config_number', ''),
                'PLAQ': _format_float(data.get('plaq')),
                'Q': _format_float(data.get('Q')),
                'SQRT(T0_CLOV)': _format_float(data.get('sqrt_t0_clov')),
                'SQRT(T0_PLAQ)': _format_float(data.get('sqrt_t0_plaq')),
                'W0_CLOV': _format_float(data.get('w0_clov')),
                'W0_PLAQ': _format_float(data.get('w0_plaq')),
            }
            rows.append(row)
        
        print(f"\nGauge observables for ensemble {ensemble_id} ({len(measurements)} configs)\n")
        _print_table(headers, rows)
    
    # Special formatting for mres
    elif measurement_type == 'mres':
        if single_cfg is not None:
            # Show full correlator data for a specific config
            _print_mres_correlators(measurements[0], ensemble_id, timeslices)
        else:
            # Summary mode: list available configs
            _print_mres_summary(measurements, ensemble_id)
    
    # Special formatting for meson2pt
    elif measurement_type == 'meson2pt':
        if single_cfg is not None:
            # Show full correlator data for a specific config
            _print_meson2pt_correlators(measurements[0], ensemble_id, timeslices)
        else:
            # Summary mode: list available configs
            _print_meson2pt_summary(measurements, ensemble_id)
    
    else:
        # Generic formatting for other measurement types
        # Extract all unique keys from data dicts
        all_keys = set()
        for m in measurements:
            all_keys.update(m.get('data', {}).keys())
        
        if not all_keys:
            print(f"No data fields found in {measurement_type} measurements")
            return
        
        headers = ['CFG'] + sorted(all_keys)
        rows = []
        for m in measurements:
            data = m.get('data', {})
            row = {'CFG': m.get('config_number', '')}
            for key in sorted(all_keys):
                value = data.get(key)
                if isinstance(value, float):
                    row[key] = _format_float(value)
                else:
                    row[key] = str(value) if value is not None else ''
            rows.append(row)
        
        print(f"\n{measurement_type} measurements for ensemble {ensemble_id} ({len(measurements)} configs)\n")
        _print_table(headers, rows)


def _format_float(value):
    """Format a float value, handling NaN."""
    if value is None:
        return ''
    try:
        if math.isnan(value):
            return 'NaN'
        return f"{value:.5f}".rstrip('0').rstrip('.')
    except (TypeError, ValueError):
        return str(value)


def _format_sci(value):
    """Format a float in scientific notation for correlator values."""
    if value is None:
        return ''
    try:
        if math.isnan(value):
            return 'NaN'
        # Use scientific notation for very small/large values
        if abs(value) < 1e-3 or abs(value) > 1e5:
            return f"{value:.4e}"
        return f"{value:.6f}".rstrip('0').rstrip('.')
    except (TypeError, ValueError):
        return str(value)


def _print_mres_summary(measurements, ensemble_id):
    """Print summary of available mres configs."""
    configs = sorted([m.get('config_number') for m in measurements])
    print(f"\nMres measurements for ensemble {ensemble_id}: {len(configs)} configs available\n")
    
    # Show config range
    if configs:
        print(f"Config range: {min(configs)} - {max(configs)}")
        
        # Show in groups of 10 per line
        print("\nAvailable configs:")
        for i in range(0, len(configs), 15):
            chunk = configs[i:i+15]
            print("  " + ", ".join(str(c) for c in chunk))
    
    print("\nTo view full correlator data for a config:")
    print(f"  mdwf_db query -e {ensemble_id} --measurements mres --cfg <CFG_NUM>")
    print(f"  mdwf_db query -e {ensemble_id} --measurements mres --cfg <CFG_NUM> -t 0 16 32  # specific timeslices")


def _print_mres_correlators(measurement, ensemble_id, timeslices=None):
    """Print full mres correlator data for a single config."""
    cfg_num = measurement.get('config_number')
    data = measurement.get('data', {})
    quarks = data.get('quarks', {})
    
    print(f"\nMres correlators for ensemble {ensemble_id}, config {cfg_num}\n")
    
    # Determine timeslices to show
    sample_quark = quarks.get('light', quarks.get('strange', quarks.get('charm', {})))
    t_extent = len(sample_quark.get('PP', []))
    
    if timeslices:
        t_indices = [t for t in timeslices if 0 <= t < t_extent]
        if not t_indices:
            print(f"ERROR: No valid timeslices in range 0-{t_extent-1}")
            return
    else:
        t_indices = list(range(t_extent))
    
    # Print quark masses
    print("Quark masses:")
    for quark in ['light', 'strange', 'charm']:
        q_data = quarks.get(quark, {})
        mass = q_data.get('mass', 'N/A')
        print(f"  {quark}: {mass}")
    print()
    
    # Build table: T, then PP and MP for each quark
    headers = ['T', 'PP_L', 'MP_L', 'PP_S', 'MP_S', 'PP_C', 'MP_C', 'MRES_L', 'MRES_S', 'MRES_C']
    rows = []
    
    for t in t_indices:
        row = {'T': t}
        for quark, label in [('light', 'L'), ('strange', 'S'), ('charm', 'C')]:
            q_data = quarks.get(quark, {})
            pp = q_data.get('PP', [])
            mp = q_data.get('MP', [])
            
            pp_val = pp[t] if t < len(pp) else None
            mp_val = mp[t] if t < len(mp) else None
            
            row[f'PP_{label}'] = _format_sci(pp_val) if pp_val is not None else ''
            row[f'MP_{label}'] = _format_sci(mp_val) if mp_val is not None else ''
            
            # Compute mres = MP/PP
            if pp_val and pp_val != 0 and mp_val is not None:
                row[f'MRES_{label}'] = _format_float(mp_val / pp_val)
            else:
                row[f'MRES_{label}'] = ''
        
        rows.append(row)
    
    _print_table(headers, rows)


def _print_meson2pt_summary(measurements, ensemble_id):
    """Print summary of available meson2pt configs."""
    configs = sorted([m.get('config_number') for m in measurements])
    
    # Collect which mesons are present
    all_mesons = set()
    for m in measurements:
        data = m.get('data', {})
        mesons = data.get('mesons', {})
        all_mesons.update(mesons.keys())
    
    meson_order = ['pion', 'kaon', 'eta_s', 'D', 'Ds', 'eta_c']
    present_mesons = [m for m in meson_order if m in all_mesons]
    
    print(f"\nMeson 2pt measurements for ensemble {ensemble_id}: {len(configs)} configs available\n")
    print(f"Mesons: {', '.join(present_mesons)}")
    
    # Show config range
    if configs:
        print(f"Config range: {min(configs)} - {max(configs)}")
        
        # Show in groups per line
        print("\nAvailable configs:")
        for i in range(0, len(configs), 15):
            chunk = configs[i:i+15]
            print("  " + ", ".join(str(c) for c in chunk))
    
    print("\nTo view full correlator data for a config:")
    print(f"  mdwf_db query -e {ensemble_id} --measurements meson2pt --cfg <CFG_NUM>")
    print(f"  mdwf_db query -e {ensemble_id} --measurements meson2pt --cfg <CFG_NUM> -t 0 16 32  # specific timeslices")


def _print_meson2pt_correlators(measurement, ensemble_id, timeslices=None):
    """Print full meson2pt correlator data for a single config."""
    cfg_num = measurement.get('config_number')
    data = measurement.get('data', {})
    mesons = data.get('mesons', {})
    
    print(f"\nMeson 2pt correlators for ensemble {ensemble_id}, config {cfg_num}\n")
    
    meson_order = ['pion', 'kaon', 'eta_s', 'D', 'Ds', 'eta_c']
    present_mesons = [m for m in meson_order if m in mesons]
    
    if not present_mesons:
        print("No meson data found")
        return
    
    # Determine timeslices to show
    sample_meson = mesons.get(present_mesons[0], {})
    t_extent = len(sample_meson.get('PP', []))
    
    if timeslices:
        t_indices = [t for t in timeslices if 0 <= t < t_extent]
        if not t_indices:
            print(f"ERROR: No valid timeslices in range 0-{t_extent-1}")
            return
    else:
        t_indices = list(range(t_extent))
    
    print(f"Mesons: {', '.join(present_mesons)}")
    print(f"T extent: {t_extent}")
    print()
    
    # Build table: T, then PP and AP for each meson
    headers = ['T']
    for meson in present_mesons:
        headers.append(f'{meson.upper()}_PP')
        headers.append(f'{meson.upper()}_AP')
    
    rows = []
    for t in t_indices:
        row = {'T': t}
        for meson in present_mesons:
            meson_data = mesons.get(meson, {})
            pp = meson_data.get('PP', [])
            ap = meson_data.get('AP', [])
            
            pp_val = pp[t] if t < len(pp) else None
            ap_val = ap[t] if t < len(ap) else None
            
            row[f'{meson.upper()}_PP'] = _format_sci(pp_val) if pp_val is not None else ''
            row[f'{meson.upper()}_AP'] = _format_sci(ap_val) if ap_val is not None else ''
        
        rows.append(row)
    
    _print_table(headers, rows)


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
