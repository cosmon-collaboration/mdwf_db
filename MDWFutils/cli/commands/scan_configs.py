#!/usr/bin/env python3
"""Scan cnfg directories to update configuration ranges in the database."""

import argparse
import re
from pathlib import Path

from ..ensemble_utils import add_ensemble_argument, get_backend_for_args


def register(subparsers):
    p = subparsers.add_parser(
        'scan',
        help='Update configuration ranges based on cnfg/ files',
        description="""
Scan ensemble directories and update the database.

WHAT THIS DOES:
• Scans cnfg/ directory to update configuration counts (first/last/increment/total)
• Scans t0/ directory to parse and store gauge observables (plaq, Q, t0, w0)
• Reports missing gauge observable measurements

GAUGE OBSERVABLES STORED:
• plaq - Plaquette
• Q - Topological charge  
• sqrt_t0_clov, sqrt_t0_plaq - t0 scales (clover and plaquette)
• w0_clov, w0_plaq - w0 scales (clover and plaquette)

EXAMPLES:
  mdwf_db scan                    # Scan all ensembles
  mdwf_db scan -e 5               # Scan only ensemble 5
  mdwf_db scan --force            # Re-update config counts even if unchanged
  mdwf_db scan --overwrite        # Re-parse gauge observables already in DB
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    add_ensemble_argument(p, help_text='Optional: scan only this ensemble (ID, path, or nickname)', required=False)
    p.add_argument('--force', action='store_true', help='Update config counts even if unchanged')
    p.add_argument('--overwrite', action='store_true', help='Re-parse gauge observables even if already in DB')
    p.set_defaults(func=do_scan)


def do_scan(args):
    backend = get_backend_for_args(args)
    
    # Get ensembles - filter if --ensemble specified
    if args.ensemble:
        try:
            ens_id, ens = backend.resolve_ensemble_identifier(args.ensemble)
            ensembles = [ens]
        except Exception as e:
            print(f"ERROR: {e}")
            return 1
    else:
        ensembles = backend.list_ensembles(detailed=True)
    
    if not ensembles:
        print('No ensembles found')
        return 0

    updated = 0
    for ens in ensembles:
        ens_id = ens.get('ensemble_id') or ens.get('id')
        
        # Existing cnfg/ scanning
        cnfg_dir = Path(ens['directory']) / 'cnfg'
        values = _extract_cfg_numbers(cnfg_dir)
        if values:
            first, last = values[0], values[-1]
            increment = _infer_increment(values)
            total = len(values)
            cfg = ens.get('configurations') or {}
            should_update = True
            if not args.force and cfg:
                if (
                    cfg.get('first') == first
                    and cfg.get('last') == last
                    and cfg.get('increment') == increment
                    and cfg.get('total') == total
                ):
                    should_update = False
            
            if should_update:
                backend.update_ensemble(
                    ens_id,
                    configurations={
                        'first': first,
                        'last': last,
                        'increment': increment,
                        'total': total,
                    },
                )
                updated += 1
                print(f"Updated ensemble {ens_id}: first={first} last={last} inc={increment} total={total}")
        
        # Gauge observable scanning
        t0_dir = Path(ens['directory']) / 't0'  # Always 't0'
        if t0_dir.exists():
            # Efficient: get all existing measurements in one query
            existing = set(backend.get_measured_configs(ens_id, 'gauge_obs'))
            parsed_count = 0
            
            for t0_file in sorted(t0_dir.glob('t0.*.out')):
                try:
                    # Extract config number from filename (t0.{cfg}.out)
                    cfg_num = int(t0_file.stem.split('.')[-1])
                    if cfg_num in existing and not args.overwrite:
                        continue
                    data = _parse_gauge_obs(t0_file)
                    backend.upsert_measurement(ens_id, cfg_num, 'gauge_obs', data)
                    parsed_count += 1
                except Exception:
                    # Skip files that can't be parsed
                    continue
            
            if parsed_count > 0:
                print(f"  Parsed {parsed_count} gauge observable file(s) for ensemble {ens_id}")
        
        # Report missing (DB-only comparison)
        _report_missing(backend, ens_id, ens)

    print(f"Scan complete: {updated} ensemble(s) updated")
    return 0


def _extract_cfg_numbers(cnfg_dir: Path):
    if not cnfg_dir.exists():
        return []
    numbers = []
    for child in cnfg_dir.iterdir():
        if not child.is_file():
            continue
        m = list(re.finditer(r"(\d+)", child.name))
        if not m:
            continue
        try:
            numbers.append(int(m[-1].group(1)))
        except ValueError:
            pass
    return sorted(set(numbers))


def _infer_increment(values):
    if len(values) < 2:
        return None
    inc = values[1] - values[0]
    if inc <= 0:
        return None
    for idx in range(2, len(values)):
        if values[idx] - values[idx - 1] != inc:
            return None
    return inc


def _parse_gauge_obs(filepath: Path) -> dict:
    """Parse gauge observables from a t0.{cfg}.out file.
    
    Returns dict with keys: plaq, Q, sqrt_t0_clov, sqrt_t0_plaq, w0_clov, w0_plaq
    Missing values stored as float('nan').
    """
    data = {
        'plaq': float('nan'),
        'Q': float('nan'),
        'sqrt_t0_clov': float('nan'),
        'sqrt_t0_plaq': float('nan'),
        'w0_clov': float('nan'),
        'w0_plaq': float('nan'),
    }
    
    try:
        content = filepath.read_text()
        lines = content.split('\n')
        
        # Parse plaquette from "Calculated Trace" line
        for line in lines:
            if 'Calculated Trace' in line:
                parts = line.split()
                if parts:
                    try:
                        data['plaq'] = float(parts[-1])
                    except (ValueError, IndexError):
                        pass
                break
        
        # Parse Q from last WFLOW line (5th-to-last word)
        wflow_lines = [line for line in lines if 'WFLOW' in line]
        if wflow_lines:
            last_wflow = wflow_lines[-1]
            parts = last_wflow.split()
            if len(parts) >= 5:
                try:
                    data['Q'] = float(parts[-5])
                except (ValueError, IndexError):
                    pass
        
        # Parse t0 and w0 scales (look for lines with "0.3")
        for line in lines:
            if 'GT-scale Clover' in line and '0.3' in line:
                parts = line.split()
                if parts:
                    try:
                        data['sqrt_t0_clov'] = float(parts[-1])
                    except (ValueError, IndexError):
                        pass
            elif 'GT-scale Plaq' in line and '0.3' in line:
                parts = line.split()
                if parts:
                    try:
                        data['sqrt_t0_plaq'] = float(parts[-1])
                    except (ValueError, IndexError):
                        pass
            elif 'WT-scale Clover' in line and '0.3' in line:
                parts = line.split()
                if parts:
                    try:
                        data['w0_clov'] = float(parts[-1])
                    except (ValueError, IndexError):
                        pass
            elif 'WT-scale Plaq' in line and '0.3' in line:
                parts = line.split()
                if parts:
                    try:
                        data['w0_plaq'] = float(parts[-1])
                    except (ValueError, IndexError):
                        pass
    
    except Exception:
        # Return data with NaN values if parsing fails
        pass
    
    return data


def _report_missing(backend, ensemble_id: int, ensemble: dict):
    """Report missing gauge observable measurements (DB-only comparison)."""
    cfg = ensemble.get('configurations', {})
    if not cfg.get('first'):
        return
    
    try:
        nick = ensemble.get('nickname') or ensemble_id
        measured = set(backend.get_measured_configs(ensemble_id, 'gauge_obs'))
        
        if not cfg.get('increment'):
            # Non-uniform config spacing - can't compute expected set
            # Just report what we have
            total = cfg.get('total', '?')
            if measured:
                print(f"  {nick}: {len(measured)}/{total} configs have gauge_obs")
            return
        
        expected = set(range(cfg['first'], cfg['last'] + 1, cfg['increment']))
        missing = sorted(expected - measured)
        
        if missing:
            if len(missing) <= 10:
                print(f"  {nick}: {len(missing)} configs missing gauge_obs: {missing}")
            else:
                print(f"  {nick}: {len(missing)} configs missing gauge_obs")
    except Exception:
        # Silently skip if reporting fails
        pass
