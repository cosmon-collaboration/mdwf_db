#!/usr/bin/env python3
"""Scan cnfg directories to update configuration ranges in the database."""

import argparse
import re
from pathlib import Path

from ...scanners.gauge_obs import GaugeObsScanner
from ...scanners.meson2pt import Meson2ptScanner
from ...scanners.mres import MresScanner
from ..ensemble_utils import add_ensemble_argument, get_backend_for_args


def register(subparsers):
    p = subparsers.add_parser(
        'scan',
        help='Update configuration ranges based on cnfg/ files',
        description="""
Scan ensemble directories and update the database.

WHAT THIS DOES:
• Scans cnfg/ directory to update configuration counts (first/last/increment/total)
• Discovers available measurement files and reports ingestion status
• Reports missing measurements (files exist but not in database)

EXAMPLES:
  mdwf_db scan                    # Scan all ensembles
  mdwf_db scan -e 5               # Scan only ensemble 5
  mdwf_db scan --force            # Re-update config counts even if unchanged
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    add_ensemble_argument(p, help_text='Optional: scan only this ensemble (ID, path, or nickname)', required=False)
    p.add_argument('--force', action='store_true', help='Update config counts even if unchanged')
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
        nick = ens.get('nickname')
        
        # Print ensemble header
        if nick:
            print(f"\nEnsemble {ens_id} ({nick})")
        else:
            print(f"\nEnsemble {ens_id}")
        print("-" * 50)
        
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
                    and cfg.get('config_list') == values
                ):
                    should_update = False
            
            # Print config info
            inc_str = f", inc={increment}" if increment else ""
            print(f"  Configurations: {total} total ({first}-{last}{inc_str})")
            
            if should_update:
                backend.update_ensemble(
                    ens_id,
                    configurations={
                        'first': first,
                        'last': last,
                        'increment': increment,
                        'total': total,
                        'config_list': values,
                    },
                )
                updated += 1
                print(f"    [updated in database]")
        else:
            print("  Configurations: none found")
        
        # Discover measurement files and report status
        ensemble_path = Path(ens['directory'])
        measurements_found = False
        
        # Collect measurement stats
        measurement_stats = []
        
        # Gauge observables
        gauge_scanner = GaugeObsScanner()
        gauge_files = gauge_scanner.scan(ensemble_path)
        if gauge_files:
            existing_gauge = set(backend.get_measured_configs(ens_id, 'gauge_obs'))
            found_configs = {r.config_number for r in gauge_files}
            ingested = len(found_configs & existing_gauge)
            pending = len(found_configs - existing_gauge)
            pending_list = sorted(found_configs - existing_gauge) if pending > 0 else []
            measurement_stats.append(('gauge_obs', ingested, pending, pending_list))
            measurements_found = True
        
        # Mres (unitary)
        mres_scanner = MresScanner()
        mres_files = mres_scanner.scan(ensemble_path)
        if mres_files:
            existing_mres = set(backend.get_measured_configs(ens_id, 'mres'))
            found_configs = {r.config_number for r in mres_files}
            ingested = len(found_configs & existing_mres)
            pending = len(found_configs - existing_mres)
            pending_list = sorted(found_configs - existing_mres) if pending > 0 else []
            measurement_stats.append(('mres', ingested, pending, pending_list))
            measurements_found = True
        
        # Meson 2pt (unitary)
        meson2pt_scanner = Meson2ptScanner()
        meson2pt_files = meson2pt_scanner.scan(ensemble_path)
        if meson2pt_files:
            existing_meson2pt = set(backend.get_measured_configs(ens_id, 'meson2pt'))
            found_configs = {r.config_number for r in meson2pt_files}
            ingested = len(found_configs & existing_meson2pt)
            pending = len(found_configs - existing_meson2pt)
            pending_list = sorted(found_configs - existing_meson2pt) if pending > 0 else []
            measurement_stats.append(('meson2pt', ingested, pending, pending_list))
            measurements_found = True
        
        # Print measurements section
        if measurements_found:
            print("  Measurements:")
            # Calculate max width for alignment
            max_name_len = max(len(name) for name, _, _, _ in measurement_stats)
            for name, ingested, pending, pending_list in measurement_stats:
                name_padded = name.ljust(max_name_len)
                if pending > 0:
                    if len(pending_list) <= 5:
                        print(f"    {name_padded}  {ingested:4d} ingested, {pending:4d} pending  {pending_list}")
                    else:
                        print(f"    {name_padded}  {ingested:4d} ingested, {pending:4d} pending")
                else:
                    print(f"    {name_padded}  {ingested:4d} ingested, {pending:4d} pending")
        else:
            print("  Measurements: no data files found")
        
        # Report missing (DB-only comparison)
        config_list = values if values else ens.get('configurations', {}).get('config_list', [])
        if config_list:
            _report_missing(backend, ens_id, ens, config_list, gauge_files, mres_files, meson2pt_files)

    print(f"\n{'=' * 50}")
    print(f"Scan complete: {updated} ensemble(s) updated")
    return 0


def _extract_cfg_numbers(cnfg_dir: Path):
    """Extract config numbers from ckpoint_EODWF_lat.{number} files."""
    if not cnfg_dir.exists():
        return []
    pattern = re.compile(r'^ckpoint_EODWF_lat\.(\d+)$')
    numbers = []
    for child in cnfg_dir.iterdir():
        if not child.is_file():
            continue
        m = pattern.match(child.name)
        if m:
            numbers.append(int(m.group(1)))
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


def _report_missing(backend, ensemble_id: int, ensemble: dict, config_list: list, gauge_files: list, mres_files: list, meson2pt_files: list):
    """Report missing measurements (DB-only comparison).
    
    Only reports "missing" for measurement types that have SOME files present,
    to avoid confusing output when a measurement type doesn't apply to the ensemble.
    """
    if not config_list:
        return
    
    try:
        expected = set(config_list)
        warnings = []
        
        # Gauge obs missing (only if we found some gauge files)
        if gauge_files:
            measured_gauge = set(backend.get_measured_configs(ensemble_id, 'gauge_obs'))
            found_gauge = {r.config_number for r in gauge_files}
            missing_gauge = sorted(expected - measured_gauge - found_gauge)
            
            if missing_gauge:
                if len(missing_gauge) <= 8:
                    warnings.append(f"gauge_obs: {len(missing_gauge)} configs have no files: {missing_gauge}")
                else:
                    warnings.append(f"gauge_obs: {len(missing_gauge)} configs have no files")
        
        # Mres missing (only if we found some mres files)
        if mres_files:
            measured_mres = set(backend.get_measured_configs(ensemble_id, 'mres'))
            found_mres = {r.config_number for r in mres_files}
            missing_mres = sorted(expected - measured_mres - found_mres)
            
            if missing_mres:
                if len(missing_mres) <= 8:
                    warnings.append(f"mres: {len(missing_mres)} configs have no files: {missing_mres}")
                else:
                    warnings.append(f"mres: {len(missing_mres)} configs have no files")
        
        # Meson2pt missing (only if we found some meson2pt files)
        if meson2pt_files:
            measured_meson2pt = set(backend.get_measured_configs(ensemble_id, 'meson2pt'))
            found_meson2pt = {r.config_number for r in meson2pt_files}
            missing_meson2pt = sorted(expected - measured_meson2pt - found_meson2pt)
            
            if missing_meson2pt:
                if len(missing_meson2pt) <= 8:
                    warnings.append(f"meson2pt: {len(missing_meson2pt)} configs have no files: {missing_meson2pt}")
                else:
                    warnings.append(f"meson2pt: {len(missing_meson2pt)} configs have no files")
        
        # Print warnings section if any
        if warnings:
            print("  Warnings:")
            for warning in warnings:
                print(f"    {warning}")
    except Exception:
        # Silently skip if reporting fails
        pass
