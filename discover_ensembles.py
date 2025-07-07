#!/usr/bin/env python3
"""
discover_ensembles.py

Standalone script to discover existing ensembles and their operation history
by analyzing the filesystem structure and files.

This script:
1. Scans ENSEMBLES/ and TUNING/ directories for ensemble structures
2. Analyzes files to detect what operations have been performed
3. Adds ensembles to the database with inferred operation history

Detection patterns:
- HMC: ckpoint_EODWF_lat.### and ckpoint_EODWF_rng.### files in cnfg/
- GLU Smearing: u_stout8n### files in cnfg_stout8/ directories
- Meson correlators: .h5 files in correlator directories
"""

import os
import re
import sys
import glob
from pathlib import Path
from collections import defaultdict
import subprocess
import argparse

def parse_ensemble_path(path):
    """
    Parse ensemble directory path to extract physics parameters.
    Expected format: .../b{beta}/b{b}Ls{Ls}/mc{mc}/ms{ms}/ml{ml}/L{L}/T{T}
    """
    path_str = str(path)
    
    # Define regex patterns for each parameter
    patterns = {
        'beta': r'b(\d+\.?\d*)',
        'b': r'b(\d+\.?\d*)Ls',
        'Ls': r'Ls(\d+)',
        'mc': r'mc(\d+\.?\d*)',
        'ms': r'ms(\d+\.?\d*)', 
        'ml': r'ml(\d+\.?\d*)',
        'L': r'L(\d+)',
        'T': r'T(\d+)'
    }
    
    params = {}
    for param, pattern in patterns.items():
        match = re.search(pattern, path_str)
        if match:
            params[param] = match.group(1)
    
    return params

def detect_hmc_operations(ensemble_dir):
    """
    Detect HMC operations by looking for checkpoint files.
    Returns list of config numbers that have been generated.
    """
    cnfg_dir = ensemble_dir / 'cnfg'
    if not cnfg_dir.exists():
        return []
    
    config_numbers = set()
    
    # Look for checkpoint files: ckpoint_EODWF_lat.### and ckpoint_EODWF_rng.###
    for pattern in ['ckpoint_EODWF_lat.*', 'ckpoint_EODWF_rng.*']:
        for file_path in cnfg_dir.glob(pattern):
            # Extract config number from filename
            match = re.search(r'\.(\d+)$', file_path.name)
            if match:
                config_numbers.add(int(match.group(1)))
    
    return sorted(config_numbers)

def detect_glu_smearing(ensemble_dir):
    """
    Detect GLU smearing operations by looking for smeared configuration files.
    Returns dict of {smear_type: [config_numbers]}
    """
    smearing_ops = defaultdict(list)
    
    # Look for directories like cnfg_stout8, cnfg_stout4, etc.
    for cnfg_smear_dir in ensemble_dir.glob('cnfg_*'):
        if not cnfg_smear_dir.is_dir():
            continue
            
        # Extract smearing type from directory name
        smear_match = re.search(r'cnfg_(\w+)', cnfg_smear_dir.name)
        if not smear_match:
            continue
            
        smear_type = smear_match.group(1).upper()
        config_numbers = set()
        
        # Look for smeared config files like u_stout8n###
        for smear_file in cnfg_smear_dir.glob('u_*'):
            # Extract config number from filename
            config_match = re.search(r'n(\d+)', smear_file.name)
            if config_match:
                config_numbers.add(int(config_match.group(1)))
        
        if config_numbers:
            smearing_ops[smear_type] = sorted(config_numbers)
    
    return dict(smearing_ops)

def detect_meson_correlators(ensemble_dir):
    """
    Detect meson correlator calculations by looking for .h5 files.
    """
    correlator_files = []
    
    # Look in common correlator directories
    for corr_dir_name in ['correlators', 'meson_2pt', 'hadron_2pt']:
        corr_dir = ensemble_dir / corr_dir_name
        if corr_dir.exists():
            h5_files = list(corr_dir.glob('*.h5'))
            correlator_files.extend(h5_files)
    
    # Also check root directory for .h5 files
    h5_files = list(ensemble_dir.glob('*.h5'))
    correlator_files.extend(h5_files)
    
    return correlator_files

def find_ensemble_directories(base_path):
    """
    Find all ensemble directories under the given base path.
    Look for directories that match the expected ensemble structure.
    """
    base_path = Path(base_path)
    if not base_path.exists():
        return []
    
    ensemble_dirs = []
    
    # Look for directories with the pattern: b*/b*Ls*/mc*/ms*/ml*/L*/T*
    pattern = "**/b*/b*Ls*/mc*/ms*/ml*/L*/T*"
    
    for potential_dir in base_path.glob(pattern):
        if potential_dir.is_dir():
            # Check if it has typical ensemble subdirectories
            has_cnfg = (potential_dir / 'cnfg').exists()
            has_log_hmc = (potential_dir / 'log_hmc').exists()
            has_slurm = (potential_dir / 'slurm').exists()
            
            if has_cnfg or has_log_hmc or has_slurm:
                ensemble_dirs.append(potential_dir)
    
    return ensemble_dirs

def add_ensemble_to_db(db_file, ensemble_dir, status, description=None):
    """
    Add ensemble to database using mdwf_db command.
    Returns (ensemble_id, success).
    """
    # Parse parameters from path
    params = parse_ensemble_path(ensemble_dir)
    
    if not params:
        print(f"Could not parse parameters from path: {ensemble_dir}")
        return None, False
    
    # Check required parameters
    required = ['beta', 'b', 'Ls', 'mc', 'ms', 'ml', 'L', 'T']
    missing = [p for p in required if p not in params]
    if missing:
        print(f"Missing required parameters {missing} for: {ensemble_dir}")
        return None, False
    
    # Build parameter string
    param_str = ' '.join([f"{k}={v}" for k, v in params.items()])
    
    # Build command
    cmd = [
        'mdwf_db', 'add-ensemble',
        '--db-file', db_file,
        '-p', param_str,
        '-s', status,
        '-d', str(ensemble_dir)
    ]
    
    if description:
        cmd.extend(['--description', description])
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        
        if result.returncode == 0:
            # Parse ensemble ID from output
            output = result.stdout.strip()
            if 'ID=' in output:
                ensemble_id = int(output.split('ID=')[1].split()[0])
                return ensemble_id, True
            else:
                print(f"⚠️  Unexpected output: {output}")
                return None, False
        else:
            print(f"Failed to add ensemble: {result.stderr.strip()}")
            return None, False
            
    except Exception as e:
        print(f"Error running mdwf_db: {e}")
        return None, False

def add_operation_to_db(db_file, ensemble_id, operation_type, status='COMPLETED', params=None):
    """
    Add operation to database using mdwf_db update command.
    """
    cmd = [
        'mdwf_db', 'update',
        '--db-file', db_file,
        '-e', str(ensemble_id),
        '--operation-type', operation_type,
        '--status', status
    ]
    
    # Add parameters if provided
    if params:
        param_str = ' '.join([f"{key}={value}" for key, value in params.items()])
        cmd.extend(['--params', param_str])
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            print(f"Error adding operation: {result.stderr.strip()}")
        return result.returncode == 0
    except Exception as e:
        print(f"Error adding operation: {e}")
        return False

def process_ensemble(db_file, ensemble_dir, status, dry_run=False):
    """
    Process a single ensemble: add to DB and detect operations.
    """
    print(f"\n📁 Processing: {ensemble_dir}")
    
    if dry_run:
        print("   [DRY RUN - no changes will be made]")
    
    # Parse parameters for description
    params = parse_ensemble_path(ensemble_dir)
    param_summary = ', '.join([f"{k}={v}" for k, v in params.items()]) if params else "unknown parameters"
    description = f"Auto-discovered ensemble ({param_summary})"
    
    # Add ensemble to database
    if not dry_run:
        ensemble_id, success = add_ensemble_to_db(db_file, ensemble_dir, status, description)
        if not success:
            return False
        print(f"Added ensemble ID={ensemble_id}")
    else:
        print(f"   Would add ensemble with status={status}")
        ensemble_id = "DRY_RUN"
    
    # Detect operations
    operations_found = []
    
    # 1. Detect HMC operations
    hmc_configs = detect_hmc_operations(ensemble_dir)
    if hmc_configs:
        config_range = f"{min(hmc_configs)}-{max(hmc_configs)}"
        operations_found.append(('HMC', {'config_start': min(hmc_configs), 'config_end': max(hmc_configs)}))
        print(f"    Found HMC: configs {config_range} ({len(hmc_configs)} configurations)")
    
    # 2. Detect GLU smearing
    smearing_ops = detect_glu_smearing(ensemble_dir)
    for smear_type, config_nums in smearing_ops.items():
        config_range = f"{min(config_nums)}-{max(config_nums)}"
        operations_found.append(('GLU_SMEARING', {
            'smear_type': smear_type,
            'config_start': min(config_nums),
            'config_end': max(config_nums)
        }))
        print(f"    Found {smear_type} smearing: configs {config_range} ({len(config_nums)} configurations)")
    
    # 3. Detect meson correlators
    correlator_files = detect_meson_correlators(ensemble_dir)
    if correlator_files:
        operations_found.append(('MESON_2PT', {'file_count': len(correlator_files)}))
        print(f"    Found meson correlators: {len(correlator_files)} files")
    
    # Add operations to database
    if not dry_run:
        for op_type, op_params in operations_found:
            success = add_operation_to_db(db_file, ensemble_id, op_type, 'COMPLETED', op_params)
            if success:
                print(f"    Added {op_type} operation")
            else:
                print(f"    Failed to add {op_type} operation")
    else:
        for op_type, op_params in operations_found:
            print(f"   Would add {op_type} operation with params: {op_params}")
    
    if not operations_found:
        print(f"    No operations detected (empty ensemble)")
    
    return True

def main():
    parser = argparse.ArgumentParser(
        description="Discover existing ensembles and their operation history",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Discover ensembles in current directory
  python discover_ensembles.py
  
  # Dry run to see what would be discovered
  python discover_ensembles.py --dry-run
  
  # Use specific database file
  python discover_ensembles.py --db-file /path/to/database.db
  
  # Process only ENSEMBLES directory
  python discover_ensembles.py --ensembles-only
        """
    )
    
    parser.add_argument('--db-file', default='mdwf_ensembles.db',
                        help='Database file path (default: mdwf_ensembles.db)')
    parser.add_argument('--base-dir', default='.',
                        help='Base directory containing ENSEMBLES/ and TUNING/ (default: current directory)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without making changes')
    parser.add_argument('--ensembles-only', action='store_true',
                        help='Only process ENSEMBLES/ directory (skip TUNING/)')
    parser.add_argument('--tuning-only', action='store_true',
                        help='Only process TUNING/ directory (skip ENSEMBLES/)')
    
    args = parser.parse_args()
    
    base_dir = Path(args.base_dir).resolve()
    
    print(f" Discovering ensembles in: {base_dir}")
    print(f" Database: {args.db_file}")
    
    if args.dry_run:
        print("🧪 DRY RUN MODE - No changes will be made")
    
    total_processed = 0
    total_success = 0
    
    # Process ENSEMBLES directory
    if not args.tuning_only:
        ensembles_dir = base_dir / 'ENSEMBLES'
        if ensembles_dir.exists():
            print(f"\n Scanning ENSEMBLES directory...")
            ensemble_dirs = find_ensemble_directories(ensembles_dir)
            print(f"   Found {len(ensemble_dirs)} ensemble directories")
            
            for ensemble_dir in ensemble_dirs:
                total_processed += 1
                if process_ensemble(args.db_file, ensemble_dir, 'PRODUCTION', args.dry_run):
                    total_success += 1
        else:
            print(f"  ENSEMBLES directory not found: {ensembles_dir}")
    
    # Process TUNING directory
    if not args.ensembles_only:
        tuning_dir = base_dir / 'TUNING'
        if tuning_dir.exists():
            print(f"\n🔧 Scanning TUNING directory...")
            ensemble_dirs = find_ensemble_directories(tuning_dir)
            print(f"   Found {len(ensemble_dirs)} ensemble directories")
            
            for ensemble_dir in ensemble_dirs:
                total_processed += 1
                if process_ensemble(args.db_file, ensemble_dir, 'TUNING', args.dry_run):
                    total_success += 1
        else:
            print(f"  TUNING directory not found: {tuning_dir}")
    
    # Summary
    print(f"\n Summary:")
    print(f"   Processed: {total_processed} ensembles")
    print(f"   Success: {total_success} ensembles")
    if total_processed > total_success:
        print(f"   Failed: {total_processed - total_success} ensembles")
    
    if args.dry_run:
        print(f"\n Run without --dry-run to actually add ensembles to the database")
    
    return 0 if total_success == total_processed else 1

if __name__ == '__main__':
    sys.exit(main()) 