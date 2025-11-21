#!/usr/bin/env python3
"""
MDWFutils/config.py

Configuration file handling for ensemble operation parameters.
Supports storing and retrieving "recipes" for HMC, smearing, and measurement operations.
"""

import yaml
import json
from pathlib import Path
from typing import Dict, Any, Optional, Union
import sys

DEFAULT_CONFIG_FILENAME = 'mdwf_default_params.yaml'

def get_config_path(ensemble_dir: Union[str, Path]) -> Path:
    """Get the path to the configuration file for an ensemble."""
    return Path(ensemble_dir) / DEFAULT_CONFIG_FILENAME

def load_ensemble_config(ensemble_dir: Union[str, Path]) -> Dict[str, Any]:
    """
    Load configuration from ensemble directory.
    Returns empty dict if no config file exists.
    """
    config_path = get_config_path(ensemble_dir)
    
    if not config_path.exists():
        return {}
    
    try:
        with open(config_path, 'r') as f:
            if config_path.suffix.lower() == '.json':
                return json.load(f)
            else:
                return yaml.safe_load(f) or {}
    except Exception as e:
        print(f"Warning: Failed to load config from {config_path}: {e}", file=sys.stderr)
        return {}

def save_ensemble_config(ensemble_dir: Union[str, Path], config: Dict[str, Any], 
                        format_type: str = 'yaml') -> bool:
    """
    Save configuration to ensemble directory.
    format_type: 'yaml' or 'json'
    """
    ensemble_path = Path(ensemble_dir)
    ensemble_path.mkdir(parents=True, exist_ok=True)
    
    if format_type.lower() == 'json':
        config_path = ensemble_path / 'mdwf_config.json'
        try:
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=2)
        except Exception as e:
            print(f"Error saving JSON config to {config_path}: {e}", file=sys.stderr)
            return False
    else:
        config_path = ensemble_path / DEFAULT_CONFIG_FILENAME
        try:
            with open(config_path, 'w') as f:
                yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        except Exception as e:
            print(f"Error saving YAML config to {config_path}: {e}", file=sys.stderr)
            return False
    
    return True

def get_operation_config(ensemble_dir: Union[str, Path], operation_type: str, 
                        mode: Optional[str] = None) -> Dict[str, Any]:
    """
    Get configuration for a specific operation type and mode.
    
    Args:
        ensemble_dir: Path to ensemble directory
        operation_type: Type of operation ('hmc', 'smearing', 'meson_2pt', etc.)
        mode: Optional mode within operation type ('tepid', 'continue', 'reseed' for HMC)
    
    Returns:
        Dict with operation parameters, empty if not found
    """
    config = load_ensemble_config(ensemble_dir)
    
    if operation_type not in config:
        return {}
    
    op_config = config[operation_type]
    
    if mode and isinstance(op_config, dict) and mode in op_config:
        return op_config[mode]
    elif not mode and isinstance(op_config, dict):
        # Return the whole operation config if no specific mode requested
        return op_config
    else:
        return {}

def merge_params(config_params: str, cli_params: str) -> str:
    """
    Merge configuration parameters with CLI parameters.
    CLI parameters take precedence over config parameters.
    
    Args:
        config_params: Space-separated key=val pairs from config
        cli_params: Space-separated key=val pairs from CLI
    
    Returns:
        Merged parameter string
    """
    def parse_params(param_str: str) -> Dict[str, str]:
        """Parse space-separated key=val pairs into dict."""
        if not param_str:
            return {}
        
        result = {}
        for param in param_str.strip().split():
            if '=' in param:
                key, val = param.split('=', 1)
                result[key] = val
        return result
    
    config_dict = parse_params(config_params)
    cli_dict = parse_params(cli_params)
    
    # CLI params override config params
    merged = {**config_dict, **cli_dict}
    
    # Convert back to space-separated string
    return ' '.join([f"{k}={v}" for k, v in merged.items()])

def create_default_config() -> Dict[str, Any]:
    """
    Create a default configuration template with common parameter sets.
    """
    return {
        'hmc': {
            'tepid': {
                'xml_params': 'StartTrajectory=0 Trajectories=100 MDsteps=2 trajL=0.75 MetropolisTest=false',
                'job_params': 'cfg_max=100 time_limit=12:00:00 nodes=1 constraint=gpu cpus_per_task=32'
            },
            'continue': {
                'xml_params': 'Trajectories=50 MDsteps=2 trajL=0.75 MetropolisTest=true',
                'job_params': 'cfg_max=500 time_limit=6:00:00 nodes=1 constraint=gpu cpus_per_task=32'
            },
            'reseed': {
                'xml_params': 'StartTrajectory=0 Trajectories=200 MDsteps=2 trajL=0.75 MetropolisTest=true',
                'job_params': 'cfg_max=200 time_limit=12:00:00 nodes=1 constraint=gpu cpus_per_task=32'
            }
        },
        'smearing': {
            'stout8': {
                'params': 'nsteps=8 rho=0.1',
                'job_params': 'time_limit=2:00:00 nodes=1'
            },
            'stout4': {
                'params': 'nsteps=4 rho=0.15',
                'job_params': 'time_limit=1:30:00 nodes=1'
            }
        },
        'meson_2pt': {
            'default': {
                'params': 'source_type=point sink_type=point',
                'job_params': 'time_limit=4:00:00 nodes=1'
            },
            'wall': {
                'params': 'source_type=wall sink_type=point',
                'job_params': 'time_limit=6:00:00 nodes=2'
            }
        },
        'wit': {
            'default': {
                'params': 'mass_preset=physical',
                'job_params': 'time_limit=8:00:00 nodes=2'
            }
        }
    }

def print_config_summary(ensemble_dir: Union[str, Path]) -> None:
    """Print a summary of the configuration file for an ensemble."""
    config = load_ensemble_config(ensemble_dir)
    config_path = get_config_path(ensemble_dir)
    
    if not config:
        print(f"No configuration file found at {config_path}")
        return
    
    print(f"Configuration file: {config_path}")
    print("Available operation configurations:")
    
    for op_type, op_config in config.items():
        print(f"\n  {op_type}:")
        if isinstance(op_config, dict):
            for mode, params in op_config.items():
                print(f"    {mode}:")
                if isinstance(params, dict):
                    for param_type, param_val in params.items():
                        print(f"      {param_type}: {param_val}")
                else:
                    print(f"      {params}")
        else:
            print(f"    {op_config}")

def save_operation_config(ensemble_dir: Union[str, Path], operation_type: str, 
                         variant: str, xml_params: str = None, job_params: str = None, 
                         params: str = None) -> bool:
    """
    Save or update operation configuration parameters.
    
    Args:
        ensemble_dir: Path to ensemble directory
        operation_type: Type of operation ('hmc', 'smearing', 'meson_2pt', etc.)
        variant: Variant/mode within operation type ('tepid', 'continue', 'stout8', etc.)
        xml_params: XML parameters string (for HMC)
        job_params: Job parameters string
        params: General parameters string (for non-HMC operations)
    
    Returns:
        True if successful, False otherwise
    """
    # Load existing config or create new one
    config = load_ensemble_config(ensemble_dir)
    
    # Ensure operation type exists
    if operation_type not in config:
        config[operation_type] = {}
    
    # Ensure variant exists
    if variant not in config[operation_type]:
        config[operation_type][variant] = {}
    
    # Update parameters
    variant_config = config[operation_type][variant]
    
    if xml_params is not None:
        variant_config['xml_params'] = xml_params
    if job_params is not None:
        variant_config['job_params'] = job_params
    if params is not None:
        variant_config['params'] = params
    
    # Save updated config
    return save_ensemble_config(ensemble_dir, config)

def validate_config(config: Dict[str, Any]) -> bool:
    """
    Validate configuration structure.
    Returns True if valid, False otherwise.
    """
    if not isinstance(config, dict):
        print("Error: Configuration must be a dictionary", file=sys.stderr)
        return False
    
    # Check known operation types have proper structure
    known_ops = ['hmc', 'smearing', 'meson_2pt', 'wit']
    
    for op_type, op_config in config.items():
        if op_type in known_ops:
            if not isinstance(op_config, dict):
                print(f"Error: {op_type} configuration must be a dictionary", file=sys.stderr)
                return False
            
            # For HMC, check that modes have proper structure
            if op_type == 'hmc':
                hmc_modes = ['tepid', 'continue', 'reseed']
                for mode, mode_config in op_config.items():
                    if mode in hmc_modes:
                        if not isinstance(mode_config, dict):
                            print(f"Error: HMC {mode} configuration must be a dictionary", file=sys.stderr)
                            return False
                        # Check for expected parameter types
                        expected_params = ['xml_params', 'job_params']
                        for param_type in expected_params:
                            if param_type in mode_config and not isinstance(mode_config[param_type], str):
                                print(f"Error: HMC {mode}.{param_type} must be a string", file=sys.stderr)
                                return False
    
    return True