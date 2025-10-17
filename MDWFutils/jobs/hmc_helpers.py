#!/usr/bin/env python3
"""
HMC Helper Functions

This module provides bash helper functions for HMC jobs that can be
sourced via process substitution in SLURM scripts.

Usage in SLURM scripts:
    source <(python -m MDWFutils.jobs.hmc_helpers)
    start=$(hmc_find_latest_config)  # Find latest valid config
"""

HMC_HELPERS_BASH = r"""
# HMC Helper Functions for configuration detection and validation

# Find the latest configuration that has both checkpoint and RNG files
# Returns: configuration number (or 0 if none found)
hmc_find_latest_config() {
    local cnfg_dir="${1:-.}"
    local config_prefix="${2:-ckpoint_EODWF_lat.}"
    local rng_prefix="${3:-ckpoint_EODWF_rng.}"
    
    # Find all checkpoint files and extract numbers
    local configs=()
    for f in "$cnfg_dir"/${config_prefix}*; do
        if [[ -f "$f" && -r "$f" ]]; then
            # Extract the configuration number
            local num=$(echo "$f" | sed "s|.*${config_prefix}||" | sed 's/[^0-9]*//g')
            if [[ -n "$num" ]]; then
                configs+=("$num")
            fi
        fi
    done
    
    # Sort configs in descending order (highest first)
    if [[ ${#configs[@]} -eq 0 ]]; then
        echo "0"
        return 0
    fi
    
    # Use printf to sort numerically
    local sorted_configs=($(printf '%s\n' "${configs[@]}" | sort -rn))
    
    # Check each config (highest first) for both checkpoint and RNG file
    for cfg in "${sorted_configs[@]}"; do
        local ckpt_file="$cnfg_dir/${config_prefix}${cfg}"
        local rng_file="$cnfg_dir/${rng_prefix}${cfg}"
        
        # Check if both files exist and are readable
        if [[ -f "$ckpt_file" && -r "$ckpt_file" ]]; then
            if [[ -f "$rng_file" && -r "$rng_file" ]]; then
                echo "Found valid config $cfg with checkpoint and RNG files" >&2
                echo "$cfg"
                return 0
            else
                echo "Warning: Config $cfg has checkpoint but missing/unreadable RNG file: $rng_file" >&2
            fi
        fi
    done
    
    # No valid config found with both files
    echo "No valid configurations found with both checkpoint and RNG files" >&2
    echo "0"
    return 0
}

# Validate that a specific configuration has all required files
# Returns: 0 if valid, 1 if invalid
hmc_validate_config() {
    local cfg=$1
    local cnfg_dir="${2:-.}"
    local config_prefix="${3:-ckpoint_EODWF_lat.}"
    local rng_prefix="${4:-ckpoint_EODWF_rng.}"
    
    if [[ -z "$cfg" ]]; then
        echo "Error: No configuration number provided" >&2
        return 1
    fi
    
    local ckpt_file="$cnfg_dir/${config_prefix}${cfg}"
    local rng_file="$cnfg_dir/${rng_prefix}${cfg}"
    
    local errors=0
    
    # Check checkpoint file
    if [[ ! -f "$ckpt_file" ]]; then
        echo "Error: Checkpoint file not found: $ckpt_file" >&2
        errors=$((errors + 1))
    elif [[ ! -r "$ckpt_file" ]]; then
        echo "Error: Checkpoint file not readable: $ckpt_file" >&2
        errors=$((errors + 1))
    fi
    
    # Check RNG file
    if [[ ! -f "$rng_file" ]]; then
        echo "Error: RNG file not found: $rng_file" >&2
        errors=$((errors + 1))
    elif [[ ! -r "$rng_file" ]]; then
        echo "Error: RNG file not readable: $rng_file" >&2
        errors=$((errors + 1))
    fi
    
    if [[ $errors -eq 0 ]]; then
        echo "Config $cfg validated: checkpoint and RNG files present and readable" >&2
        return 0
    else
        echo "Config $cfg validation failed with $errors error(s)" >&2
        return 1
    fi
}

# List all configurations with their file status
hmc_list_configs() {
    local cnfg_dir="${1:-.}"
    local config_prefix="${2:-ckpoint_EODWF_lat.}"
    local rng_prefix="${3:-ckpoint_EODWF_rng.}"
    
    echo "Configuration Status Report:" >&2
    echo "Directory: $cnfg_dir" >&2
    echo "---" >&2
    
    # Find all checkpoint files
    local found_any=0
    for f in "$cnfg_dir"/${config_prefix}*; do
        if [[ -f "$f" ]]; then
            found_any=1
            local num=$(echo "$f" | sed "s|.*${config_prefix}||" | sed 's/[^0-9]*//g')
            if [[ -n "$num" ]]; then
                local ckpt_status="✓"
                local rng_file="$cnfg_dir/${rng_prefix}${num}"
                local rng_status="✗"
                
                [[ ! -r "$f" ]] && ckpt_status="✗(unreadable)"
                [[ -f "$rng_file" && -r "$rng_file" ]] && rng_status="✓"
                [[ -f "$rng_file" && ! -r "$rng_file" ]] && rng_status="✗(unreadable)"
                
                echo "Config $num: checkpoint=$ckpt_status rng=$rng_status" >&2
            fi
        fi
    done
    
    if [[ $found_any -eq 0 ]]; then
        echo "No configurations found" >&2
    fi
}
"""


def get_hmc_helpers_inline() -> str:
    """Return the HMC helper bash functions as a string."""
    return HMC_HELPERS_BASH


if __name__ == "__main__":
    # When called as a module, output the bash functions
    print(HMC_HELPERS_BASH)


__all__ = [
    "get_hmc_helpers_inline",
]

