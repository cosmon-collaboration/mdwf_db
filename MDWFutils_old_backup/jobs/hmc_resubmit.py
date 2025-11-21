#!/usr/bin/env python3
"""
HMC Self-Resubmission Logic

This module provides bash functions for HMC job self-resubmission that can be
sourced via process substitution in SLURM scripts.

Usage in SLURM scripts:
    source <(python -m MDWFutils.jobs.hmc_resubmit)
    hmc_auto_resubmit  # Call this function at the end of HMC job

Required environment variables:
    cfg_max      - Maximum configuration number for resubmission
    n_trajec     - Number of trajectories to run per job
    batch        - Path to the current batch script
    SLURM_JOBID  - Current SLURM job ID (auto-set by SLURM)
"""

HMC_RESUBMIT_BASH = r"""
# HMC Self-Resubmission Functions

# Main function to handle HMC automatic resubmission (call before HMC run)
hmc_auto_resubmit() {
    # Only proceed if cfg_max is set
    if [[ -z "$cfg_max" ]]; then
        echo "cfg_max not set - no automatic resubmission"
        return 0
    fi
    
    echo "# HMC Auto-Resubmission Logic (pre-run queue submission)"
    echo "cfg_max = $cfg_max"
    echo "n_trajec = $n_trajec"
    echo "current start = $start"
    
    # Calculate what the next start will be after this run
    local next_start=$((start + n_trajec))
    echo "Expected next start: $next_start"
    
    # Check if we should resubmit based on expected next start
    if [[ $next_start -lt $cfg_max ]]; then
        echo "Submitting next job to queue (will start after this job completes)"
        echo "Command: sbatch --dependency=afterok:$SLURM_JOBID $batch"
        sbatch --dependency=afterok:$SLURM_JOBID "$batch"
        echo "Next job submitted to queue with dependency on current job"
    else
        echo "Next start ($next_start) will reach/exceed target ($cfg_max)"
        echo "No resubmission needed"
    fi
}

# Helper function to validate required variables
hmc_validate_resubmit_env() {
    local missing=()
    [[ -z "$cfg_max" ]] && missing+=("cfg_max")
    [[ -z "$n_trajec" ]] && missing+=("n_trajec")
    [[ -z "$batch" ]] && missing+=("batch")
    [[ -z "$SLURM_JOBID" ]] && missing+=("SLURM_JOBID")
    
    if [[ ${#missing[@]} -gt 0 ]]; then
        echo "Warning: Missing required variables for HMC resubmission: ${missing[*]}"
        return 1
    fi
    return 0
}

# Auto-call hmc_auto_resubmit if cfg_max is set (backward compatibility)
if [[ -n "$cfg_max" ]]; then
    # Only auto-call if we're at the end of an HMC script
    # This allows scripts to call hmc_auto_resubmit manually if desired
    :  # No-op placeholder - scripts should call hmc_auto_resubmit explicitly
fi
"""


def get_hmc_resubmit_inline() -> str:
    """Return the HMC resubmission bash functions as a string."""
    return HMC_RESUBMIT_BASH


if __name__ == "__main__":
    # When called as a module, output the bash functions
    print(HMC_RESUBMIT_BASH)


__all__ = [
    "get_hmc_resubmit_inline",
]
