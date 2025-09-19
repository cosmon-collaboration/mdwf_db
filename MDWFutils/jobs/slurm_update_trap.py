from pathlib import Path
import os


SLURM_UPDATE_TRAP_BASH = """# mdwf_db slurm update helpers
# Sourced or inlined into generated SLURM scripts.

# Queue a RUNNING update line to the mdwf_db updater log
mdwf_queue_running_update() {
  local PARAMS_STR=""
  if [[ -n "$SC" && -n "$EC" && -n "$IC" ]]; then
    PARAMS_STR="config_start=$SC config_end=$EC config_increment=$IC"
  elif [[ -n "$PARAMS" ]]; then
    PARAMS_STR="$PARAMS"
  fi
  echo "mdwf_db update --db-file=$DB --ensemble-id=$EID --operation-type=$OP --status=RUNNING --user=$USER --params=\"$PARAMS_STR slurm_job=$SLURM_JOB_ID\"" >> "$LOGFILE"
}

# Install a trap to update status on exit with details
mdwf_setup_update_trap() {
  update_status() {
    local EC=$?
    local ST="COMPLETED"
    local REASON=""
    local SLURM_STATUS=""
    
    if [[ -n "$SLURM_JOB_ID" ]]; then
      SLURM_STATUS=$(sacct -j $SLURM_JOB_ID -n -o State --parsable2 | head -1)
      if [[ -z "$SLURM_STATUS" ]]; then
        SLURM_STATUS=$(scontrol show job $SLURM_JOB_ID | grep -o 'JobState=[^ ]*' | cut -d= -f2)
      fi
      [[ -z "$SLURM_STATUS" ]] && SLURM_STATUS="UNKNOWN"
    else
      SLURM_STATUS="NO_JOBID"
    fi
    
    if [[ $EC -eq 143 || $EC -eq 130 || $EC -eq 129 ]]; then
      ST="CANCELED"
      REASON="job_killed"
    elif [[ $EC -ne 0 ]]; then
      ST="FAILED"
      REASON="job_failed"
    else
      REASON="job_completed"
    fi

    local PARAMS_STR=""
    if [[ -n "$SC" && -n "$EC" && -n "$IC" ]]; then
      PARAMS_STR="config_start=$SC config_end=$EC config_increment=$IC"
    elif [[ -n "$PARAMS" ]]; then
      PARAMS_STR="$PARAMS"
    fi

    echo "mdwf_db update --db-file=$DB --ensemble-id=$EID --operation-type=$OP --status=$ST --user=$USER --params=\"exit_code=$EC runtime=$SECONDS slurm_job=$SLURM_JOB_ID host=$(hostname) reason=$REASON slurm_status=$SLURM_STATUS $PARAMS_STR\"" >> "$LOGFILE"
    echo \"$OP job $ST ($EC) - $REASON (SLURM: $SLURM_STATUS)\"
  }
  trap update_status EXIT TERM INT HUP QUIT
}

# Auto-run when sourced if required variables are present
if [[ -n "$DB" && -n "$EID" && -n "$OP" && ( ( -n "$SC" && -n "$EC" && -n "$IC" ) || -n "$PARAMS" ) ]]; then
  mdwf_queue_running_update
  mdwf_setup_update_trap
fi
"""


def get_slurm_update_trap_inline() -> str:
    return SLURM_UPDATE_TRAP_BASH


__all__ = [
    "get_slurm_update_trap_inline",
]


def emit():
    """Print the bash helper to stdout for `python -m` usage."""
    print(SLURM_UPDATE_TRAP_BASH)


if __name__ == "__main__":
    emit()


