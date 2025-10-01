from pathlib import Path
import os


SLURM_UPDATE_TRAP_BASH = """# mdwf_db slurm update helpers
# Sourced or inlined into generated SLURM scripts.

# Ensure the updater log file is present and writable by all users
mdwf_ensure_logfile() {
  if [[ -z "$LOGFILE" ]]; then
    return
  fi
  local dir="$(dirname "$LOGFILE")"
  mkdir -p "$dir"
  # Create file if missing and relax permissions only on creation
  local created=0
  if [[ ! -e "$LOGFILE" ]]; then
    : > "$LOGFILE"
    created=1
  fi
  if [[ $created -eq 1 ]]; then
    chmod a+rw "$LOGFILE" 2>/dev/null || true
  fi
}

# Queue a RUNNING update line to the mdwf_db updater log
mdwf_queue_running_update() {
  mdwf_ensure_logfile
  local PARAMS_STR=""
  if [[ -n "$SC" && -n "$EC" ]]; then
    PARAMS_STR="config_start=$SC config_end=$EC"
    if [[ -n "$IC" ]]; then
      PARAMS_STR+=" config_increment=$IC"
    fi
  elif [[ -n "$PARAMS" ]]; then
    PARAMS_STR="$PARAMS"
  fi
  if [[ -n "$RUN_DIR" ]]; then
    PARAMS_STR="$PARAMS_STR run_dir=$RUN_DIR"
  fi
  echo "mdwf_db update --db-file=$DB --ensemble-id=$EID --operation-type=$OP --status=RUNNING --user=$USER --params=\"$PARAMS_STR slurm_job=$SLURM_JOB_ID\"" >> "$LOGFILE"
}

# Install a trap to update status on exit with details
mdwf_setup_update_trap() {
  update_status() {
    # Guard to ensure this handler runs only once
    if [[ -n "$__MDWF_UPDATE_RAN" ]]; then
      return
    fi
    __MDWF_UPDATE_RAN=1
    # Clear the EXIT trap immediately to avoid any re-entry
    trap - EXIT
    local EXIT_CODE=$?
    local ST="COMPLETED"
    local REASON=""
    local SLURM_STATUS=""
    mdwf_ensure_logfile
    
    if [[ -n "$SLURM_JOB_ID" ]]; then
      SLURM_STATUS=$(sacct -j $SLURM_JOB_ID -n -o State --parsable2 | head -1)
      if [[ -z "$SLURM_STATUS" ]]; then
        SLURM_STATUS=$(scontrol show job $SLURM_JOB_ID | grep -o 'JobState=[^ ]*' | cut -d= -f2)
      fi
      [[ -z "$SLURM_STATUS" ]] && SLURM_STATUS="UNKNOWN"
    else
      SLURM_STATUS="NO_JOBID"
    fi
    
    # Normalize status: allow RUNNING, COMPLETED, FAILED, TIMEOUT, CANCELED
    local SLURM_STATE_UPPER="$(echo "$SLURM_STATUS" | tr '[:lower:]' '[:upper:]')"
    if [[ "$SLURM_STATE_UPPER" == *"TIMEOUT"* ]]; then
      ST="TIMEOUT"
      REASON="job_timeout"
    elif [[ "$SLURM_STATE_UPPER" == *"CANCEL"* ]]; then
      ST="CANCELED"
      REASON="job_cancelled"
    elif [[ $EXIT_CODE -eq 143 || $EXIT_CODE -eq 130 || $EXIT_CODE -eq 129 ]]; then
      ST="CANCELED"
      REASON="job_killed"
    elif [[ $EXIT_CODE -ne 0 ]]; then
      ST="FAILED"
      REASON="job_failed"
    else
      REASON="job_completed"
    fi

    local PARAMS_STR=""
    if [[ -n "$SC" && -n "$EC" ]]; then
      PARAMS_STR="config_start=$SC config_end=$EC"
      if [[ -n "$IC" ]]; then
        PARAMS_STR+=" config_increment=$IC"
      fi
    elif [[ -n "$PARAMS" ]]; then
      PARAMS_STR="$PARAMS"
    fi
    if [[ -n "$RUN_DIR" ]]; then
      PARAMS_STR="$PARAMS_STR run_dir=$RUN_DIR"
    fi

    echo "mdwf_db update --db-file=$DB --ensemble-id=$EID --operation-type=$OP --status=$ST --user=$USER --params=\"exit_code=$EXIT_CODE runtime=$SECONDS slurm_job=$SLURM_JOB_ID host=$(hostname) reason=$REASON slurm_status=$SLURM_STATUS $PARAMS_STR\"" >> "$LOGFILE"
    echo \"$OP job $ST ($EXIT_CODE) - $REASON (SLURM: $SLURM_STATUS)\"
  }
  # Trap only EXIT to avoid duplicate invocations on both signal and exit
  trap update_status EXIT
}

# Log a post-run ingest/move of files from a scratch dir to shared
# Usage: mdwf_log_ingest SRC_DIR DEST_DIR [OP_TYPE]
# OP_TYPE defaults to FILE_INGEST
mdwf_log_ingest() {
  local SRC="$1"
  local DEST="$2"
  local OPTYPE="${3:-FILE_INGEST}"
  local FILES="$(find "$SRC" -type f 2>/dev/null | wc -l | tr -d ' ')"
  local BYTES="$(du -sb "$SRC" 2>/dev/null | awk '{print $1}')"
  mdwf_ensure_logfile
  echo "mdwf_db update --db-file=$DB --ensemble-id=$EID --operation-type=$OPTYPE --status=COMPLETED --user=$USER --params=\"action=ingest source=$SRC dest=$DEST file_count=$FILES bytes=$BYTES\"" >> "$LOGFILE"
}

# Auto-run when sourced if required variables are present
if [[ -n "$DB" && -n "$EID" && -n "$OP" && ( ( -n "$SC" && -n "$EC" ) || -n "$PARAMS" ) ]]; then
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


