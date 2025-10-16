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

    # Helper: query current Slurm state (prefer .batch step via sacct, fallback to job and scontrol)
    mdwf_query_state_once() {
      local JID="$1"
      local S=""
      S=$(sacct -X -j ${JID}.batch -n -o State --parsable2 2>/dev/null | head -1)
      if [[ -z "$S" ]]; then
        S=$(sacct -X -j ${JID} -n -o State --parsable2 2>/dev/null | head -1)
      fi
      if [[ -z "$S" ]]; then
        S=$(scontrol show job ${JID} 2>/dev/null | grep -o 'JobState=[^ ]*' | cut -d= -f2)
      fi
      echo "$S"
    }

    # Wait for Slurm to record a terminal state (accounting can lag)
    mdwf_get_final_state() {
      local JID="$1"
      local MAX_TRIES=${MDWF_DB_SLURM_POLL_TRIES:-20}
      local S=""
      for ((i=0;i<MAX_TRIES;i++)); do
        S="$(mdwf_query_state_once "$JID")"
        local U="$(echo "$S" | tr '[:lower:]' '[:upper:]')"
        if [[ "$U" == *"COMPLETED"* || "$U" == *"FAILED"* || "$U" == *"TIMEOUT"* || "$U" == *"CANCEL"* || "$U" == *"PREEMPT"* || "$U" == *"NODE_FAIL"* || "$U" == *"OUT_OF_MEMORY"* ]]; then
          echo "$S"
          return 0
        fi
        sleep 1
      done
      echo "$S"
      return 0
    }

    if [[ -n "$SLURM_JOB_ID" ]]; then
      SLURM_STATUS="$(mdwf_get_final_state "$SLURM_JOB_ID")"
      [[ -z "$SLURM_STATUS" ]] && SLURM_STATUS="UNKNOWN"
    else
      SLURM_STATUS="NO_JOBID"
    fi

    # Normalize status: map Slurm states to our limited set
    local SLURM_STATE_UPPER="$(echo "$SLURM_STATUS" | tr '[:lower:]' '[:upper:]')"
    if [[ "$SLURM_STATE_UPPER" == *"TIMEOUT"* ]]; then
      ST="TIMEOUT"
      REASON="job_timeout"
    elif [[ "$SLURM_STATE_UPPER" == *"CANCEL"* || "$SLURM_STATE_UPPER" == *"PREEMPT"* ]]; then
      ST="CANCELED"
      REASON="job_cancelled"
    elif [[ "$SLURM_STATE_UPPER" == *"FAILED"* || "$SLURM_STATE_UPPER" == *"NODE_FAIL"* || "$SLURM_STATE_UPPER" == *"OUT_OF_MEMORY"* ]]; then
      ST="FAILED"
      REASON="job_failed"
    elif [[ $EXIT_CODE -eq 143 || $EXIT_CODE -eq 130 || $EXIT_CODE -eq 129 ]]; then
      ST="CANCELED"
      REASON="job_killed"
    elif [[ $EXIT_CODE -ne 0 ]]; then
      ST="FAILED"
      REASON="job_failed"
    else
      ST="COMPLETED"
      REASON="job_completed"
    fi

    # If Slurm status string is non-terminal or empty, log the normalized status instead
    local LOG_SLURM_STATUS="$SLURM_STATUS"
    local LOG_U="$(echo "$LOG_SLURM_STATUS" | tr '[:lower:]' '[:upper:]')"
    if [[ -z "$LOG_SLURM_STATUS" || "$LOG_U" == "RUNNING" || "$LOG_U" == "PENDING" || "$LOG_U" == "UNKNOWN" ]]; then
      LOG_SLURM_STATUS="$ST"
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
      PARAMS_STR="$PARAMS_STR run_dir=$RUN_DIR"Right now the slurm_status parameter isn't getting changed properly on job finishing. For example,

Op 39: GLU_WFLOW [COMPLETED]
  Created: 2025-10-07T09:00:30.006691 (by walkloud)
  Updated: 2025-10-07T09:00:30.254623
    config_end = 596
    config_increment = 4
    config_start = 576
    exit_code = 0
    host = nid004300
    reason = job_completed
    run_dir = /global/cfs/cdirs/m2986/cosmon/mdwf/ENSEMBLES/b4.008/b1.75Ls10/mc0.8555/ms0.0725/ml0.0195/L24/T48
    runtime = 144
    slurm_job = 43680978
    slurm_status = RUNNING
Op 40: WIT_MESON2PT [COMPLETED]
  Created: 2025-10-07T09:00:30.498519 (by walkloud)
  Updated: 2025-10-07T10:00:20.973805
    config_end = 596
    config_increment = 4
    config_start = 576
    exit_code = 0
    host = nid002328
    reason = job_completed
    run_dir = /global/cfs/cdirs/m2986/cosmon/mdwf/ENSEMBLES/b4.008/b1.75Ls10/mc0.8555/ms0.0725/ml0.0195/L24/T48
    runtime = 1418
    slurm_job = 43682452
    slurm_status = RUNNING

Is appearing. So the [COMPLETED] updated but the slurm_status didn't. This is common for all the slurm scripts, and may be related to the exit trap
    fi

    echo "mdwf_db update --db-file=$DB --ensemble-id=$EID --operation-type=$OP --status=$ST --user=$USER --params=\"exit_code=$EXIT_CODE runtime=$SECONDS slurm_job=$SLURM_JOB_ID host=$(hostname) reason=$REASON slurm_status=$LOG_SLURM_STATUS $PARAMS_STR\"" >> "$LOGFILE"
    echo \"$OP job $ST ($EXIT_CODE) - $REASON (SLURM: $LOG_SLURM_STATUS)\"
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


