#!/bin/bash

# Test script to demonstrate enhanced error handling
# This simulates the behavior of the enhanced update_status() function

# Simulate the enhanced update_status function
update_status() {
  local EC=$?
  local ST="COMPLETED"
  local REASON=""
  
  # Check if we were interrupted by a signal (user cancel/SLURM kill)
  if [[ $EC -eq 143 ]] || [[ $EC -eq 130 ]] || [[ $EC -eq 129 ]]; then
    ST="CANCELED"
    REASON="job_killed"
  elif [[ $EC -ne 0 ]]; then
    ST="FAILED"
    REASON="job_failed"
  else
    REASON="job_completed"
  fi
  
  echo "Job $ST ($EC) - $REASON"
  echo "Database update: status=$ST, exit_code=$EC, reason=$REASON"
}

# Set up trap to catch signals
trap update_status EXIT TERM INT HUP QUIT

echo "Starting test job..."
echo "Press Ctrl+C to test CANCELED status, or wait for completion"

# Simulate some work
sleep 5

# Simulate successful completion
echo "Job completed successfully"
exit 0
