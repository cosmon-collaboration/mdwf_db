#!/bin/bash
# Comprehensive CLI test script for MDWF database
# Usage: ./test_all_cli.sh 2>&1 | tee test_output.log

set -e  # Exit on error
set -u  # Exit on undefined variable

# Color codes for terminal output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print section headers
print_section() {
    echo ""
    echo "============================================================================"
    echo "  $1"
    echo "============================================================================"
    echo ""
}

# Function to run a command and report status
run_cmd() {
    echo -e "${BLUE}[CMD]${NC} $*"
    if "$@"; then
        echo -e "${GREEN}[OK]${NC} Command succeeded"
        echo ""
        return 0
    else
        local exit_code=$?
        echo -e "${RED}[FAIL]${NC} Command failed with exit code $exit_code"
        echo ""
        return $exit_code
    fi
}

# Start of test script
echo "================================================================================"
echo "  MDWF CLI Comprehensive Test Suite"
echo "  Started: $(date)"
echo "================================================================================"
echo ""

# -----------------------------------------------------------------------------
print_section "1. INITIALIZATION & BASIC SETUP"
# -----------------------------------------------------------------------------

run_cmd mdwf_db init-db --base-dir test_run

run_cmd mdwf_db add-ensemble -s TUNING -p 'beta=6.0 b=2.5 Ls=12 mc=0.6 ms=0.04 ml=0.005 L=24 T=48'

run_cmd mdwf_db query

run_cmd mdwf_db query --sort-by-id

run_cmd mdwf_db query -e 1

run_cmd mdwf_db query -e 1 --detailed

run_cmd mdwf_db query -e 1 --dir

# -----------------------------------------------------------------------------
print_section "2. NICKNAME MANAGEMENT"
# -----------------------------------------------------------------------------

run_cmd mdwf_db nickname -e 1

run_cmd mdwf_db nickname -e 1 --set test_ensemble

run_cmd mdwf_db nickname -e 1

run_cmd mdwf_db nickname -e 1 --clear

run_cmd mdwf_db nickname -e 1

# -----------------------------------------------------------------------------
print_section "3. STANDALONE INPUT FILE GENERATION"
# -----------------------------------------------------------------------------

run_cmd mdwf_db glu-input -e 1 -i 'SMEARTYPE=STOUT SMITERS=8'

run_cmd mdwf_db wit-input -e 1 -i 'Configurations.first=0 Configurations.last=12'

run_cmd mdwf_db hmc-xml -e 1 -i 'Trajectories=100 trajL=0.75'

# -----------------------------------------------------------------------------
print_section "4. DEFAULT PARAMETERS MANAGEMENT"
# -----------------------------------------------------------------------------

run_cmd mdwf_db default_params list -e 1

run_cmd mdwf_db default_params set -e 1 --job-type smear --variant stout8 \
  --input 'SMEARTYPE=STOUT SMITERS=8' \
  --job 'config_start=0 config_end=12 nodes=1 ranks=1'

run_cmd mdwf_db default_params list -e 1

run_cmd mdwf_db default_params show -e 1 --job-type smear --variant stout8

run_cmd mdwf_db default_params list -e 1 --job-type smear

run_cmd mdwf_db default_params set -e 1 --job-type smear --variant ape10 \
  --input 'SMEARTYPE=APE SMITERS=10' \
  --job 'config_start=0 config_end=12 nodes=2 ranks=1'

run_cmd mdwf_db default_params list -e 1

# -----------------------------------------------------------------------------
print_section "5. JOB SCRIPT GENERATION (Smear & Wilson Flow)"
# -----------------------------------------------------------------------------

run_cmd mdwf_db smear-script -e 1 --use-default-params --params-variant stout8

run_cmd mdwf_db smear-script -e 1 \
  -i 'SMEARTYPE=HYP SMITERS=5' \
  -j 'config_start=0 config_end=12 config_inc=4 nodes=1 ranks=1' \
  --save-default-params --params-variant hyp5

run_cmd mdwf_db wflow-script -e 1 \
  -i 'SMEARTYPE=ADAPTWFLOW_STOUT SMITERS=250' \
  -j 'config_start=0 config_end=12 config_inc=4 nodes=1 ranks=1'

# -----------------------------------------------------------------------------
print_section "6. JOB SCRIPT GENERATION (HMC)"
# -----------------------------------------------------------------------------

run_cmd mdwf_db hmc-script gpu -e 1 \
  -i 'Trajectories=100 trajL=0.75' \
  -j 'n_trajec=100 trajL=0.75 lvl_sizes=9,1,1 exec_path=/dummy/hmc/path bind_script=/dummy/bind.sh nodes=1'

run_cmd mdwf_db hmc-script cpu -e 1 \
  -i 'Trajectories=100 trajL=0.75' \
  -j 'n_trajec=100 trajL=0.75 lvl_sizes=9,1,1 exec_path=/dummy/hmc/path bind_script=/dummy/bind.sh nodes=4'

# -----------------------------------------------------------------------------
print_section "7. JOB SCRIPT GENERATION (WIT-based measurements)"
# -----------------------------------------------------------------------------

run_cmd mdwf_db mres-script -e 1 \
  -i 'Configurations.first=0 Configurations.last=12' \
  -j 'nodes=1 ranks=4'

run_cmd mdwf_db mres-mq-script -e 1 \
  -i 'Configurations.first=0 Configurations.last=12' \
  -j 'nodes=1 ranks=4'

run_cmd mdwf_db meson2pt-script -e 1 \
  -i 'Configurations.first=0 Configurations.last=12' \
  -j 'nodes=1 ranks=4'

run_cmd mdwf_db zv-script -e 1 \
  -i 'Configurations.first=0 Configurations.last=12' \
  -j 'nodes=1 ranks=4'

# -----------------------------------------------------------------------------
print_section "8. OPERATION TRACKING"
# -----------------------------------------------------------------------------

run_cmd mdwf_db update -e 1 -o GLU_SMEAR -s COMPLETED \
  -p 'slurm_job_id=12345 exit_code=0 runtime=3600 host=nid001234'

run_cmd mdwf_db update -e 1 -o WIT_MRES -s FAILED \
  -p 'slurm_job_id=12346 exit_code=1 runtime=1800 host=nid001235'

run_cmd mdwf_db update -e 1 -o HMC_GPU -s RUNNING \
  -p 'slurm_job_id=12347 host=nid001236'

run_cmd mdwf_db query -e 1 --detailed

run_cmd mdwf_db update -e 1 -o HMC_GPU -s COMPLETED -i 1 \
  -p 'exit_code=0 runtime=7200'

run_cmd mdwf_db query -e 1 --detailed

# -----------------------------------------------------------------------------
print_section "9. CONFIGURATION SCANNING"
# -----------------------------------------------------------------------------

run_cmd mdwf_db scan

run_cmd mdwf_db scan --force

run_cmd mdwf_db query -e 1 --detailed

# -----------------------------------------------------------------------------
print_section "10. CLEAR HISTORY (Preserves ensemble, clears operations)"
# -----------------------------------------------------------------------------

run_cmd mdwf_db clear-history -e 1 --force

run_cmd mdwf_db query -e 1 --detailed

# -----------------------------------------------------------------------------
print_section "11. PROMOTE ENSEMBLE (TUNING → PRODUCTION)"
# -----------------------------------------------------------------------------

run_cmd mdwf_db promote-ensemble -e 1 --base-dir test_run --force

run_cmd mdwf_db query -e 1

# -----------------------------------------------------------------------------
print_section "12. CLEANUP DEFAULT PARAMS"
# -----------------------------------------------------------------------------

run_cmd mdwf_db default_params delete -e 1 --job-type smear --variant stout8 --force

run_cmd mdwf_db default_params delete -e 1 --job-type smear --variant ape10 --force

run_cmd mdwf_db default_params delete -e 1 --job-type smear --variant hyp5 --force

run_cmd mdwf_db default_params list -e 1

# -----------------------------------------------------------------------------
print_section "13. FINAL VERIFICATION"
# -----------------------------------------------------------------------------

run_cmd mdwf_db query -e 1 --detailed

run_cmd mdwf_db query --sort-by-id

# -----------------------------------------------------------------------------
echo ""
echo "================================================================================"
echo "  Test Suite Completed Successfully!"
echo "  Finished: $(date)"
echo "================================================================================"

