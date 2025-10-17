#!/bin/bash
#SBATCH -A acct
#SBATCH -C cpu
#SBATCH -q regular
#SBATCH -t 17:00:00
#SBATCH --cpus-per-task=32
#SBATCH -N 1
#SBATCH --ntasks-per-node=32
#SBATCH --mail-type=BEGIN,END

#SBATCH --signal=B:TERM@60
#SBATCH --output=/Users/wyatt/Development/mdwf_db/__tmp_hmc_test/TUNING/b6.0/b1.8Ls24/mc0.8/ms0.07/ml0.02/L32/T64/cnfg/jlog/%j.out
#SBATCH --error=/Users/wyatt/Development/mdwf_db/__tmp_hmc_test/TUNING/b6.0/b1.8Ls24/mc0.8/ms0.07/ml0.02/L32/T64/cnfg/jlog/%j.err

batch="$0"
DB="/Users/wyatt/Development/mdwf_db/__tmp_hmc_test/mdwf_ensembles.db"
EID=1
mode="continue"
ens="b6.0_b1.8Ls24_mc0.8_ms0.07_ml0.02_L32_T64"
ens_rel="TUNING/b6.0/b1.8Ls24/mc0.8/ms0.07/ml0.02/L32/T64"
VOL="32.32.32.64"
EXEC="/bin/true"
BIND="/bin/echo"
n_trajec=10
mpi="4.4.4.8"
cacheblocking="2.2.2.2"
trajL="0.75"
lvl_sizes="9,1,1"
work_root="/Users/wyatt/Development/mdwf_db/__tmp_hmc_test/TUNING/b6.0/b1.8Ls24/mc0.8/ms0.07/ml0.02/L32/T64"
cfg_max=100

cd "$work_root"
LOGFILE="/global/cfs/cdirs/m2986/cosmon/mdwf/mdwf_update.log"

echo "========================================"
echo "HMC Job Configuration"
echo "========================================"
echo "Ensemble: $ens"
echo "Ensemble dir: /Users/wyatt/Development/mdwf_db/__tmp_hmc_test/TUNING/b6.0/b1.8Ls24/mc0.8/ms0.07/ml0.02/L32/T64"
echo "Work root: $work_root"
echo "Mode: $mode"
echo "Trajectories per job: $n_trajec"
echo "Target config (cfg_max): $cfg_max"
echo "Executable: $EXEC"
echo "Binding: $BIND"
echo "MPI grid: $mpi"
echo "========================================"

mkdir -p cnfg/log_hmc cnfg/jlog
module load conda
conda activate /global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf

# Source HMC helper functions
source <(python -m MDWFutils.jobs.hmc_helpers)

# Determine starting configuration
start=$(hmc_find_latest_config "cnfg")
[[ -z "$start" || "$start" -eq 0 ]] && start=0

# Ensure StartTrajectory in XML matches detected start
XML_PATH="$work_root/cnfg/HMCparameters.xml"
if [[ -f "$XML_PATH" ]]; then
    sed -i -E "s#(<StartTrajectory>)[0-9]+(</StartTrajectory>)#\1${start}\2#" "$XML_PATH"
else
    echo "WARNING: HMCparameters.xml not found at $XML_PATH" >&2
fi

# Self-resubmission logic (submit to queue early for better priority)
echo "Self-Resubmission Check:"
echo "------------------------"
# Source HMC resubmission helper via process substitution
if [[ -n "$cfg_max" ]]; then
    source <(python -m MDWFutils.jobs.hmc_resubmit)
    hmc_auto_resubmit
else
    echo "cfg_max not set - no automatic resubmission"
fi
echo "------------------------"
echo ""

# Prepare DB update variables and queue RUNNING update off-node
USER=$(whoami)
OP="HMC_$mode"
SC=$start
EC=$(( start + n_trajec ))
# Build params string with HMC-specific info
PARAMS="config_start=$start config_end=$EC n_trajectories=$n_trajec cfg_max=$cfg_max mode=$mode"
RUN_DIR="$work_root"
# Source logging helper via process substitution
source <(python -m MDWFutils.jobs.slurm_update_trap)

SECONDS=0

cd cnfg

export I_MPI_PIN=1
export SLURM_CPU_BIND="cores"
export OMP_NUM_THREADS=4

echo "Nthreads $OMP_NUM_THREADS"

echo "START `date`"
srun $BIND $EXEC --mpi $mpi --grid $VOL --dslash-unroll --cacheblocking $cacheblocking > log_hmc/log_b6.0_b1.8Ls24_mc0.8_ms0.07_ml0.02_L32_T64.$start
echo "STOP `date`"

echo "All done in $SECONDS seconds"
