#!/bin/bash
#SBATCH -A m2986
#SBATCH -J HMC_e1
#SBATCH -C gpu
#SBATCH -q regular
#SBATCH -t 04:00:00
#SBATCH -N 2
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --mail-type=BEGIN,END
#SBATCH --signal=B:TERM@60
#SBATCH -o /global/u2/s/smithwya/mdwf_db/test_run/ENSEMBLES/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/cnfg/jlog/%j.log
#SBATCH -e /global/u2/s/smithwya/mdwf_db/test_run/ENSEMBLES/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/cnfg/jlog/%j.err

batch="$0"
DB="mongodb://mdwf_ensembles_admin:a%20place%20for%20everything%20and%20everything%20in%20its%20place@mongodb05.nersc.gov:27017/mdwf_ensembles?authSource=mdwf_ensembles"
EID=1
mode="tepid"
ens="b6.0_b2.5Ls12_mc0.6_ms0.04_ml0.005_L24_T48"
ens_rel=""
VOL="24.24.24.48"
EXEC="/bin/true"
BIND="/bin/true"
n_trajec=50
mpi="4.4.4.8"
cacheblocking="2.2.2.2"
trajL="0.75"
lvl_sizes="9,1,1"
work_root="/global/u2/s/smithwya/mdwf_db/test_run/ENSEMBLES/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48"
# cfg_max not set

cd "$work_root"
LOGFILE="/global/cfs/cdirs/m2986/cosmon/mdwf/mdwf_update.log"

echo "========================================"
echo "HMC Job Configuration"
echo "========================================"
echo "Ensemble: $ens"
echo "Ensemble dir: /global/u2/s/smithwya/mdwf_db/test_run/ENSEMBLES/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48"
echo "Work root: $work_root"
echo "Mode: $mode"
echo "Trajectories per job: $n_trajec"
echo "No cfg_max set (single job)"
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
    sed -i -E "s#(<StartTrajectory>)[0-9]+(</StartTrajectory>)#\\1${start}\\2#" "$XML_PATH"
else
    echo "WARNING: HMCparameters.xml not found at $XML_PATH" >&2
fi

# Self-resubmission logic (submit to queue early for better priority)
echo "No automatic resubmission (cfg_max not set)"
echo ""

# Prepare DB update variables and queue RUNNING update off-node
USER=$(whoami)
OP="HMC_tepid"
SC=$start
EC=$(( start + n_trajec ))
PARAMS="config_start=$start config_end=$EC n_trajectories=$n_trajec mode=tepid"
RUN_DIR="$work_root"
source <(python -m MDWFutils.jobs.slurm_update_trap)

SECONDS=0

cd cnfg

export I_MPI_PIN=1
export SLURM_CPU_BIND="cores"
export OMP_NUM_THREADS=4

echo "Nthreads $OMP_NUM_THREADS"

echo "START $(date)"
srun $BIND $EXEC --mpi 4.4.4.8 --grid 24.24.24.48 --dslash-unroll --cacheblocking 2.2.2.2 > log_hmc/log_b6.0_b2.5Ls12_mc0.6_ms0.04_ml0.005_L24_T48.$start
echo "STOP $(date)"

echo "All done in $SECONDS seconds"
CONFIG_START=
CONFIG_END=
CONFIG_STEP=1

for (( cfg=CONFIG_START; cfg<=CONFIG_END; cfg+=CONFIG_STEP )); do
    python -m MDWFutils.jobs.hmc run \
        --mode cpu \
        --db "" \
        --ensemble 1 \
        --config ${cfg} \
        --work-dir "/global/u2/s/smithwya/mdwf_db/test_run/ENSEMBLES/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48" \
        --trajectory-length 0.75 \
        --trajectories 1
done
