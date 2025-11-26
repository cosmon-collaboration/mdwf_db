#!/bin/bash
#SBATCH -A m0000
#SBATCH -J HMC_e1
#SBATCH -C gpu
#SBATCH -q regular
#SBATCH -t 01:00:00
#SBATCH -N 4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --mail-type=BEGIN,END
#SBATCH --signal=B:TERM@60
#SBATCH -o /global/u2/s/smithwya/mdwf_db/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/cnfg/jlog/%j.log
#SBATCH -e /global/u2/s/smithwya/mdwf_db/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/cnfg/jlog/%j.err

batch="$0"
mode="tepid"
ens="b6.0_b2.5Ls12_mc0.6_ms0.04_ml0.005_L24_T48"
ens_rel=""
VOL="24.24.24.48"
EXEC="/dummy/hmc/path"
BIND="/dummy/bind.sh"
n_trajec=100
mpi="4.4.4.8"
cacheblocking="2.2.2.2"
trajL="0.75"
lvl_sizes="9,1,1"
work_root="/global/u2/s/smithwya/mdwf_db/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48"
# cfg_max not set

cd "$work_root"

echo "========================================"
echo "HMC Job Configuration"
echo "========================================"
echo "Ensemble: $ens"
echo "Ensemble dir: /global/u2/s/smithwya/mdwf_db/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48"
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

# Record job start
EC=$(( start + n_trajec ))
mdwf_db update -e 1 -o HMC_tepid -s RUNNING \
  -p "slurm_job_id=$SLURM_JOB_ID n_trajec=$n_trajec traj_length=0.75 mode=tepid xml_path=$work_root/cnfg/HMCparameters.xml workdir=$work_root/cnfg nodes=4 config_start=$start config_end=$EC" \
  || true

SECONDS=0

cd cnfg

export I_MPI_PIN=1
export SLURM_CPU_BIND="cores"
export OMP_NUM_THREADS=4

echo "Nthreads $OMP_NUM_THREADS"

echo "START $(date)"
srun $BIND $EXEC --mpi 4.4.4.8 --grid 24.24.24.48 --dslash-unroll --cacheblocking 2.2.2.2 > log_hmc/log_b6.0_b2.5Ls12_mc0.6_ms0.04_ml0.005_L24_T48.$start
echo "STOP $(date)"

# Capture exit code immediately
EXIT_CODE=$?

# Determine status based on exit code
if [ $EXIT_CODE -eq 0 ]; then
    # Job completed successfully
    mdwf_db update -e 1 -o HMC_tepid -s COMPLETED \
      -p "slurm_job_id=$SLURM_JOB_ID exit_code=0 runtime=$SECONDS host=$(hostname)" \
      || true
elif [ $EXIT_CODE -eq 130 ] || [ $EXIT_CODE -eq 137 ] || [ $EXIT_CODE -eq 143 ]; then
    # Job was killed/canceled
    mdwf_db update -e 1 -o HMC_tepid -s CANCELED \
      -p "slurm_job_id=$SLURM_JOB_ID exit_code=$EXIT_CODE runtime=$SECONDS host=$(hostname)" \
      || true
    exit $EXIT_CODE
else
    # Job failed
    mdwf_db update -e 1 -o HMC_tepid -s FAILED \
      -p "slurm_job_id=$SLURM_JOB_ID exit_code=$EXIT_CODE runtime=$SECONDS host=$(hostname)" \
      || true
    exit $EXIT_CODE
fi

echo "All done in $SECONDS seconds"
