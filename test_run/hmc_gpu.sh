#!/bin/bash
#SBATCH -A m2986_g
#SBATCH -J HMC_e1
#SBATCH -C gpu
#SBATCH -q regular
#SBATCH -t 06:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --gpus-per-task=1
#SBATCH --gpu-bind=none
#SBATCH --gres=gpu:1
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
trajL="0.75"
lvl_sizes="9,1,1"
work_root="/global/u2/s/smithwya/mdwf_db/test_run/ENSEMBLES/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48"
# cfg_max not set

cd "$work_root"
LOGFILE="/global/cfs/cdirs/m2986/cosmon/mdwf/mdwf_update.log"

echo "ens = $ens"
echo "ens_dir = /global/u2/s/smithwya/mdwf_db/test_run/ENSEMBLES/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48"
echo "work_root = $work_root"
echo "EXEC = $EXEC"
echo "BIND = $BIND"
echo "n_trajec = $n_trajec"

mkdir -p cnfg/log_hmc cnfg/jlog
module load conda
conda activate /global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf

# Source HMC helper functions for robust config detection
source <(python -m MDWFutils.jobs.hmc_helpers)

# Determine starting configuration quietly
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

export CRAY_ACCEL_TARGET=nvidia80
export MPICH_OFI_NIC_POLICY=GPU
export SLURM_CPU_BIND="cores"
export MPICH_GPU_SUPPORT_ENABLED=1
export MPICH_RDMA_ENABLED_CUDA=1
export MPICH_GPU_IPC_ENABLED=1
export MPICH_GPU_EAGER_REGISTER_HOST_MEM=0
export MPICH_GPU_NO_ASYNC_MEMCPY=0
export OMP_NUM_THREADS=16

echo "Nthreads $OMP_NUM_THREADS"

echo "START $(date)"
srun $BIND $EXEC --mpi 4.4.4.8 --grid 24.24.24.48 --accelerator-threads 32 --dslash-unroll --shm 2048 --comms-overlap -shm-mpi 0 > log_hmc/log_b6.0_b2.5Ls12_mc0.6_ms0.04_ml0.005_L24_T48.$start
echo "STOP $(date)"

echo "Job completed in $SECONDS seconds"
echo "Log file: cnfg/log_hmc/log_b6.0_b2.5Ls12_mc0.6_ms0.04_ml0.005_L24_T48.$start"