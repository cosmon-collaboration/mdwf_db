#!/bin/bash
#SBATCH -A acct
#SBATCH -C gpu
#SBATCH -q regular
#SBATCH -t 17:00:00
#SBATCH --cpus-per-task=32
#SBATCH -N 1
#SBATCH --ntasks-per-node=32
#SBATCH --gpus-per-task=1
#SBATCH --gpu-bind=none
#SBATCH --gres=gpu:1
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
mpi="2.1.1.2"
trajL="0.75"
lvl_sizes="9,1,1"
work_root="/Users/wyatt/Development/mdwf_db/__tmp_hmc_test/TUNING/b6.0/b1.8Ls24/mc0.8/ms0.07/ml0.02/L32/T64"
cfg_max=100

cd "$work_root"
LOGFILE="/global/cfs/cdirs/m2986/cosmon/mdwf/mdwf_update.log"

echo "ens = $ens"
echo "ens_dir = /Users/wyatt/Development/mdwf_db/__tmp_hmc_test/TUNING/b6.0/b1.8Ls24/mc0.8/ms0.07/ml0.02/L32/T64"
echo "work_root = $work_root"
echo "EXEC = $EXEC"
echo "BIND = $BIND"
echo "n_trajec = $n_trajec"
echo "cfg_max = $cfg_max"

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

echo "START `date`"
srun $BIND $EXEC --mpi $mpi --grid $VOL --accelerator-threads 32 --dslash-unroll --shm 2048 --comms-overlap -shm-mpi 0 > log_hmc/log_b6.0_b1.8Ls24_mc0.8_ms0.07_ml0.02_L32_T64.$start
echo "STOP `date`"

echo "Job completed in $SECONDS seconds"
echo "Log file: cnfg/log_hmc/log_b6.0_b1.8Ls24_mc0.8_ms0.07_ml0.02_L32_T64.$start"
