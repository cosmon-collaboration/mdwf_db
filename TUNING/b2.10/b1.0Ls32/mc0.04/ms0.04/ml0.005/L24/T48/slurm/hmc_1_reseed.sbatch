#!/bin/bash
#SBATCH -A physics123
#SBATCH -C gpu
#SBATCH -q regular
#SBATCH -t 17:00:00
#SBATCH --cpus-per-task=32
#SBATCH -N 1
#SBATCH --ntasks-per-node=32
#SBATCH --gres=gpu:1
#SBATCH --gpu-bind=none
#SBATCH --mail-type=BEGIN,END
#SBATCH --mail-user=wyatt
#SBATCH --signal=B:TERM@60

batch="$0"
DB="./mdwf_ensembles.db"
EID=1
mode="reseed"
ens="b2.10_b1.0Ls32_mc0.04_ms0.04_ml0.005_L24_T48"
ens_rel="TUNING/b2.10/b1.0Ls32/mc0.04/ms0.04/ml0.005/L24/T48"
VOL="24.24.24.48"
EXEC="/usr/bin/hmc_exec"
BIND="/usr/bin/core_bind.sh"
n_trajec=50
cfg_max=50
mpi="2.1.1.2"

cd /Users/wyatt/Development/mdwf_db/TUNING/b2.10/b1.0Ls32/mc0.04/ms0.04/ml0.005/L24/T48

echo "ens = $ens"
echo "ens_dir = /Users/wyatt/Development/mdwf_db/TUNING/b2.10/b1.0Ls32/mc0.04/ms0.04/ml0.005/L24/T48"
echo "EXEC = $EXEC"
echo "BIND = $BIND"
echo "n_trajec = $n_trajec"
echo "cfg_max = $cfg_max"

mkdir -p cnfg
mkdir -p log_hmc

start=`ls -v cnfg/| grep lat | tail -1 | sed 's/[^0-9]*//g'`
if [[ -z $start ]]; then
    echo "no configs - start is empty - doing TepidStart"
    start=0
fi

# check if start <= cfg_max
if [[ $start -ge $cfg_max ]]; then
    echo "your latest config is greater than the target:"
    echo "  $start >= $cfg_max"
    exit
fi

echo "cfg_current = $start"

# Update database to show running job
out=$(
  mdwf_db update \
    --db-file="$DB" \
    --ensemble-id=$EID \
    --operation-type="$mode" \
    --status=RUNNING \
    --params="config_start=$start config_end=$(( start + n_trajec )) config_increment=$n_trajec slurm_job=$SLURM_JOB_ID exec_path=$EXEC bind_script=$BIND"
)
echo "$out"
op_id=${out#*operation }
op_id=${op_id%%:*}
export op_id

# Generate HMC parameters XML
mdwf_db hmc-xml -e $EID -m $mode --params "StartTrajectory=$start Trajectories=$n_trajec"

cp HMCparameters.xml cnfg/
cd cnfg

export CRAY_ACCEL_TARGET=nvidia80
export MPICH_OFI_NIC_POLICY=GPU
export SLURM_CPU_BIND="cores"
export MPICH_GPU_SUPPORT_ENABLED=1
export MPICH_RDMA_ENABLED_CUDA=1
export MPICH_GPU_IPC_ENABLED=1
export MPICH_GPU_EAGER_REGISTER_HOST_MEM=0
export MPICH_GPU_NO_ASYNC_MEMCPY=0
export OMP_NUM_THREADS=8

echo "Nthreads $OMP_NUM_THREADS"

echo "START `date`"
srun $BIND $EXEC --mpi $mpi --grid $VOL --accelerator-threads 32 --dslash-unroll --shm 2048 --comms-overlap -shm-mpi 0 > ../log_hmc/log_b2.10_b1.0Ls32_mc0.04_ms0.04_ml0.005_L24_T48.$start
EXIT_CODE=$?
echo "STOP `date`"

# Update database with job status
STATUS=COMPLETED
[[ $EXIT_CODE -ne 0 ]] && STATUS=FAILED

mdwf_db update \
  --db-file="$DB" \
  --ensemble-id=$EID \
  --operation-id=$op_id \
  --operation-type="$mode" \
  --status=$STATUS \
  --params="exit_code=$EXIT_CODE runtime=$SECONDS slurm_job=$SLURM_JOB_ID host=$(hostname)"

echo "DB updated: operation $op_id to $STATUS (exit=$EXIT_CODE) [SLURM_JOB_ID=$SLURM_JOB_ID]"

# Check if we should resubmit
if [[ $EXIT_CODE -eq 0 && "false" == "true" && $mode != "reseed" ]]; then
    next_start=$((start + n_trajec))
    if [[ $next_start -lt $cfg_max ]]; then
        echo "Resubmitting with start=$next_start in continue mode"
        # Generate new XML for continue mode
        mdwf_db hmc-xml -e $EID -m continue --params "StartTrajectory=$next_start Trajectories=$n_trajec"
        # Resubmit the job
        sbatch --dependency=afterok:$SLURM_JOBID $batch
    else
        echo "Reached target config_max=$cfg_max"
    fi
fi

exit $EXIT_CODE