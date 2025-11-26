#!/bin/bash
#SBATCH -A m0000
#SBATCH -J mres_1
#SBATCH -C gpu
#SBATCH -q regular
#SBATCH -t 01:00:00
#SBATCH -N 1
#SBATCH --gpus=4
#SBATCH --gpu-bind=none
#SBATCH --mail-type=ALL
#SBATCH -o /global/u2/s/smithwya/test_run/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/mres/jlog/%j.log

cd "/global/u2/s/smithwya/test_run/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/mres"
mkdir -p DATA

module load conda
conda activate /global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf

# Record job start
mdwf_db update -e 1 -o WIT_MRES -s RUNNING \
  -p "slurm_job_id=$SLURM_JOB_ID config_start=0 config_end=12 config_increment=4 wit_input=/global/u2/s/smithwya/test_run/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/mres/DWF_mres.in workdir=/global/u2/s/smithwya/test_run/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/mres nodes=1 ranks=4" \
  || true

SECONDS=0

source /global/cfs/cdirs/m2986/cosmon/mdwf/software/scripts/env_gpu.sh
export LD_LIBRARY_PATH=/global/cfs/cdirs/m2986/cosmon/mdwf/software/install_gpu/quda/lib:$LD_LIBRARY_PATH

export MPICH_RDMA_ENABLED_CUDA=1
export MPICH_GPU_SUPPORT_ENABLED=1
export MPICH_NEMESIS_ASYNC_PROGRESS=1

export SLURM_CPU_BIND=cores
export CRAY_ACCEL_TARGET=nvidia80

export QUDA_RESOURCE_PATH="$(pwd)/../quda_resource"
[[ -d $QUDA_RESOURCE_PATH ]] || mkdir -p $QUDA_RESOURCE_PATH
export QUDA_ENABLE_GDR=1

export MPICH_VERSION_DISPLAY=1
export MPICH_OFI_NIC_VERBOSE=2
export MPICH_OFI_NIC_POLICY="USER"
export MPICH_OFI_NIC_MAPPING="0:3;1:2;2:1;3:0"
echo "MPICH_OFI_NIC_POLICY=${MPICH_OFI_NIC_POLICY}"
echo "MPICH_OFI_NIC_MAPPING=${MPICH_OFI_NIC_MAPPING}"

EXEC="/global/cfs/cdirs/m2986/cosmon/mdwf/software/install_gpu/wit/bin/Mres"
BIND="/global/cfs/cdirs/m2986/cosmon/mdwf/ANALYSIS/WIT/bind.sh"
WIT_INPUT="/global/u2/s/smithwya/test_run/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/mres/DWF_mres.in"

echo "Running mres range 0-12 step 4"
srun -n 4 $BIND $EXEC -i "$WIT_INPUT" \
    -ogeom 1 1 1 4 \
    -lgeom 24 24 24 12

# Capture exit code immediately
EXIT_CODE=$?

# Determine status based on exit code
if [ $EXIT_CODE -eq 0 ]; then
    # Job completed successfully
    mdwf_db update -e 1 -o WIT_MRES -s COMPLETED \
      -p "slurm_job_id=$SLURM_JOB_ID exit_code=0 runtime=$SECONDS host=$(hostname)" \
      || true
elif [ $EXIT_CODE -eq 130 ] || [ $EXIT_CODE -eq 137 ] || [ $EXIT_CODE -eq 143 ]; then
    # Job was killed/canceled
    mdwf_db update -e 1 -o WIT_MRES -s CANCELED \
      -p "slurm_job_id=$SLURM_JOB_ID exit_code=$EXIT_CODE runtime=$SECONDS host=$(hostname)" \
      || true
    exit $EXIT_CODE
else
    # Job failed
    mdwf_db update -e 1 -o WIT_MRES -s FAILED \
      -p "slurm_job_id=$SLURM_JOB_ID exit_code=$EXIT_CODE runtime=$SECONDS host=$(hostname)" \
      || true
    exit $EXIT_CODE
fi

echo "All done in $SECONDS seconds"