#!/bin/bash
#SBATCH -A m0000
#SBATCH -J mresmq_1
#SBATCH -C gpu
#SBATCH -q regular
#SBATCH -t 01:00:00
#SBATCH -N 1
#SBATCH --gpus=4
#SBATCH --gpu-bind=none
#SBATCH --mail-type=ALL
#SBATCH -o /global/u2/s/smithwya/mdwf_db/test_run/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/mres_mq/jlog/%j.log

cd "/global/u2/s/smithwya/mdwf_db/test_run/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/mres_mq"
mkdir -p DATA

module load conda
conda activate /global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf

DB="mongodb://mdwf_ensembles_admin:a%20place%20for%20everything%20and%20everything%20in%20its%20place@mongodb05.nersc.gov:27017/mdwf_ensembles?authSource=mdwf_ensembles"
EID=1
OP="WIT_MRES_MQ"
SC=0
EC=100
IC=1
USER=$(whoami)
RUN_DIR="/global/u2/s/smithwya/mdwf_db/test_run/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48"
PARAMS="kappaC=0.108696"
LOGFILE="/global/cfs/cdirs/m2986/cosmon/mdwf/mdwf_update.log"

# Source logging helper via process substitution
source <(python -m MDWFutils.jobs.slurm_update_trap)

SECONDS=0

source /global/cfs/cdirs/m2986/cosmon/mdwf/software/scripts/env_gpu.sh
export LD_LIBRARY_PATH=/global/cfs/cdirs/m2986/cosmon/mdwf/software/install_gpu/quda/lib:$LD_LIBRARY_PATH

export MPICH_RDMA_ENABLED_CUDA=1
export MPICH_GPU_SUPPORT_ENABLED=1
export MPICH_NEMESIS_ASYNC_PROGRESS=1

export SLURM_CPU_BIND=cores
export CRAY_ACCEL_TARGET=nvidia80

export QUDA_RESOURCE_PATH="$(pwd)"/../quda_resource
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
WIT_INPUT="/global/u2/s/smithwya/mdwf_db/test_run/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/mres_mq/DWF_mres_mq.in"

echo "Running mres-mq range 0-100 step 1"
srun -n 4 "$BIND" "$EXEC" -i "$WIT_INPUT" \
    -ogeom 1 1 1 4 \
    -lgeom 24 24 24 12

echo "All done in $SECONDS seconds"