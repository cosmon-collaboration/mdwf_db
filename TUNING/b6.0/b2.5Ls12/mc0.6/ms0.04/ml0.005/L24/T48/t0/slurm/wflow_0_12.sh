#!/bin/bash
#SBATCH -A m0000
#SBATCH -J wflow_1
#SBATCH -C gpu
#SBATCH -q regular
#SBATCH -t 01:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --mail-type=ALL
#SBATCH --signal=B:TERM@60
#SBATCH -o /global/u2/s/smithwya/mdwf_db/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/t0/jlog/%j.log
#SBATCH -e /global/u2/s/smithwya/mdwf_db/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/t0/jlog/%j.err

set -euo pipefail

cd "/global/u2/s/smithwya/mdwf_db/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/t0"
mkdir -p jlog

module load cpu
module load intel-mixed/2023.2.0
module load cray-fftw/3.3.10.8
module load conda
conda activate /global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf

# On many network filesystems WAL journal mode causes SQLite disk I/O errors.
export MDWF_DB_JOURNAL="${MDWF_DB_JOURNAL:-DELETE}"

# Record job start
mdwf_db update -e 1 -o GLU_WFLOW -s RUNNING \
  -p "slurm_job_id=$SLURM_JOB_ID config_start=0 config_end=12 config_increment=4 glu_input=/global/u2/s/smithwya/mdwf_db/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/t0/glu_smear.in workdir=/global/u2/s/smithwya/mdwf_db/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/t0 nodes=1 smear_type=ADAPTWFLOW_STOUT smiters=250" \
  || true

SECONDS=0

GLU="/global/cfs/cdirs/m2986/cosmon/mdwf/software/install/GLU_ICC/bin/GLU"
STEP=4
NSIM=4
let 'Nth=32 / NSIM'
export OMP_NUM_THREADS=$Nth

let 'mxcnf=STEP*NSIM'
for((cnf=0;cnf<12;cnf+=$mxcnf)); do
    for((i=0;i<NSIM;i++)); do
        let 'c=cnf+STEP*i'
        (( c>12 )) && break

        let 'lo=i*Nth/2'
        let 'hi=lo+Nth/2-1'
        let 'loh=128+i*Nth/2'
        let 'hih=loh+Nth/2-1'

        echo "Config $c: CPUs $lo-$hi $loh-$hih"
        export GOMP_CPU_AFFINITY="${lo}-${hi} ${loh}-${hih}"

        in_cfg="/global/u2/s/smithwya/mdwf_db/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/cnfg/ckpoint_EODWF_lat.${c}"
        out_cfg="/global/u2/s/smithwya/mdwf_db/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/t0/t0.${c}.out"
        "$GLU" -i "/global/u2/s/smithwya/mdwf_db/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/t0/glu_smear.in" -c "$in_cfg" > "$out_cfg" &
    done
    wait
done

# Capture exit code immediately
EXIT_CODE=$?

# Determine status based on exit code
if [ $EXIT_CODE -eq 0 ]; then
    # Job completed successfully
    mdwf_db update -e 1 -o GLU_WFLOW -s COMPLETED \
      -p "slurm_job_id=$SLURM_JOB_ID exit_code=0 runtime=$SECONDS host=$(hostname)" \
      || true
elif [ $EXIT_CODE -eq 130 ] || [ $EXIT_CODE -eq 137 ] || [ $EXIT_CODE -eq 143 ]; then
    # Job was killed/canceled
    mdwf_db update -e 1 -o GLU_WFLOW -s CANCELED \
      -p "slurm_job_id=$SLURM_JOB_ID exit_code=$EXIT_CODE runtime=$SECONDS host=$(hostname)" \
      || true
    exit $EXIT_CODE
else
    # Job failed
    mdwf_db update -e 1 -o GLU_WFLOW -s FAILED \
      -p "slurm_job_id=$SLURM_JOB_ID exit_code=$EXIT_CODE runtime=$SECONDS host=$(hostname)" \
      || true
    exit $EXIT_CODE
fi

echo "Done in $SECONDS s"