#!/bin/bash
#SBATCH -A m0000
#SBATCH -J smear_1
#SBATCH -C gpu
#SBATCH -q regular
#SBATCH -t 01:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=32
#SBATCH --mail-type=ALL
#SBATCH --signal=B:TERM@60
#SBATCH -o /global/u2/s/smithwya/mdwf_db/test_run/ENSEMBLES/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/cnfg_STOUT8/jlog/%j.log
#SBATCH -e /global/u2/s/smithwya/mdwf_db/test_run/ENSEMBLES/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/cnfg_STOUT8/jlog/%j.err

set -euo pipefail

cd "/global/u2/s/smithwya/mdwf_db/test_run/ENSEMBLES/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/cnfg_STOUT8"
mkdir -p jlog

module load cpu
module load intel-mixed/2023.2.0
module load cray-fftw/3.3.10.8
module load conda
conda activate /global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf

DB="mongodb://mdwf_ensembles_admin:a%20place%20for%20everything%20and%20everything%20in%20its%20place@mongodb05.nersc.gov:27017/mdwf_ensembles?authSource=mdwf_ensembles"
EID=1
OP="GLU_SMEAR"
SC=0
EC=100
IC=4
USER=$(whoami)
RUN_DIR="/global/u2/s/smithwya/mdwf_db/test_run/ENSEMBLES/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48"
PARAMS="smear_type=STOUT smiters=8"
LOGFILE="/global/cfs/cdirs/m2986/cosmon/mdwf/mdwf_update.log"

# Source logging helper via process substitution
source <(python -m MDWFutils.jobs.slurm_update_trap)
SECONDS=0

GLU="/global/cfs/cdirs/m2986/cosmon/mdwf/software/install/GLU_ICC/bin/GLU"
STEP=4
NSIM=8
let 'Nth=32 / NSIM'
export OMP_NUM_THREADS=$Nth

let 'mxcnf=STEP*NSIM'
for((cnf=$SC; cnf<$EC; cnf+=$mxcnf)); do
    for((i=0;i<NSIM;i++)); do
        let 'c=cnf+STEP*i'
        (( c>EC )) && break

        # Calculate CPU binding for physical and logical cores
        let 'lo=i*Nth/2'
        let 'hi=lo+Nth/2-1'
        let 'loh=128+i*Nth/2'
        let 'hih=loh+Nth/2-1'

        echo "Config $c: CPUs $lo-$hi $loh-$hih"
        export GOMP_CPU_AFFINITY="${lo}-${hi} ${loh}-${hih}"

        in_cfg="/global/u2/s/smithwya/mdwf_db/test_run/ENSEMBLES/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/cnfg/ckpoint_EODWF_lat.${c}"
        out_cfg="/global/u2/s/smithwya/mdwf_db/test_run/ENSEMBLES/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/cnfg_STOUT8/ckn${c}"
        "$GLU" -i "/global/u2/s/smithwya/mdwf_db/test_run/ENSEMBLES/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48/cnfg_STOUT8/glu_smear.in" -c "$in_cfg" -o "$out_cfg" &
    done
    wait
done

echo "Done in $SECONDS s"