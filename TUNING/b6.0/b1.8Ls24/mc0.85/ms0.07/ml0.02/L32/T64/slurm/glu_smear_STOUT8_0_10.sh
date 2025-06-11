#!/usr/bin/env bash
#SBATCH -A m2986_g
#SBATCH -C gpu
#SBATCH -q regular
#SBATCH -t 06:00:00
#SBATCH -J glu_smear
#SBATCH --output=/Users/wyatt/Development/mdwf_db/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/jlog/%j.out
#SBATCH --error=/Users/wyatt/Development/mdwf_db/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/jlog/%j.err
#SBATCH -N 1
#SBATCH --cpus-per-task=16
#SBATCH --signal=B:TERM@60

module load cpu
module load intel-mixed/2023.2.0
module load cray-fftw/3.3.10.8

DB="/Users/wyatt/Development/mdwf_db/mdwf_ensembles.db"
EID=1
OP="GLU_SMEAR"
SC=0
EC=10

mkdir -p "/Users/wyatt/Development/mdwf_db/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/jlog" "/Users/wyatt/Development/mdwf_db/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/cnfg_STOUT8"

mdwf_db update \
  --db-file="$DB" \
  --ensemble-id=$EID \
  --operation-type="$OP" \
  --status=RUNNING \
  --params="config_start=$SC config_end=$EC config_increment=4 slurm_job=$SLURM_JOBID"

update_status() {
  code=$?
  status=COMPLETED
  (( code!=0 )) && status=FAILED
  mdwf_db update \
    --db-file="$DB" \
    --ensemble-id=$EID \
    --operation-type="$OP" \
    --status=$status \
    --params="slurm_job=$SLURM_JOBID runtime=$SECONDS host=$(hostname)"
  exit $code
}
trap update_status EXIT TERM INT HUP QUIT
SECONDS=0

GLU="/global/cfs/cdirs/m2986/cosmon/mdwf/software/install/GLU_ICC/bin/GLU"
step=4
nsim=8
let 'Nth=16/nsim'
export OMP_NUM_THREADS=$Nth

echo "step=$step nsim=$nsim Nth=$Nth"

let 'mxcnf=step*nsim'
for((cnf=$SC;cnf<$EC;cnf+=$mxcnf));do
    for((i=0;i<$nsim;i++));do
        let 'c=cnf+step*i'
        (( c>=EC )) && break
        
        # Calculate CPU binding for physical and logical cores
        let 'lo=i*Nth/2'
        let 'hi=lo+Nth/2-1'
        let 'loh=128+i*Nth/2'
        let 'hih=loh+Nth/2-1'
        
        echo "Config $c: CPUs $lo-$hi $loh-$hih"
        export GOMP_CPU_AFFINITY="$lo-$hi $loh-$hih"
        
        in_cfg="/Users/wyatt/Development/mdwf_db/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/cnfg/ckpoint_lat.${c}"
        out_cfg="/Users/wyatt/Development/mdwf_db/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/cnfg_STOUT8/u_stoutSTOUT8_n${c}"
        "$GLU" -i "/Users/wyatt/Development/mdwf_db/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/cnfg_STOUT8/glu_smear.in" -c "$in_cfg" -o "$out_cfg" &
    done
    wait
done

echo "Done in $SECONDS s"
