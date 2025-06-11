# MDWF Database Tool

## Overview
The MDWF Database Tool is a command-line utility designed to manage and automate workflows for lattice QCD simulations. It provides a robust interface for generating SLURM scripts, managing ensemble parameters, and automating common tasks in the MDWF workflow.

## Installation
To install the MDWF Database Tool, follow these steps:

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/mdwf_db.git
   cd mdwf_db
   ```

2. Install the package:
   ```bash
   pip install -e .
   ```

## Usage
The MDWF Database Tool provides several commands for managing your workflow:

### Initialize the Database
To initialize a new MDWF database in your working directory, use the following command:

**Command:**
```bash
mdwf_db init-db
```

This will create a new SQLite database file (`db.sqlite`) and the required directory structure in the current directory.

**Available Options:**
- `--db-file <path>`: Path to the database file to create (default: `db.sqlite` in the current directory)
- `--base-dir <path>`: Root directory for the database and ensemble folders (default: current directory)

**Sample Command:**
```bash
mdwf_db init-db
```

**Expected Output:**
```
Ensured directory: /Users/wyatt/Development/mdwf_db/test_cli
Ensured directory: /Users/wyatt/Development/mdwf_db/test_cli/TUNING
Ensured directory: /Users/wyatt/Development/mdwf_db/test_cli/ENSEMBLES
init_database returned: True
```

**Files and Directories Created:**
- `db.sqlite` (SQLite database file)
- `TUNING/` (directory for tuning ensembles)
- `ENSEMBLES/` (directory for production ensembles)

### Add an Ensemble
To add a new ensemble to the database, use the following command:

**Command:**
```bash
mdwf_db add-ensemble -p "beta=6.0 b=1.8 Ls=24 mc=0.85 ms=0.07 ml=0.02 L=32 T=64" -s TUNING --description "Test ensemble for workflow"
```

**Available Options:**
- `-p <params>`: Space-separated key=value pairs for ensemble parameters (required)
- `-s <status>`: Ensemble status (TUNING or PRODUCTION) (required)
- `--description <text>`: Description of the ensemble (optional)

**Sample Command:**
```bash
mdwf_db add-ensemble -p "beta=6.0 b=1.8 Ls=24 mc=0.85 ms=0.07 ml=0.02 L=32 T=64" -s TUNING --description "Test ensemble for workflow"
```

**Output:**
```
Ensemble added: ID=1
```

**Resulting Directory Structure:**
```
TUNING/
└── b6.0/
    └── b1.8Ls24/
        └── mc0.85/
            └── ms0.07/
                └── ml0.02/
                    └── L32/
                        └── T64/
                            ├── cnfg/
                            ├── jlog/
                            ├── log_hmc/
                            └── slurm/
```

### Promote an Ensemble
To promote an ensemble, use the following command:

**Command:**
```bash
mdwf_db promote --db-file=/path/to/db.sqlite --ensemble-id=<ensemble_id>
```

**Parameters:**
- `--db-file=<db_file>`: Path to the database file.
- `--ensemble-id=<ensemble_id>`: The ID of the ensemble to promote.

**Sample Command:**
```bash
mdwf_db promote --db-file=/path/to/db.sqlite --ensemble-id=1
```

**Expected Output:**
```
Ensemble promoted successfully.
```

### Print History
To print the history of an ensemble, use the following command:

**Command:**
```bash
mdwf_db history --db-file=/path/to/db.sqlite --ensemble-id=<ensemble_id>
```

**Parameters:**
- `--db-file=<db_file>`: Path to the database file.
- `--ensemble-id=<ensemble_id>`: The ID of the ensemble to print history for.

**Sample Command:**
```bash
mdwf_db history --db-file=/path/to/db.sqlite --ensemble-id=1
```

**Expected Output:**
```
History for Ensemble1:
- Added on 2023-01-01
- Promoted on 2023-01-02
```

### Generate Smearing Script for an Ensemble
You can generate a smearing SLURM script for an ensemble using the following command. The script will generate both a GLU input file and an SBATCH script.

**Command:**
```bash
mdwf_db smear-script -e 1 -j "queue=regular config_start=0 config_end=10 mail_user=wyatt@example.com"
```

**Output:**
```
Generated GLU input file: /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/cnfg_STOUT8/glu_smear.in
Wrote smearing SBATCH script → /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/slurm/glu_smear_STOUT8_0_10.sh
```

**Generated Files:**
- GLU Input File: `/Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/cnfg_STOUT8/glu_smear.in`
- SBATCH Script: `/Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/slurm/glu_smear_STOUT8_0_10.sh`

**Example Generated Smearing SBATCH Script:**
```bash
#!/usr/bin/env bash
#SBATCH -A m2986_g
#SBATCH -C gpu
#SBATCH -q regular
#SBATCH -t 06:00:00
#SBATCH -J glu_smear
#SBATCH --output=/Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/jlog/%j.out
#SBATCH --error=/Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/jlog/%j.err
#SBATCH -N 1
#SBATCH --cpus-per-task=16
#SBATCH --signal=B:TERM@60

module load cpu
module load intel-mixed/2023.2.0
module load cray-fftw/3.3.10.8

DB="/Users/wyatt/Development/mdwf_db/test_cli/mdwf_ensembles.db"
EID=1
OP="GLU_SMEAR"
SC=0
EC=10

mkdir -p "/Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/jlog" "/Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/cnfg_STOUT8"

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
        
        in_cfg="/Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/cnfg/ckpoint_lat.${c}"
        out_cfg="/Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/cnfg_STOUT8/u_stoutSTOUT8_n${c}"
        "$GLU" -i "/Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/cnfg_STOUT8/glu_smear.in" -c "$in_cfg" -o "$out_cfg" &
    done
    wait
done

echo "Done in $SECONDS s"

### Generate Meson 2pt Script for an Ensemble
You can generate a meson 2pt SLURM script for an ensemble using the following command. The script will generate both a WIT input file and an SBATCH script.

**Command:**
```bash
mdwf_db meson-2pt -e 1 -j "queue=regular time_limit=1:00:00 nodes=1 cpus_per_task=16 mail_user=wyatt@example.com" -w "Configurations.first=0 Configurations.last=10"
```

**Output:**
```
Generated WIT input file: /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/meson2pt/DWF.in
Wrote meson 2pt SBATCH script → /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/meson2pt/meson2pt_0_10.sh
```

**Generated Files:**
- WIT Input File: `/Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/meson2pt/DWF.in`
- SBATCH Script: `/Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/meson2pt/meson2pt_0_10.sh`

**Example Generated Meson 2pt SBATCH Script:**
```bash
#!/bin/bash
#SBATCH -A m2986_g
#SBATCH --nodes=1
#SBATCH -C gpu
#SBATCH --gpus=4
#SBATCH --time=1:00:00
#SBATCH --qos=regular
#SBATCH --mail-user=wyatt@example.com
#SBATCH --mail-type=ALL
#SBATCH -o /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/meson2pt/jlog/%j.log

set -euo pipefail
cd /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/meson2pt

#record RUNNING (one shot for the entire meson2pt job)
mdwf_db update \
  --db-file="/Users/wyatt/Development/mdwf_db/test_cli/mdwf_ensembles.db" \
  --ensemble-id=1 \
  --operation-type="WIT_MESON2PT" \
  --status="RUNNING" \
  --params="Configurations={'first': 0, 'last': 10, 'step': '4'}"

# On exit/failure, update status + code + runtime
update_status() {
  local EC=$?
  local ST="COMPLETED"
  [[ $EC -ne 0 ]] && ST="FAILED"

  mdwf_db update \
    --db-file="/Users/wyatt/Development/mdwf_db/test_cli/mdwf_ensembles.db" \
    --ensemble-id=1 \
    --operation-type="WIT_MESON2PT" \
    --status="$ST" \
    --exit-code=$EC \
    --runtime=$SECONDS \
    --params="slurm_job=$SLURM_JOB_ID host=$(hostname)"

  echo "Meson2pt job $ST ($EC)"
}
trap update_status EXIT TERM INT HUP QUIT

SECONDS=0

source /global/cfs/cdirs/m2986/cosmon/mdwf/software/scripts/env_gpu.sh
export LD_LIBRARY_PATH=/global/cfs/cdirs/m2986/cosmon/mdwf/software/install_gpu/quda/lib:$LD_LIBRARY_PATH

### MPI flags
export MPICH_RDMA_ENABLED_CUDA=1
export MPICH_GPU_SUPPORT_ENABLED=1
export MPICH_NEMESIS_ASYNC_PROGRESS=1

### Cray/Slurm flags
export OMP_NUM_THREADS=16
export SLURM_CPU_BIND=cores
export CRAY_ACCEL_TARGET=nvidia80

### QUDA specific flags
export QUDA_RESOURCE_PATH=`pwd`/quda_resource
[[ -d $QUDA_RESOURCE_PATH ]] || mkdir -p $QUDA_RESOURCE_PATH
export QUDA_ENABLE_GDR=1

### MPICH debugging flags
export MPICH_VERSION_DISPLAY=1
export MPICH_OFI_NIC_VERBOSE=2
export MPICH_OFI_NIC_POLICY="USER"
export MPICH_OFI_NIC_MAPPING="0:3;1:2;2:1;3:0"
echo "MPICH_OFI_NIC_POLICY=${MPICH_OFI_NIC_POLICY}"
echo "MPICH_OFI_NIC_MAPPING=${MPICH_OFI_NIC_MAPPING}"

# Generate random seed for each config
generate_seed() {
    local cfg=$1
    # Use config number as part of seed to ensure uniqueness
    echo $(( (RANDOM + cfg) % 10000 ))
}

# loop over cfg numbers
for cfg in $(seq 0 4 10); do
    if [[ ! -e DATA/Meson_2pt_00u_stout8n${cfg}.bin ]]; then
        # Generate new seed for this config
        seed=$(generate_seed $cfg)
        
        # Generate WIT input for this config, only changing seed and config numbers
        mdwf_db wit-input -e 1 -o DWF.in \
            -w "Configurations.first=$cfg Configurations.last=$cfg \
                Random number generator.seed=$seed \
                Propagator 0.Seed=$seed Propagator 1.Seed=$seed Propagator 2.Seed=$seed"
        
        echo "Running cfg $cfg with seed $seed"
        srun -n 4 /global/cfs/cdirs/m2986/cosmon/mdwf/ANALYSIS/WIT/bind.sh /global/cfs/cdirs/m2986/cosmon/mdwf/software/install_gpu/wit/bin/Meson \
             -i DWF.in -ogeom 1 1 1 4 \
             -lgeom 32 32 32 16
    fi
done

echo "All done in $SECONDS seconds"

### Generate HMC Script for an Ensemble
You can generate an HMC SLURM script for an ensemble using the following command. The script will prompt for the HMC executable and core binding script if not already set.

**Command:**
```bash
mdwf_db hmc-script -e 1 -a m2986_g -m tepid -j "queue=regular cfg_max=10 mail_user=wyatt@example.com"
```

**Output:**
```
Please enter the path to the HMC executable: test/hmc_exec
Please enter the path to the core binding script: test/bind
Wrote HMC sbatch -> /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/slurm/hmc_1_tepid.sbatch
```

**Generated Script Location:**
```
/Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/slurm/hmc_1_tepid.sbatch
```

**Example Generated HMC SBATCH Script:**
```bash
#!/bin/bash
#SBATCH -A m2986_g
#SBATCH -C gpu
#SBATCH -q regular
#SBATCH -t 17:00:00
#SBATCH --cpus-per-task=32
#SBATCH -N 1
#SBATCH --ntasks-per-node=32
#SBATCH --gres=gpu:1
#SBATCH --gpu-bind=none
#SBATCH --mail-type=BEGIN,END
#SBATCH --mail-user=wyatt@example.com
#SBATCH --signal=B:TERM@60

batch="$0"
DB="/Users/wyatt/Development/mdwf_db/test_cli/mdwf_ensembles.db"
EID=1
mode="tepid"
ens="TUNING_b6.0_b1.8Ls24_mc0.85_ms0.07_ml0.02_L32_T64"
ens_rel="TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64"
VOL="32.32.32.64"
EXEC="test/hmc_exec"
BIND="test/bind"
n_trajec=10
cfg_max=10
mpi="2.1.1.2"

cd /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64

echo "ens = $ens"
echo "ens_dir = /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64"
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
out=$(\
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
srun $BIND $EXEC --mpi $mpi --grid $VOL --accelerator-threads 32 --dslash-unroll --shm 2048 --comms-overlap -shm-mpi 0 > ../log_hmc/log_${'id': 1, 'directory': '/Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64', 'creation_time': '2025-06-11T15:32:53.297052', 'description': 'Test ensemble for workflow', 'status': 'TUNING', 'parameters': {'L': '32', 'Ls': '24', 'T': '64', 'b': '1.8', 'beta': '6.0', 'mc': '0.85', 'ml': '0.02', 'ms': '0.07'}, 'operation_count': 0}.$start
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

echo "DB updated: operation $op_id → $STATUS (exit=$EXIT_CODE) [SLURM_JOB_ID=$SLURM_JOB_ID]"

# Check if we should resubmit
if [[ $EXIT_CODE -eq 0 && "true" == "true" && $mode != "reseed" ]]; then
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
```

### Manage Ensemble Parameters
Use the `mdwf_db` command to update and manage ensemble parameters.

**Command:**
```bash
mdwf_db update --db-file=<db_file> --ensemble-id=<ensemble_id> --operation-type=<operation> --status=<status> --params=<params>
```

**Parameters:**
- `--db-file=<db_file>`: Path to the database file.
- `--ensemble-id=<ensemble_id>`: The ID of the ensemble to update.
- `--operation-type=<operation>`: The type of operation (e.g., "WIT_MESON2PT").
- `--status=<status>`: The status to set (e.g., "RUNNING", "COMPLETED", "FAILED").
- `--params=<params>`: Additional parameters to include.

**Sample Command:**
```bash
mdwf_db update --db-file=/path/to/db.sqlite --ensemble-id=1 --operation-type="WIT_MESON2PT" --status="RUNNING" --params="slurm_job=12345 host=node1"
```

**Expected Output:**
```
Ensemble parameters updated successfully.
```

### Add Another Ensemble and Query Ensembles
You can add additional ensembles with different parameters. After adding, you can query all ensembles to see a list, promote an ensemble, and query a single ensemble for a detailed view.

**Add a Second Ensemble:**
```bash
mdwf_db add-ensemble -p "beta=6.0 b=1.8 Ls=16 mc=0.80 ms=0.06 ml=0.01 L=24 T=48" -s TUNING --description "Second test ensemble"
```
**Output:**
```
✅ Ensemble added: ID=2
```

**Query All Ensembles:**
```bash
mdwf_db query
```
**Output:**
```
[1] (TUNING) /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64
[2] (TUNING) /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls16/mc0.80/ms0.06/ml0.01/L24/T48
```

**Promote an Ensemble:**
```bash
mdwf_db promote-ensemble -e 2
```
**Output:**
```
Promote ensemble 2:
  from /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls16/mc0.80/ms0.06/ml0.01/L24/T48
    to /Users/wyatt/Development/mdwf_db/test_cli/ENSEMBLES/b6.0/b1.8Ls16/mc0.80/ms0.06/ml0.01/L24/T48
Proceed? (y/N) y
Created operation 1: Created
Promotion OK
```

**Query a Single Ensemble (Detailed View):**
```bash
mdwf_db query -e 2 --detailed
```
**Output:**
```
ID          = 2
Directory   = /Users/wyatt/Development/mdwf_db/test_cli/ENSEMBLES/b6.0/b1.8Ls16/mc0.80/ms0.06/ml0.01/L24/T48
Status      = PRODUCTION
Created     = 2025-06-11T15:36:14.231365
Description = Second test ensemble
Parameters:
    L = 24
    Ls = 16
    T = 48
    b = 1.8
    beta = 6.0
    mc = 0.80
    ml = 0.01
    ms = 0.06

=== Operation history ===
Op 1: PROMOTE_ENSEMBLE [COMPLETED]
  Created: 2025-06-11T15:36:27.298971
  Updated: 2025-06-11T15:36:27.298971
```

**Resulting Directory Structure for Promoted Ensemble:**
```
ENSEMBLES/
└── b6.0/
    └── b1.8Ls16/
        └── mc0.80/
            └── ms0.06/
                └── ml0.01/
                    └── L24/
                        └── T48/
                            ├── cnfg/
                            ├── jlog/
                            ├── log_hmc/
                            └── slurm/
```

### Update an Ensemble and View History
You can update an ensemble by adding a new operation (such as a note or status change). This is useful for tracking manual interventions or additional metadata.

**Update an Ensemble:**
```bash
mdwf_db update --ensemble-id=2 --operation-type=NOTE --status=COMPLETED --params "note=Testing_update_command"
```
**Output:**
```
Created operation 2: Created
```

**Query the Ensemble to View Updated History:**
```bash
mdwf_db query -e 2 --detailed
```
**Output:**
```
ID          = 2
Directory   = /Users/wyatt/Development/mdwf_db/test_cli/ENSEMBLES/b6.0/b1.8Ls16/mc0.80/ms0.06/ml0.01/L24/T48
Status      = PRODUCTION
Created     = 2025-06-11T15:36:14.231365
Description = Second test ensemble
Parameters:
    L = 24
    Ls = 16
    T = 48
    b = 1.8
    beta = 6.0
    mc = 0.80
    ml = 0.01
    ms = 0.06

=== Operation history ===
Op 1: PROMOTE_ENSEMBLE [COMPLETED]
  Created: 2025-06-11T15:36:27.298971
  Updated: 2025-06-11T15:36:27.298971
Op 2: NOTE [COMPLETED]
  Created: 2025-06-11T15:37:26.713743
  Updated: 2025-06-11T15:37:26.713743
    note = Testing_update_command
```

## Workflow Log
Below is a log of all operations performed during the testing and documentation of the MDWF Database Tool.

### Initialize Database
**Command:**
```bash
mdwf_db init --db-file=mdwf_ensembles.db
```
**Output:**
```
Database initialized at mdwf_ensembles.db
```

### Add First Ensemble
**Command:**
```bash
mdwf_db add-ensemble -p "beta=6.0 b=1.8 Ls=24 mc=0.85 ms=0.07 ml=0.02 L=32 T=64" -s TUNING --description "Test ensemble for workflow"
```
**Output:**
```
Ensemble added: ID=1
```

### Query All Ensembles
**Command:**
```bash
mdwf_db query
```
**Output:**
```
[1] (TUNING) /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64
```

### Add Second Ensemble
**Command:**
```bash
mdwf_db add-ensemble -p "beta=6.0 b=1.8 Ls=16 mc=0.80 ms=0.06 ml=0.01 L=24 T=48" -s TUNING --description "Second test ensemble"
```
**Output:**
```
✅ Ensemble added: ID=2
```

### Query All Ensembles Again
**Command:**
```bash
mdwf_db query
```
**Output:**
```
[1] (TUNING) /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64
[2] (TUNING) /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls16/mc0.80/ms0.06/ml0.01/L24/T48
```

### Promote an Ensemble
**Command:**
```bash
mdwf_db promote-ensemble -e 2
```
**Output:**
```
Promote ensemble 2:
  from /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls16/mc0.80/ms0.06/ml0.01/L24/T48
    to /Users/wyatt/Development/mdwf_db/test_cli/ENSEMBLES/b6.0/b1.8Ls16/mc0.80/ms0.06/ml0.01/L24/T48
Proceed? (y/N) y
Created operation 1: Created
Promotion OK
```

### Query a Single Ensemble (Detailed View)
**Command:**
```bash
mdwf_db query -e 2 --detailed
```
**Output:**
```
ID          = 2
Directory   = /Users/wyatt/Development/mdwf_db/test_cli/ENSEMBLES/b6.0/b1.8Ls16/mc0.80/ms0.06/ml0.01/L24/T48
Status      = PRODUCTION
Created     = 2025-06-11T15:36:14.231365
Description = Second test ensemble
Parameters:
    L = 24
    Ls = 16
    T = 48
    b = 1.8
    beta = 6.0
    mc = 0.80
    ml = 0.01
    ms = 0.06

=== Operation history ===
Op 1: PROMOTE_ENSEMBLE [COMPLETED]
  Created: 2025-06-11T15:36:27.298971
  Updated: 2025-06-11T15:36:27.298971
```

### Update an Ensemble
**Command:**
```bash
mdwf_db update --ensemble-id=2 --operation-type=NOTE --status=COMPLETED --params "note=Testing_update_command"
```
**Output:**
```
Created operation 2: Created
```

### Query the Ensemble After Update
**Command:**
```bash
mdwf_db query -e 2 --detailed
```
**Output:**
```
ID          = 2
Directory   = /Users/wyatt/Development/mdwf_db/test_cli/ENSEMBLES/b6.0/b1.8Ls16/mc0.80/ms0.06/ml0.01/L24/T48
Status      = PRODUCTION
Created     = 2025-06-11T15:36:14.231365
Description = Second test ensemble
Parameters:
    L = 24
    Ls = 16
    T = 48
    b = 1.8
    beta = 6.0
    mc = 0.80
    ml = 0.01
    ms = 0.06

=== Operation history ===
Op 1: PROMOTE_ENSEMBLE [COMPLETED]
  Created: 2025-06-11T15:36:27.298971
  Updated: 2025-06-11T15:36:27.298971
Op 2: NOTE [COMPLETED]
  Created: 2025-06-11T15:37:26.713743
  Updated: 2025-06-11T15:37:26.713743
    note = Testing_update_command
```

### Generate HMC Script
**Command:**
```bash
mdwf_db hmc-script -e 1 -a m2986_g -m tepid -j "queue=regular cfg_max=10 mail_user=wyatt@example.com"
```
**Output:**
```
Please enter the path to the HMC executable: test/hmc_exec
Please enter the path to the core binding script: test/bind
Wrote HMC sbatch -> /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/slurm/hmc_1_tepid.sbatch
```

### Generate Smearing Script
**Command:**
```bash
mdwf_db smear-script -e 1 -j "queue=regular config_start=0 config_end=10 mail_user=wyatt@example.com"
```
**Output:**
```
Generated GLU input file: /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/cnfg_STOUT8/glu_smear.in
Wrote smearing SBATCH script → /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/slurm/glu_smear_STOUT8_0_10.sh
```

### Generate Meson 2pt Script
**Command:**
```bash
mdwf_db meson-2pt -e 1 -j "queue=regular time_limit=1:00:00 nodes=1 cpus_per_task=16 mail_user=wyatt@example.com" -w "Configurations.first=0 Configurations.last=10"
```
**Output:**
```
Generated WIT input file: /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/meson2pt/DWF.in
Generated WIT SBATCH script: /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/meson2pt/meson2pt_0_10.sh
```

## Contributing
Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License
This project is licensed under the MIT License - see the LICENSE file for details. 