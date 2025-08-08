# MDWFutils/jobs/hmc.py

import random
import xml.etree.ElementTree as ET
from pathlib import Path
from xml.dom import minidom
from MDWFutils.db import get_ensemble_details, get_connection, update_operation
import sys

def _make_default_tree(mode: str, seed_override: int = None):
    """
    Build a fresh ElementTree with defaults for tepid/continue/reseed.
    """
    # pick the seed
    if mode == 'reseed':
        seed = seed_override if seed_override is not None else random.randint(1, 10**6)
    else:
        # tepid & continue always share one seed
        seed = seed_override if seed_override is not None else random.randint(1, 10**6)

    # defaults for each mode
    if mode == 'tepid':
        start, traj = 0, 100
        stype, metropolis = 'TepidStart', False
    elif mode == 'continue':
        start, traj = 12, 20
        stype, metropolis = 'CheckpointStart', True
    elif mode == 'reseed':
        start, traj = 0, 200
        stype, metropolis = 'CheckpointStartReseed', True
    else:
        raise ValueError(f"Unknown mode '{mode}'")

    # default MD‐block
    md_name   = ['OMF2_5StepV','OMF2_5StepV','OMF4_11StepV']
    md_steps  = 1
    trajL     = 0.75
    lvl_sizes = [9,1,1]

    # build skeleton
    root = ET.Element('grid')
    hmc  = ET.SubElement(root, 'HMCparameters')
    def E(tag,val):
        ET.SubElement(hmc, tag).text = str(val)

    E('StartTrajectory',   start)
    E('Trajectories',      traj)
    E('MetropolisTest',    str(metropolis).lower())
    E('NoMetropolisUntil', 0)
    E('PerformRandomShift','false')
    E('StartingType',      stype)
    E('Seed',              seed)

    md = ET.SubElement(hmc, 'MD')
    nm = ET.SubElement(md, 'name')
    for e in md_name:
        ET.SubElement(nm, 'elem').text = e

    ET.SubElement(md, 'MDsteps').text = str(md_steps)
    ET.SubElement(md, 'trajL').text = str(trajL)

    lvl = ET.SubElement(md, 'lvl_sizes')
    for x in lvl_sizes:
        ET.SubElement(lvl, 'elem').text = str(x)

    return ET.ElementTree(root), root


def _pretty_write(tree: ET.ElementTree, path: Path):
    """
    Pretty-print the XML with indentation and write to `path`,
    but strip out any empty lines that minidom inserts.
    """
    raw = ET.tostring(tree.getroot(), encoding='utf-8')
    parsed = minidom.parseString(raw)
    pretty = parsed.toprettyxml(indent='  ')
    # drop all-blank lines
    lines = [ln for ln in pretty.splitlines() if ln.strip()]
    # re-join and ensure trailing newline
    out = '\n'.join(lines) + '\n'
    path.write_text(out, encoding='utf-8')

# -----------------------------------------------------------------------------
def generate_hmc_parameters(
    ensemble_dir: str,
    mode: str,
    **overrides
):
    """
    Create or update HMCparameters.xml under `ensemble_dir`.

    mode = 'tepid' | 'continue' | 'reseed'
    overrides = tag=value pairs (e.g. StartTrajectory=100, Trajectories=50,
    lvl_sizes='10,1,1', md_name='A,B,C', etc.)
    """
    base    = Path(ensemble_dir)
    base.mkdir(parents=True, exist_ok=True)
    xmlpath = base/'HMCparameters.xml'

    seed_override = None
    if 'Seed' in overrides:
        if mode != 'reseed':
            raise RuntimeError(f"Cannot override Seed in mode='{mode}'")
        try:
            seed_override = int(overrides.pop('Seed'))
        except ValueError:
            raise RuntimeError("Seed override must be an integer")

    # load existing xml or build fresh
    if xmlpath.exists():
        tree = ET.parse(xmlpath)
    else:
        tree, _ = _make_default_tree(mode, seed_override)
    root = tree.getroot().find('HMCparameters')
    if root is None:
        raise RuntimeError("Malformed XML: missing <HMCparameters>")

    # if we have a seed_override in reseed mode, update it
    if mode == 'reseed' and seed_override is not None:
        root.find('Seed').text = str(seed_override)

    # Update mode-specific parameters (StartingType and MetropolisTest) to match the current mode
    if mode == 'tepid':
        stype, metropolis = 'TepidStart', False
    elif mode == 'continue':
        stype, metropolis = 'CheckpointStart', True
    elif mode == 'reseed':
        stype, metropolis = 'CheckpointStartReseed', True
    
    # Update StartingType and MetropolisTest to match the current mode
    starting_type_elem = root.find('StartingType')
    metropolis_elem = root.find('MetropolisTest')
    
    if starting_type_elem is None:
        print(f"WARNING: <StartingType> element not found in XML, creating it", file=sys.stderr)
        ET.SubElement(root, 'StartingType').text = stype
    else:
        starting_type_elem.text = stype
        
    if metropolis_elem is None:
        print(f"WARNING: <MetropolisTest> element not found in XML, creating it", file=sys.stderr)
        ET.SubElement(root, 'MetropolisTest').text = str(metropolis).lower()
    else:
        metropolis_elem.text = str(metropolis).lower()

    # apply all other overrides
    for key, val in overrides.items():
        txt = str(val)
        # handle list‐tags specially
        if key == 'md_name':
            nm = root.find('MD/name')
            nm.clear()
            for e in txt.split(','):
                ET.SubElement(nm, 'elem').text = e.strip()
            continue
        if key == 'lvl_sizes':
            lvl = root.find('MD/lvl_sizes')
            lvl.clear()
            for e in txt.split(','):
                ET.SubElement(lvl, 'elem').text = e.strip()
            continue
        # try top‐level
        el = root.find(key)
        if el is not None:
            el.text = txt
            continue
        # try under MD
        el2 = root.find(f"MD/{key}")
        if el2 is not None:
            el2.text = txt
            continue
        # unknown override
        print(f"WARNING: no XML element <{key}> to override", file=sys.stderr)

    # write it out
    _pretty_write(tree, xmlpath)
    return True


# -----------------------------------------------------------------------------
def generate_hmc_slurm_gpu(
    out_path: str,
    db_file: str,
    ensemble_id: int,
    base_dir: str,
    type_: str,             # "TUNING" or "ENSEMBLES"
    ens_relpath: str,       # e.g. "b6.0/.../T32"
    ens_name: str,          # e.g. "b6.0_..._T32"
    account: str,
    mode: str,              # "tepid"|"continue"|"reseed"
    constraint: str,
    time_limit: str,
    cpus_per_task: str,
    nodes: str,
    ntasks_per_node: str,
    gpus_per_task: str,
    gpu_bind: str,
    mail_user: str,
    exec_path: str = None,       # optional - will check DB first
    bind_script: str = None,     # optional - will check DB first
    n_trajec: str = None,        
    cfg_max: str = None,
    mpi: str = None,
    resubmit: str = 'true',
    queue: str = 'regular'
):
    """
    Write a GPU SBATCH script that:
      - handles tepid/continue/reseed modes
      - sets up proper environment variables
      - runs HMC with appropriate parameters
      - handles resubmission if needed
      - tracks job history in database
    """
    # fetch ensemble metadata & parameters
    ens = get_ensemble_details(db_file, ensemble_id)
    if not ens:
        raise RuntimeError(f"Ensemble {ensemble_id} not found")
    p = ens['parameters']
    L, T = int(p['L']), int(p['T'])
    VOL = f"{L}.{L}.{L}.{T}"

    # Check for HMC parameters in ensemble parameters
    if exec_path is None:
        if 'hmc_exec_path' in p:
            exec_path = p['hmc_exec_path']
        else:
            # Prompt user for executable path
            exec_path = input("Please enter the path to the HMC executable: ").strip()
            if not exec_path:
                raise RuntimeError("HMC executable path is required")
            
            # Save to ensemble parameters
            conn = get_connection(db_file)
            c = conn.cursor()
            c.execute("""
                INSERT OR REPLACE INTO ensemble_parameters (ensemble_id, name, value)
                VALUES (?, ?, ?)
            """, (ensemble_id, 'hmc_exec_path', exec_path))
            conn.commit()
            conn.close()

    if bind_script is None:
        if 'hmc_bind_script' in p:
            bind_script = p['hmc_bind_script']
        else:
            # Prompt user for binding script path
            bind_script = input("Please enter the path to the core binding script: ").strip()
            if not bind_script:
                raise RuntimeError("Core binding script path is required")
            
            # Save to ensemble parameters
            conn = get_connection(db_file)
            c = conn.cursor()
            c.execute("""
                INSERT OR REPLACE INTO ensemble_parameters (ensemble_id, name, value)
                VALUES (?, ?, ?)
            """, (ensemble_id, 'hmc_bind_script', bind_script))
            conn.commit()
            conn.close()

    if n_trajec is None and cfg_max is not None:
        n_trajec = cfg_max
    elif n_trajec is None and cfg_max is None:
        raise RuntimeError("cfg_max must be provided (or n_trajec passed) for default.")

    # prepare output file
    script_file = Path(out_path)
    script_file.parent.mkdir(parents=True, exist_ok=True)

    ensemble_dir = Path(ens['directory']).resolve()
    
    ens_name = f"b{ens['parameters']['beta']}_b{ens['parameters']['b']}Ls{ens['parameters']['Ls']}_mc{ens['parameters']['mc']}_ms{ens['parameters']['ms']}_ml{ens['parameters']['ml']}_L{ens['parameters']['L']}_T{ens['parameters']['T']}"

    txt = f"""#!/bin/bash
#SBATCH -A {account}
#SBATCH -C {constraint}
#SBATCH -q {queue}
#SBATCH -t {time_limit}
#SBATCH --cpus-per-task={cpus_per_task}
#SBATCH -N {nodes}
#SBATCH --ntasks-per-node={ntasks_per_node}
#SBATCH --gres=gpu:{gpus_per_task}
#SBATCH --gpu-bind={gpu_bind}
#SBATCH --mail-type=BEGIN,END
#SBATCH --mail-user={mail_user}
#SBATCH --signal=B:TERM@60

batch="$0"
DB="{db_file}"
EID={ensemble_id}
mode="{mode}"
ens="{ens_name}"
ens_rel="{ens_relpath}"
VOL="{VOL}"
EXEC="{exec_path}"
BIND="{bind_script}"
n_trajec={n_trajec}
cfg_max={cfg_max}
mpi="{mpi}"

cd {ensemble_dir}

echo "ens = $ens"
echo "ens_dir = {ensemble_dir}"
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
  mdwf_db update \\
    --db-file="$DB" \\
    --ensemble-id=$EID \\
    --operation-type="$mode" \\
    --status=RUNNING \\
    --params="config_start=$start config_end=$(( start + n_trajec )) config_increment=$n_trajec slurm_job=$SLURM_JOB_ID exec_path=$EXEC bind_script=$BIND"
)
echo "$out"
op_id=${{out#*operation }}
op_id=${{op_id%%:*}}
export op_id

# Generate HMC parameters XML
mdwf_db hmc-xml -e $EID -m $mode -x "StartTrajectory=$start Trajectories=$n_trajec"

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
srun $BIND $EXEC --mpi $mpi --grid $VOL --accelerator-threads 32 --dslash-unroll --shm 2048 --comms-overlap -shm-mpi 0 > ../log_hmc/log_{ens_name}.$start
EXIT_CODE=$?
echo "STOP `date`"

# Update database with job status
STATUS=COMPLETED
[[ $EXIT_CODE -ne 0 ]] && STATUS=FAILED

mdwf_db update \\
  --db-file="$DB" \\
  --ensemble-id=$EID \\
  --operation-id=$op_id \\
  --operation-type="$mode" \\
  --status=$STATUS \\
  --params="exit_code=$EXIT_CODE runtime=$SECONDS slurm_job=$SLURM_JOB_ID host=$(hostname)"

echo "DB updated: operation $op_id to $STATUS (exit=$EXIT_CODE) [SLURM_JOB_ID=$SLURM_JOB_ID]"

# Check if we should resubmit
if [[ $EXIT_CODE -eq 0 && "{resubmit}" == "true" && $mode != "reseed" ]]; then
    next_start=$((start + n_trajec))
    if [[ $next_start -lt $cfg_max ]]; then
        # Determine next mode: tepid -> continue, continue -> continue
        next_mode="continue"
        if [[ $mode == "tepid" ]]; then
            echo "Resubmitting with start=$next_start in continue mode (transitioning from tepid)"
        else
            echo "Resubmitting with start=$next_start in continue mode"
        fi
        # Generate new XML for the next mode
        mdwf_db hmc-xml -e $EID -m $next_mode -x "StartTrajectory=$next_start Trajectories=$n_trajec"
        # Resubmit the job with the next mode by modifying the script
        # We need to update the mode variable in the resubmitted script
        sed -i "s/mode=\"$mode\"/mode=\"$next_mode\"/" $batch
        sbatch --dependency=afterok:$SLURM_JOBID $batch
    else
        echo "Reached target config_max=$cfg_max"
    fi
fi

exit $EXIT_CODE"""
    script_file.write_text(txt, encoding='utf-8')
    return True
