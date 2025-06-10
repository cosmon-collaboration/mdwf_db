# MDWFutils/jobs/hmc.py

import random
import xml.etree.ElementTree as ET
from pathlib import Path
from xml.dom import minidom
from pathlib import Path
from db import get_ensemble_details
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

    E('MDsteps',  md_steps)
    E('trajL',    trajL)

    lvl = ET.SubElement(md, 'lvl_sizes')
    for x in lvl_sizes:
        ET.SubElement(lvl, 'elem').text = str(x)

    return ET.ElementTree(root), root


def _pretty_write(tree: ET.ElementTree, path: Path):
    """
    Pretty-print the XML with indentation and write to `path`.
    """
    raw = ET.tostring(tree.getroot(), encoding='utf-8')
    parsed = minidom.parseString(raw)
    pretty = parsed.toprettyxml(indent='  ')
    path.write_text(pretty, encoding='utf-8')


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

    An override for the <Seed> tag is only allowed if mode=='reseed'. 
    In other modes it raises an error.
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
    queue: str,
    time_limit: str,
    cpus_per_task: str,
    nodes: str,
    ntasks_per_node: str,
    gpus_per_task: str,
    gpu_bind: str,
    mail_user: str,
    exec_path: str,
    bind_script: str,
    n_trajec: str,
    cfg_max: str,
    mpi: str,
    resubmit: str = 'true'
):
    """
    Write a GPU SBATCH script that:
      - creates a RUNNING operation and grabs its op_id (recording slurm_job too)
      - traps EXIT/TERM/INT to UPDATE that same op_id (again recording slurm_job)
      - tees program output to ../log_hmc/run_${start}.log
      - appends SLURM_JOB_ID at end of that log
      - re-queues if needed
    """
    ens = get_ensemble_details(db_file, ensemble_id)
    if not ens:
        raise RuntimeError(f"Ensemble {ensemble_id} not found")
    p = ens['parameters']
    L, T = int(p['L']), int(p['T'])
    VOL = f"{L}.{L}.{L}.{T}"

    script_file = Path(out_path)
    script_file.parent.mkdir(parents=True, exist_ok=True)

    # build optional requeue block
    requeue_blk = ""
    if resubmit.lower() in ('1','y','yes','true') and mode != 'reseed':
        requeue_blk = f"""
# re-queue if under cfg_max and not a reseed job
if (( start < {cfg_max} )); then
  sbatch --dependency=afterok:$SLURM_JOBID $batch
fi
"""

    # NB: we use {{ and }} around 'start' so that f-string emits "${start}"
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

cd {base_dir}/$type/$ens_rel
mkdir -p cnfg log_hmc jlog

# find last config index
start=$(ls -v cnfg/ | grep lat | tail -1 | sed 's/[^0-9]*//g')
if [[ -z $start ]]; then start=0; fi
echo "START = $start (mode=$mode)"


# ------------------------------------------------------------------------
# Update ensemble's history in db to show running/ending job
# ------------------------------------------------------------------------
out=$(
  mdwf_db.py update \\
    --db-file="$DB" \\
    --ensemble-id=$EID \\
    --operation-type="$mode" \\
    --status=RUNNING \\
    --params="config_start=$start config_end=$(( start + n_trajec )) config_increment=$n_trajec slurm_job=$SLURM_JOB_ID"
)
echo "$out"
op_id=${{out#*operation }}
op_id=${{op_id%%:*}}
export op_id

update_status() {{
  EXIT_CODE=$?
  STATUS=COMPLETED
  [[ $EXIT_CODE -ne 0 ]] && STATUS=FAILED

  mdwf_db.py update \\
    --db-file="$DB" \\
    --ensemble-id=$EID \\
    --operation-id=$op_id \\
    --operation-type="$mode" \\
    --status=$STATUS \\
    --params="exit_code=$EXIT_CODE runtime=$SECONDS slurm_job=$SLURM_JOB_ID host=$(hostname)"

  echo "DB updated: operation $op_id → $STATUS (exit=$EXIT_CODE) [SLURM_JOB_ID=$SLURM_JOB_ID]"
  exit $EXIT_CODE
}}
trap update_status EXIT TERM INT HUP QUIT
SECONDS=0

# ------------------------------------------------------------------------

# copy in the pre-generated XML and cd into cnfg
mdwf_db hmc-xml -e {ensemble_id} -m continue --params "StartTrajectory=$start"

cp HMCparameters.xml cnfg/
cd cnfg

# environment setup...
export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
export CRAY_ACCEL_TARGET=nvidia80
export MPICH_OFI_NIC_POLICY=GPU
export MPICH_GPU_SUPPORT_ENABLED=1
export MPICH_RDMA_ENABLED_CUDA=1
export MPICH_GPU_IPC_ENABLED=1
export MPICH_GPU_EAGER_REGISTER_HOST_MEM=0
export MPICH_GPU_NO_ASYNC_MEMCPY=0

echo "RUNNING srun at $(date)"
srun $BIND $EXEC --mpi $mpi --grid $VOL 2>&1 | tee ../log_hmc/run_${{start}}.log
echo "SLURM_JOB_ID: $SLURM_JOB_ID" >> ../log_hmc/run_${{start}}.log

{requeue_blk}
"""
    script_file.write_text(txt, encoding='utf-8')
    return True