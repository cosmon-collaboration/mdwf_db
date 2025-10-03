# MDWFutils/jobs/hmc.py

import random
import xml.etree.ElementTree as ET
from pathlib import Path
from xml.dom import minidom
from MDWFutils.db import get_ensemble_details, get_connection, update_operation
from MDWFutils.jobs.slurm_update_trap import get_slurm_update_trap_inline
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
    mail_user: str = None,
    exec_path: str = None,       # optional - will check DB first
    bind_script: str = None,     # optional - will check DB first
    run_dir: str = None,
    gres: str = None,
    n_trajec: str = None,
    mpi: str = None,
    omp_num_threads: str = None,
    queue: str = 'regular',
    trajL: str = None,           # required - trajectory length
    lvl_sizes: str = None        # required - level sizes as comma-separated string
):
    """
    Write a GPU SBATCH script that:
      - handles tepid/continue/reseed modes
      - sets up proper environment variables
      - runs HMC with appropriate parameters
      - tracks job history in database
    """
    # fetch ensemble metadata & parameters
    ens = get_ensemble_details(db_file, ensemble_id)
    if not ens:
        raise RuntimeError(f"Ensemble {ensemble_id} not found")
    p = ens['parameters']
    L, T = int(p['L']), int(p['T'])
    VOL = f"{L}.{L}.{L}.{T}"

    # Get HMC executable path: prefer explicit arg, then ensemble param; otherwise error
    if exec_path is None:
        if 'hmc_exec_path' in p:
            exec_path = p['hmc_exec_path']
        else:
            raise RuntimeError("HMC executable path (exec_path) is required. Pass via CLI or save in ensemble parameters as hmc_exec_path.")

    if bind_script is None:
        if 'hmc_bind_script_gpu' in p:
            bind_script = p['hmc_bind_script_gpu']
        elif 'hmc_bind_script' in p:  # backward compatibility
            bind_script = p['hmc_bind_script']
        else:
            raise RuntimeError("HMC GPU binding script path (bind_script) is required. Pass via CLI or save in ensemble parameters as hmc_bind_script_gpu.")

    if n_trajec is None:
        raise RuntimeError("n_trajec (Trajectories) must be provided from XML params.")

    # Validate required HMC parameters
    if trajL is None:
        raise RuntimeError("trajL parameter is required")
    if lvl_sizes is None:
        raise RuntimeError("lvl_sizes parameter is required")

    # prepare output file
    script_file = Path(out_path)
    script_file.parent.mkdir(parents=True, exist_ok=True)

    ensemble_dir = Path(ens['directory']).resolve()
    work_root = Path(run_dir).resolve() if run_dir else ensemble_dir
    
    # Make database path absolute for robustness
    db_file_abs = Path(db_file).resolve()
    
    ens_name = f"b{ens['parameters']['beta']}_b{ens['parameters']['b']}Ls{ens['parameters']['Ls']}_mc{ens['parameters']['mc']}_ms{ens['parameters']['ms']}_ml{ens['parameters']['ml']}_L{ens['parameters']['L']}_T{ens['parameters']['T']}"

    # Build optional SBATCH resource lines
    gres_line = f"#SBATCH --gres={gres}" if gres else f"#SBATCH --gres=gpu:{gpus_per_task}"
    # OMP threads default
    omp_threads = str(omp_num_threads) if omp_num_threads else "16"

    txt = f"""#!/bin/bash
#SBATCH -A {account}
#SBATCH -C {constraint}
#SBATCH -q {queue}
#SBATCH -t {time_limit}
#SBATCH --cpus-per-task={cpus_per_task}
#SBATCH -N {nodes}
#SBATCH --ntasks-per-node={ntasks_per_node}
#SBATCH --gpus-per-task={gpus_per_task}
#SBATCH --gpu-bind={gpu_bind}
#SBATCH --gres={gres if gres else f"gpu:{gpus_per_task}"}
#SBATCH --mail-type=BEGIN,END
{f"#SBATCH --mail-user={mail_user}" if mail_user else ""}
#SBATCH --signal=B:TERM@60

batch="$0"
DB="{db_file_abs}"
EID={ensemble_id}
mode="{mode}"
ens="{ens_name}"
ens_rel="{ens_relpath}"
VOL="{VOL}"
EXEC="{exec_path}"
BIND="{bind_script}"
n_trajec={n_trajec}
mpi="{mpi}"
trajL="{trajL}"
lvl_sizes="{lvl_sizes}"
work_root="{str(work_root)}"

cd "$work_root"
LOGFILE="/global/cfs/cdirs/m2986/cosmon/mdwf/mdwf_update.log"

echo "ens = $ens"
echo "ens_dir = {ensemble_dir}"
echo "work_root = $work_root"
echo "EXEC = $EXEC"
echo "BIND = $BIND"
echo "n_trajec = $n_trajec"


mkdir -p cnfg
mkdir -p log_hmc
mkdir -p out
module load conda
conda activate /global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf

  # Ensure SQLite uses a network-FS friendly journal mode to avoid 'disk I/O error'
  export MDWF_DB_JOURNAL=${{MDWF_DB_JOURNAL:-DELETE}}


start=`ls -v cnfg/| grep lat | tail -1 | sed 's/[^0-9]*//g'`
if [[ -z $start ]]; then
    echo "no configs - start is empty - doing TepidStart"
    start=0
fi



echo "cfg_current = $start"

# Prepare DB update variables and queue RUNNING update off-node
USER=$(whoami)
OP="HMC_$mode"
SC=$start
EC=$(( start + n_trajec ))
# No increment tracking for HMC; EC reflects StartTrajectory + Trajectories
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
export OMP_NUM_THREADS={omp_threads}

echo "Nthreads $OMP_NUM_THREADS"

echo "START `date`"
srun $BIND $EXEC --mpi $mpi --grid $VOL --accelerator-threads 32 --dslash-unroll --shm 2048 --comms-overlap -shm-mpi 0 > ../log_hmc/log_{ens_name}.$start
echo "STOP `date`"

echo "All done in $SECONDS seconds"
"""
    script_file.write_text(txt, encoding='utf-8')
    return True


def generate_hmc_slurm_cpu(
    out_path: str,
    db_file: str,
    ensemble_id: int,
    base_dir: str,
    type_: str,
    ens_relpath: str,
    ens_name: str,
    account: str,
    mode: str,
    constraint: str,
    time_limit: str,
    cpus_per_task: str,
    nodes: str,
    ntasks_per_node: str,
    mail_user: str = None,
    exec_path: str = None,
    bind_script: str = None,
    run_dir: str = None,
    n_trajec: str = None,
    queue: str = 'regular',
    mpi: str = None,
    cacheblocking: str = None,
    omp_num_threads: str = None,
    trajL: str = None,
    lvl_sizes: str = None
):
    """
    CPU variant of HMC SLURM script: omits GPU directives, reuses logging and XML logic.
    """
    ens = get_ensemble_details(db_file, ensemble_id)
    if not ens:
        raise RuntimeError(f"Ensemble {ensemble_id} not found")
    p = ens['parameters']
    L, T = int(p['L']), int(p['T'])
    VOL = f"{L}.{L}.{L}.{T}"

    if exec_path is None:
        exec_path = p.get('hmc_exec_path')
        if exec_path is None:
            raise RuntimeError("HMC executable path (exec_path) is required. Pass via CLI or save in ensemble parameters as hmc_exec_path.")
    if bind_script is None:
        bind_script = p.get('hmc_bind_script_cpu')
    if bind_script is None:
        raise RuntimeError("HMC CPU binding script path (bind_script) is required. Pass via CLI or save in ensemble parameters as hmc_bind_script_cpu.")

    if n_trajec is None:
        raise RuntimeError("n_trajec (Trajectories) must be provided from XML params.")

    if trajL is None:
        raise RuntimeError("trajL parameter is required")
    if lvl_sizes is None:
        raise RuntimeError("lvl_sizes parameter is required")

    script_file = Path(out_path)
    script_file.parent.mkdir(parents=True, exist_ok=True)

    ensemble_dir = Path(ens['directory']).resolve()
    work_root = Path(run_dir).resolve() if run_dir else ensemble_dir
    db_file_abs = Path(db_file).resolve()

    # defaults for optional compute params
    mpi_val = mpi if mpi else "4.4.4.8"
    cb_val = cacheblocking if cacheblocking else "2.2.2.2"
    omp_threads = str(omp_num_threads) if omp_num_threads else "4"

    txt = f"""#!/bin/bash
#SBATCH -A {account}
#SBATCH -C {constraint}
#SBATCH -q {queue}
#SBATCH -t {time_limit}
#SBATCH --cpus-per-task={cpus_per_task}
#SBATCH -N {nodes}
#SBATCH --ntasks-per-node={ntasks_per_node}
#SBATCH --mail-type=BEGIN,END
{f"#SBATCH --mail-user={mail_user}" if mail_user else ""}
#SBATCH --signal=B:TERM@60

batch="$0"
DB="{db_file_abs}"
EID={ensemble_id}
mode="{mode}"
ens="{ens_name}"
ens_rel="{ens_relpath}"
VOL="{VOL}"
EXEC="{exec_path}"
BIND="{bind_script}"
n_trajec={n_trajec}
mpi="{mpi_val}"
cacheblocking="{cb_val}"
trajL="{trajL}"
lvl_sizes="{lvl_sizes}"
work_root="{str(work_root)}"

cd "$work_root"
LOGFILE="/global/cfs/cdirs/m2986/cosmon/mdwf/mdwf_update.log"

echo "ens = $ens"
echo "ens_dir = {ensemble_dir}"
echo "work_root = $work_root"
echo "EXEC = $EXEC"
echo "BIND = $BIND"
echo "n_trajec = $n_trajec"

mkdir -p cnfg
mkdir -p log_hmc
mkdir -p out
module load conda
conda activate /global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf

  # Ensure SQLite uses a network-FS friendly journal mode to avoid 'disk I/O error'
  export MDWF_DB_JOURNAL=${{MDWF_DB_JOURNAL:-DELETE}}

start=`ls -v cnfg/| grep lat | tail -1 | sed 's/[^0-9]*//g'`
if [[ -z $start ]]; then
    echo "no configs - start is empty - doing TepidStart"
    start=0
fi

# No cfg_max threshold; controlled by Trajectories in XML

echo "cfg_current = $start"

# Prepare DB update variables and queue RUNNING update off-node
USER=$(whoami)
OP="HMC_$mode"
SC=$start
EC=$(( start + n_trajec ))
# No increment tracking for HMC; EC reflects StartTrajectory + Trajectories
RUN_DIR="$work_root"
source <(python -m MDWFutils.jobs.slurm_update_trap)

SECONDS=0

cd cnfg

export I_MPI_PIN=1
export SLURM_CPU_BIND="cores"
export OMP_NUM_THREADS={omp_threads}

echo "Nthreads $OMP_NUM_THREADS"

echo "START `date`"
srun $BIND $EXEC --mpi $mpi --grid $VOL --dslash-unroll --cacheblocking $cacheblocking > ../log_hmc/log_{ens_name}.$start
echo "STOP `date`"

echo "All done in $SECONDS seconds"
"""
    script_file.write_text(txt, encoding='utf-8')
    return True
