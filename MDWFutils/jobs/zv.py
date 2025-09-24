import os
import copy
from pathlib import Path
from MDWFutils.db import get_ensemble_details
from MDWFutils.jobs.wit import generate_wit_input, update_nested_dict
from MDWFutils.jobs.slurm_update_trap import get_slurm_update_trap_inline



DEFAULT_WIT_ENV = 'source /global/cfs/cdirs/m2986/cosmon/mdwf/software/scripts/env_gpu.sh'
DEFAULT_WIT_BIND = '/global/cfs/cdirs/m2986/cosmon/mdwf/ANALYSIS/WIT/bind.sh'
DEFAULT_WIT_EXEC = '/global/cfs/cdirs/m2986/cosmon/mdwf/software/install_gpu/wit/bin/FDiagonal_3pt'





def generate_zv_sbatch(
    *,
    output_file=None,
    db_file=None,
    ensemble_id=None,
    ensemble_dir=None,
    run_dir=None,
    custom_changes=None,
    wit_exec_path=None,
    bind_script=None,
    account='m2986_g',
    constraint='gpu',
    queue='regular',
    time_limit='00:10:00',
    nodes=1,
    gpus=4,
    gpu_bind='none',
    mail_user=None,
    ranks=4,
    ogeom=None,
):
    """
    Create a Zv SBATCH script under ensemble_dir/Zv/.
    Mirrors smear-script style inputs.
    """
    # mail_user optional

    ensemble_dir = os.path.abspath(ensemble_dir)
    db_file      = os.path.abspath(db_file)

    ens = get_ensemble_details(db_file, ensemble_id)
    if not ens:
        raise RuntimeError(f"Ensemble {ensemble_id} not found")
    p = ens['parameters']

    try:
        L = int(p['L']); T = int(p['T'])
        if L <= 0 or T <= 0:
            raise ValueError
    except Exception:
        raise RuntimeError("Failed to get lattice dimensions from ensemble parameters")

    # Get light quark mass from ensemble parameters (only need ml for Zv)
    try:
        ml_ens = float(p['ml'])
        if ml_ens <= 0:
            raise ValueError
    except Exception:
        raise RuntimeError("Failed to get light quark mass (ml) from ensemble parameters")

    kappaL = 1 / (2 * ml_ens + 8)

    # Parse/normalize ogeom
    if ogeom is None:
        ogeom_vals = [1, 1, 1, 4]
    else:
        if isinstance(ogeom, (list, tuple)):
            ogeom_vals = list(ogeom)
        else:
            s = str(ogeom).strip()
            if ',' in s:
                parts = s.split(',')
            else:
                parts = s.split()
            ogeom_vals = [int(x) for x in parts if str(x).strip()]
        if len(ogeom_vals) != 4 or any(v <= 0 for v in ogeom_vals):
            raise ValueError(f"ogeom must have 4 positive integers; got {ogeom_vals}")

    ogeom = ogeom_vals
    # Validate product(ogeom) == 4 * nodes
    prod_ogeom = 1
    for v in ogeom:
        prod_ogeom *= v
    if prod_ogeom != 4 * int(nodes):
        raise ValueError(
            f"Invalid ogeom {ogeom}: product={prod_ogeom} must equal 4*nodes={4*int(nodes)}"
        )

    # Validate divisibility and compute lgeom
    if (L % ogeom[0]) != 0 or (L % ogeom[1]) != 0 or (L % ogeom[2]) != 0 or (T % ogeom[3]) != 0:
        raise ValueError(
            f"ogeom {ogeom} does not evenly divide lattice {L}x{L}x{L}x{T}"
        )
    lgeom = [L // ogeom[0], L // ogeom[1], L // ogeom[2], T // ogeom[3]]
    # lgeom must be even
    if any(x % 2 != 0 for x in lgeom):
        raise ValueError(f"lgeom values must be even, got {lgeom}")

    env_setup = DEFAULT_WIT_ENV
    bind_sh   = bind_script or DEFAULT_WIT_BIND
    exec_path = wit_exec_path or DEFAULT_WIT_EXEC

    # Check for Ls and b overrides in custom_changes (WIT parameters)
    ls_override = None
    b_override = None
    if custom_changes:
        lattice_params = custom_changes.get('Lattice_parameters', {})
        ls_override = lattice_params.get('Ls')
        b_override = lattice_params.get('b')
    
    # Get ensemble Ls and b values for comparison
    ls_ens = p.get('Ls')
    b_ens = p.get('b')
    
    # Determine final values (override if provided, otherwise ensemble)
    ls_final = ls_override if ls_override is not None else ls_ens
    b_final = b_override if b_override is not None else b_ens
    
    # Create custom folder name if any parameters are overridden
    if ls_override is not None or b_override is not None:
        print("Warning: overriding ensemble parameters")
        folder_name = f"Zv_Ls{ls_final}_b{b_final}_ml{ml_ens}"
        print(f"Using custom output folder: {folder_name}")
    else:
        folder_name = "Zv"
    
    # Determine workdir: run in run_dir if provided, else under ensemble_dir
    base_work = Path(run_dir).resolve() if run_dir else Path(ensemble_dir)
    workdir = base_work / folder_name
    workdir.mkdir(parents=True, exist_ok=True)
    (workdir / "jlog").mkdir(parents=True, exist_ok=True)
    # Build SBATCH output path under slurm/
    sbatch_dir = Path(ensemble_dir) / "slurm"
    sbatch_dir.mkdir(parents=True, exist_ok=True)

    wit_input_file = workdir / "DWF_Zv.in"
    wit_params = {
        'Propagator_0': {'kappa': str(kappaL)},
        'Witness': {'no_prop': 1, 'no_solver': 1}  # Zv only needs 1 propagator and 1 solver
    }
    if custom_changes:
        update_nested_dict(wit_params, custom_changes)
    
    generate_wit_input(
        str(wit_input_file),
        custom_params=wit_params,
        ensemble_params=p,
        cli_format=True,
        prune_prop_solvers=(1, 1)
    )

    # Extract required config range from WIT parameters
    cfg_section = (custom_changes or {}).get('Configurations', {})
    try:
        config_start = int(cfg_section['first'])
        config_end   = int(cfg_section['last'])
        config_inc   = int(cfg_section.get('step', 4))
    except Exception:
        raise ValueError("WIT parameters must include Configurations.first, Configurations.last, and Configurations.step")

    # Default output filename if not provided
    if not output_file:
        output_file = str(sbatch_dir / f"Zv_{config_start}_{config_end}.sh")

    params_str = f"config_start={config_start} config_end={config_end} config_increment={config_inc}"

    script = f"""#!/bin/bash
#SBATCH -A {account}
#SBATCH --nodes={nodes}
#SBATCH -C {constraint}
#SBATCH --gpus={gpus}
#SBATCH --time={time_limit}
#SBATCH --qos={queue}
#SBATCH --mail-type=ALL
{f"#SBATCH --mail-user={mail_user}" if mail_user else ""}
#SBATCH -o {workdir}/jlog/%J.log

cd {workdir}

module load conda
conda activate /global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf

RUN_DIR="{str(base_work)}"
# Prepare DB update variables and queue RUNNING update off-node
DB="{db_file}"
EID={ensemble_id}
OP="WIT_Zv"
SC={config_start}
EC={config_end}
IC={config_inc}
USER=$(whoami)
LOGFILE="/global/cfs/cdirs/m2986/cosmon/mdwf/mdwf_update.log"

# Source logging helper with process substitution
source <(python -m MDWFutils.jobs.slurm_update_trap)

SECONDS=0

{env_setup}
export LD_LIBRARY_PATH=/global/cfs/cdirs/m2986/cosmon/mdwf/software/install_gpu/quda/lib:$LD_LIBRARY_PATH

ranks=4 # TBD: find how to determine this via SLURM environment variables

### MPI flags
export MPICH_RDMA_ENABLED_CUDA=1
export MPICH_GPU_SUPPORT_ENABLED=1
export MPICH_NEMESIS_ASYNC_PROGRESS=1

### Cray/Slurm flags
export OMP_NUM_THREADS=16
export SLURM_CPU_BIND=cores
export CRAY_ACCEL_TARGET=nvidia80

### QUDA specific flags
export QUDA_RESOURCE_PATH=`pwd`/../quda_resource
[[ -d $QUDA_RESOURCE_PATH ]] || mkdir -p $QUDA_RESOURCE_PATH
export QUDA_ENABLE_GDR=1

### MPICH debugging flags
export MPICH_VERSION_DISPLAY=1
export MPICH_OFI_NIC_VERBOSE=2
export MPICH_OFI_NIC_POLICY="USER"
export MPICH_OFI_NIC_MAPPING="0:3;1:2;2:1;3:0"
echo "MPICH_OFI_NIC_POLICY=${{MPICH_OFI_NIC_POLICY}}"
echo "MPICH_OFI_NIC_MAPPING=${{MPICH_OFI_NIC_MAPPING}}"

EXEC="{exec_path}"
BIND="{bind_sh}"
echo "Running Zv range {config_start}-{config_end} step {config_inc}"
srun -n {ranks} $BIND $EXEC -i {wit_input_file} -ogeom {ogeom[0]} {ogeom[1]} {ogeom[2]} {ogeom[3]} -lgeom {lgeom[0]} {lgeom[1]} {lgeom[2]} {lgeom[3]}

echo "All done in $SECONDS seconds"
"""

    Path(output_file).write_text(script)
    os.chmod(output_file, 0o755)
    print(f"Generated WIT SBATCH script: {output_file}")
    return str(output_file)


__all__ = [
    'generate_zv_sbatch',
]


