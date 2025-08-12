import os
import copy
from pathlib import Path
from MDWFutils.db import get_ensemble_details

# Defaults for WIT meson2pt
DEFAULT_WIT_PARAMS = {
    "Run name": {"name": "ck"},
    "Directories": {"cnfg_dir": "../cnfg_STOUT8/"},
    "Configurations": {"first": "CFGNO", "last": "CFGNO", "step": "4"},
    "Random number generator": {"level": "0", "seed": "3993"},
    "Lattice parameters": {"Ls": "10", "M5": "1.0", "b": "1.75", "c": "0.75"},
    "Boundary conditions": {"type": "APeri"},
    "Witness": {"no_prop": "3", "no_solver": "2"},
    "Solver 0": {"solver": "CG", "nkv": "24", "isolv": "1", "nmr": "3", "ncy": "3", "nmx": "8000", "exact_deflation": "true"},
    "Solver 1": {"solver": "CG", "nkv": "24", "isolv": "1", "nmr": "3", "ncy": "3", "nmx": "8000", "exact_deflation": "false"},
    "Exact Deflation": {"Cheby_fine": "0.01,-1,24", "Cheby_smooth": "0,0,0", "Cheby_coarse": "0,0,0", "kappa": "0.125", "res": "1E-5", "nmx": "64", "Ns": "64"},
    "Propagator 0": {"Noise": "Z2xZ2", "Source": "Wall", "Dilution": "PS", "pos": "0 0 0 -1", "mom": "0 0 0 0", "twist": "0 0 0", "kappa": "KAPPA_L", "mu": "0.", "Seed": "12345", "idx_solver": "0", "res": "1E-12", "sloppy_res": "1E-4"},
    "Propagator 1": {"Noise": "Z2xZ2", "Source": "Wall", "Dilution": "PS", "pos": "0 0 0 -1", "mom": "0 0 0 0", "twist": "0 0 0", "kappa": "KAPPA_S", "mu": "0.", "Seed": "12345", "idx_solver": "1", "res": "1E-12", "sloppy_res": "1E-6"},
    "Propagator 2": {"Noise": "Z2xZ2", "Source": "Wall", "Dilution": "PS", "pos": "0 0 0 -1", "mom": "0 0 0 0", "twist": "0 0 0", "kappa": "KAPPA_C", "mu": "0.", "Seed": "12345", "idx_solver": "1", "res": "5E-15", "sloppy_res": "5E-15"},
    "AMA": {"NEXACT": "2", "SLOPPY_PREC": "1E-5", "NHITS": "1", "NT": "48"}
}

DEFAULT_WIT_ENV = 'source /global/cfs/cdirs/m2986/cosmon/mdwf/software/scripts/env_gpu.sh'
DEFAULT_WIT_BIND = '/global/cfs/cdirs/m2986/cosmon/mdwf/ANALYSIS/WIT/bind.sh'
DEFAULT_WIT_EXEC = '/global/cfs/cdirs/m2986/cosmon/mdwf/software/install_gpu/wit/bin/Meson'


def update_nested_dict(d, updates):
    for key, val in updates.items():
        if isinstance(val, dict) and key in d and isinstance(d[key], dict):
            update_nested_dict(d[key], val)
        else:
            d[key] = val
    return d


def generate_wit_input(
    output_file,
    custom_changes=None,
    *,
    ensemble_params=None,
    custom_params=None
):
    """
    Generate a WIT .ini-style input file (DWF.in).
    Backward compatible with 'custom_changes'; prefer 'custom_params'.
    """
    overrides = custom_params if custom_params is not None else (custom_changes or {})
    params = copy.deepcopy(DEFAULT_WIT_PARAMS)

    # auto-fill from ensemble parameters when available
    if ensemble_params:
        ep = ensemble_params
        lat_updates = {}
        for key in ('Ls', 'b', 'c', 'M5'):
            if key in ep:
                lat_updates[key] = str(ep[key])
        if lat_updates:
            update_nested_dict(params.setdefault('Lattice parameters', {}), lat_updates)

        try:
            for mass_key, prop_section in (('ml', 'Propagator 0'), ('ms', 'Propagator 1'), ('mc', 'Propagator 2')):
                if mass_key in ep:
                    m = float(ep[mass_key])
                    kappa = 1.0 / (2.0 * m + 8.0)
                    update_nested_dict(params.setdefault(prop_section, {}), {'kappa': str(kappa)})
        except Exception:
            pass

    if overrides:
        update_nested_dict(params, overrides)

    outf = Path(output_file)
    outf.parent.mkdir(parents=True, exist_ok=True)
    with open(outf, 'w') as f:
        for section, block in params.items():
            f.write(f"[{section}]\n")
            for key, val in block.items():
                f.write(f"{key:<12} {val}\n")
            f.write("\n")
    print(f"Generated WIT input file: {outf}")
    return str(outf)


def generate_meson2pt_sbatch(
    *,
    output_file=None,
    db_file=None,
    ensemble_id=None,
    ensemble_dir=None,
    custom_changes=None,
    wit_exec_path=None,
    bind_script=None,
    account='m2986_g',
    constraint='gpu',
    queue='regular',
    time_limit='06:00:00',
    nodes=1,
    gpus=4,
    gpu_bind='none',
    mail_user=None,
    ranks=4,
    ogeom=None,
):
    """
    Create a meson2pt SBATCH script under ensemble_dir/meson2pt/.
    Mirrors smear-script style inputs.
    """
    if mail_user is None:
        raise ValueError("mail_user is required")

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

    try:
        ml = float(p['ml']); ms = float(p['ms']); mc = float(p['mc'])
        if ml <= 0 or ms <= 0 or mc <= 0:
            raise ValueError
    except Exception:
        raise RuntimeError("Failed to get quark masses (ml, ms, mc) from ensemble parameters")

    kappaL = 1 / (2 * ml + 8)
    kappaS = 1 / (2 * ms + 8)
    kappaC = 1 / (2 * mc + 8)

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

    workdir = Path(ensemble_dir) / "meson2pt"
    workdir.mkdir(parents=True, exist_ok=True)
    (workdir / "jlog").mkdir(parents=True, exist_ok=True)
    # Build SBATCH output path under slurm/
    sbatch_dir = Path(ensemble_dir) / "slurm"
    sbatch_dir.mkdir(parents=True, exist_ok=True)

    wit_input_file = workdir / "DWF.in"
    wit_params = {
        'Propagator 0': {'kappa': str(kappaL)},
        'Propagator 1': {'kappa': str(kappaS)},
        'Propagator 2': {'kappa': str(kappaC)}
    }
    if custom_changes:
        update_nested_dict(wit_params, custom_changes)
    generate_wit_input(str(wit_input_file), custom_params=wit_params, ensemble_params=p)

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
        output_file = str(sbatch_dir / f"meson2pt_{config_start}_{config_end}.sh")

    params_str = f"config_start={config_start} config_end={config_end} config_increment={config_inc}"

    script = f"""#!/bin/bash
#SBATCH -A {account}
#SBATCH --nodes={nodes}
#SBATCH -C {constraint}
#SBATCH --gpus={gpus}
#SBATCH --time={time_limit}
#SBATCH --qos={queue}
#SBATCH --mail-user={mail_user}
#SBATCH --mail-type=ALL
#SBATCH -o {workdir}/jlog/%J.log

cd {workdir}

module load conda
conda activate /global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf

# Prepare DB update variables and queue RUNNING update off-node
DB="{db_file}"
EID={ensemble_id}
OP="WIT_MESON2PT"
SC={config_start}
EC={config_end}
IC={config_inc}
USER=$(whoami)
LOGFILE="/global/cfs/cdirs/m2986/cosmon/mdwf/mdwf_update.log"
echo "mdwf_db update --db-file=$DB --ensemble-id=$EID --operation-type=$OP --status=RUNNING --user=$USER --params=\"config_start=$SC config_end=$EC config_increment=$IC slurm_job=$SLURM_JOB_ID\"" >> "$LOGFILE"

# On exit/failure, update status + code + runtime
update_status() {{
  local EC=$?
  local ST="COMPLETED"
  [[ $EC -ne 0 ]] && ST="FAILED"

  echo "mdwf_db update --db-file=$DB --ensemble-id=$EID --operation-type=$OP --status=$ST --user=$USER --params=\"exit_code=$EC runtime=$SECONDS slurm_job=$SLURM_JOB_ID host=$(hostname)\"" >> "$LOGFILE"

  echo "Meson2pt job $ST ($EC)"
}}
trap update_status EXIT TERM INT HUP QUIT

SECONDS=0

{env_setup}
export LD_LIBRARY_PATH=/global/cfs/cdirs/m2986/cosmon/mdwf/software/install_gpu/quda/lib:$LD_LIBRARY_PATH

### MPI flags
export MPICH_RDMA_ENABLED_CUDA=1
export MPICH_GPU_SUPPORT_ENABLED=1
export MPICH_NEMESIS_ASYNC_PROGRESS=1

### Cray/Slurm flags
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
echo "MPICH_OFI_NIC_POLICY=${{MPICH_OFI_NIC_POLICY}}"
echo "MPICH_OFI_NIC_MAPPING=${{MPICH_OFI_NIC_MAPPING}}"

EXEC="{exec_path}"
BIND="{bind_sh}"
echo "Running meson2pt range {config_start}-{config_end} step {config_inc}"
srun -n {ranks} $BIND $EXEC -i DWF.in -ogeom {ogeom[0]} {ogeom[1]} {ogeom[2]} {ogeom[3]} -lgeom {lgeom[0]} {lgeom[1]} {lgeom[2]} {lgeom[3]}

echo "All done in $SECONDS seconds"
"""

    Path(output_file).write_text(script)
    os.chmod(output_file, 0o755)
    print(f"Generated WIT SBATCH script: {output_file}")
    return str(output_file)


__all__ = [
    'generate_wit_input',
    'generate_meson2pt_sbatch',
]


