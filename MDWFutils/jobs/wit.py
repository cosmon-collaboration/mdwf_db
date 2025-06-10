import os
import copy
from pathlib import Path

DEFAULT_WIT_PARAMS = {
    "Run name": {
        "name": "u_stout8"
    },
    "Directories": {
        "cnfg_dir": "../cfgs_stout/"
    },
    "Configurations": {
        "first": "444",
        "last":  "444",
        "step":  "4"
    },
    "Random number generator": {
        "level": "0",
        "seed":  "3993"
    },
    "Lattice parameters": {
        "Ls": "10",
        "M5": "1.0",
        "b":  "1.75",
        "c":  "0.75"
    },
    "Boundary conditions": {
        "type": "APeri"
    },
    "Witness": {
        "no_prop":   "3",
        "no_solver": "2"
    },
    "Solver 0": {
        "solver": "CG", "nkv": "24", "isolv": "1",
        "nmr":    "3",  "ncy": "3",  "nmx":   "8000", "exact_deflation": "true"
    },
    "Solver 1": {
        "solver": "CG", "nkv": "24", "isolv": "1",
        "nmr":    "3",  "ncy": "3",  "nmx":   "8000", "exact_deflation": "false"
    },
    "Exact Deflation": {"Cheby_fine": "0.01,-1,24", "Cheby_smooth": "0,0,0", 
                        "Cheby_coarse": "0,0,0", "kappa": "0.125", "res": "1E-5","rmx":"1E-5", 
                        "nmx":"64", "ns":"64"},
    ###Add propagators
    #compute kappa from m K = 1/(2*(m+4))
    #Seed should be chosen randomly, same for all propagators, but diff for each config
    "AMA": {
        "NEXACT":      "1",
        "SLOPPY_PREC": "1E-5",
        "NHITS":       "1",
        "NT":          "32"
    }
}


def update_nested_dict(d, updates):
    """
    Recursively merge updates into d.
    """
    for key, val in updates.items():
        if isinstance(val, dict) and key in d and isinstance(d[key], dict):
            update_nested_dict(d[key], val)
        else:
            d[key] = val
    return d


def generate_wit_input(
    output_file,
    custom_changes=None
):
    """
    Write a WIT .ini‐style input file.
      - output_file: path to write DWF.in
      - custom_changes: nested dict { section: { key: value, … }, … }
    """
    params = copy.deepcopy(DEFAULT_WIT_PARAMS)
    if custom_changes:
        update_nested_dict(params, custom_changes)

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


def generate_wit_sbatch(
    output_file,
    db_file,
    ensemble_id,
    ensemble_dir,
    account,
    constraint='gpu',
    gpus=4,
    nodes=1,
    cpus_per_task=16,
    time='04:00:00',
    qos='regular',
    ranks=4,
    bind_sh='bind.sh',
    exec_path='/path/to/Meson',
    sample_in='DWF.sample.in',
    first=444,
    last=444,
    step=4,
    ogeom=(1,1,1,4),
    lgeom=(16,16,16,8),
    no_prop=3,
    no_solver=1
):
    """
    Create a SBATCH script under ensemble_dir/meson2pt/.
    """
    ensemble_dir = os.path.abspath(ensemble_dir)
    db_file      = os.path.abspath(db_file)

    workdir = Path(ensemble_dir) / "meson2pt"
    workdir.mkdir(parents=True, exist_ok=True)

    if not output_file:
        output_file = str(workdir / f"meson2pt_{first}_{last}.sh")

    ogeom_str = " ".join(map(str,ogeom))
    lgeom_str = " ".join(map(str,lgeom))

    # pack all WIT‐specific params into one --params string
    params_str = (
        f"first={first} last={last} step={step} "
        f"no_prop={no_prop} no_solver={no_solver} "
        f"ogeom='{ogeom_str}' lgeom='{lgeom_str}'"
    )

    script = f"""#!/usr/bin/env bash
#SBATCH -A {account}
#SBATCH --nodes={nodes}
#SBATCH -C {constraint}
#SBATCH --gpus={gpus}
#SBATCH --cpus-per-task={cpus_per_task}
#SBATCH --time={time}
#SBATCH --qos={qos}
#SBATCH -o {workdir}/jlog/%j.log

set -euo pipefail
cd {workdir}

#record RUNNING (one shot for the entire meson2pt job)
mdwf_db.py update \\
  --db-file="{db_file}" \\
  --ensemble-id={ensemble_id} \\
  --operation-type="WIT_MESON2PT" \\
  --status="RUNNING" \\
  --params="{params_str}"

# On exit/failure, update status + code + runtime
update_status() {{
  local EC=$?
  local ST="COMPLETED"
  [[ $EC -ne 0 ]] && ST="FAILED"

  mdwf_db.py update \\
    --db-file="{db_file}" \\
    --ensemble-id={ensemble_id} \\
    --operation-type="WIT_MESON2PT" \\
    --status="$ST" \\
    --exit-code=$EC \\
    --runtime=$SECONDS \\
    --params="slurm_job=$SLURM_JOB_ID host=$(hostname)"

  echo "Meson2pt job $ST ($EC)"
}}
trap update_status EXIT TERM INT HUP QUIT

SECONDS=0

# load environment
source /path/to/env_gpu.sh
export LD_LIBRARY_PATH=/path/to/quda/lib:$LD_LIBRARY_PATH
export MPICH_GPU_SUPPORT_ENABLED=1
export MPICH_RDMA_ENABLED_CUDA=1
export MPICH_NEMESIS_ASYNC_PROGRESS=1
export OMP_NUM_THREADS=$cpus_per_task
export SLURM_CPU_BIND=cores
export CRAY_ACCEL_TARGET=nvidia80

# loop over cfg numbers
for cfg in $(seq {first} {step} {last}); do
  seed=$(python rng_seed.py $cfg)
  if [[ ! -e DATA/Meson_2pt_00u_stout8n${{cfg}}.bin ]]; then
    cp {sample_in} DWF.in
    sed -i "s/^first.*/first        {first}/"  DWF.in
    sed -i "s/^last.*/last         {last}/"   DWF.in
    sed -i "s/^step.*/step         {step}/"   DWF.in
    sed -i "s/^Seed.*/Seed         $seed/"    DWF.in
    sed -i "s/CFGNO/$cfg/"                   DWF.in
    echo "running cfg $cfg with seed $seed"
    srun -n {ranks} {bind_sh} {exec_path} \\
         -i DWF.in -ogeom {ogeom_str} -lgeom {lgeom_str}
  fi
done

echo "All done in $SECONDS seconds"
"""

    Path(output_file).write_text(script)
    os.chmod(output_file, 0o755)
    print(f"Generated WIT SBATCH script: {output_file}")
    return str(output_file)