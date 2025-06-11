import os
from pathlib import Path
from MDWFutils.db            import get_ensemble_details
from MDWFutils.jobs.glu      import generate_glu_input

def generate_smear_sbatch(
    *,
    # output SBATCH script path; if None we auto-name
    output_file: str      = None,
    db_file: str,
    ensemble_id: int,
    ensemble_dir: str,
    glu_path: str         = '/global/cfs/cdirs/m2986/cosmon/mdwf/software/install/GLU_ICC/bin/GLU',
    # SBATCH arguments
    account: str       = 'm2986',
    constraint: str    = 'cpu',
    queue: str         = 'regular',
    time_limit: str    = '0:20:00',
    job_name: str      = 'glu_smear',
    nodes: int         = 1,
    cpus_per_task: int = 256,
    gpus: int          = 4,
    gpu_bind: str      = 'none',
    ranks: int         = 4,
    bind_sh: str       = 'bind.sh',
    mail_user: str     = None,
    cfg_max: int       = None,
    # smearing‐run arguments (must supply config_start/end)
    config_start: int,
    config_end:   int,
    config_prefix: str = 'ckpoint_lat.',
    output_prefix: str = 'u_stout',
    SMEARTYPE:     str = 'STOUT',
    SMITERS:       int = 8,
    alpha_values:  list = None,
    config_inc:    int = 4,
    nsim:          int = 8,

    # any extra overrides for generate_glu_input
    custom_changes: dict = None
) -> str:
    """
    read L,T from DB
    write glu_smear.in under CFGS_<SMEARTYPE><SMITERS>/
    write SBATCH script under slurm/
    """
    ensemble_dir = Path(ensemble_dir).expanduser().resolve()
    db_file      = str(Path(db_file).expanduser().resolve())

    # fetch L,T
    ens = get_ensemble_details(db_file, ensemble_id)
    if not ens:
        raise RuntimeError(f"Ensemble {ensemble_id} not found")
    p = ens['parameters']
    L = int(p['L']); T = int(p['T'])

    # build GLU input
    smear_dir = ensemble_dir / f"cnfg_{SMEARTYPE}{SMITERS}"
    smear_dir.mkdir(parents=True, exist_ok=True)
    inp = smear_dir / "glu_smear.in"

    glu_overrides = {
        'config_number': config_start,
        'lattice_dims':  (L, L, L, T),
        'MODE':          'SMEARING',
        'SMEARTYPE':     SMEARTYPE,
        'SMITERS':       SMITERS,
        'alpha_values':  alpha_values or [],
    }
    if custom_changes:
        glu_overrides.update(custom_changes)

    generate_glu_input(str(inp), glu_overrides)

    # build SBATCH
    sbatch_dir = ensemble_dir / "slurm"
    sbatch_dir.mkdir(parents=True, exist_ok=True)

    if not output_file:
        fname = f"glu_smear_{SMEARTYPE}{SMITERS}_{config_start}_{config_end}.sh"
        output_file = str(sbatch_dir / fname)

    alpha_str = "[" + ",".join(map(str, alpha_values or [])) + "]"
    txt = f"""#!/usr/bin/env bash
#SBATCH -A {account}
#SBATCH -C {constraint}
#SBATCH -q {queue}
#SBATCH -t {time_limit}
#SBATCH -J {job_name}
#SBATCH --output={ensemble_dir}/jlog/%j.out
#SBATCH --error={ensemble_dir}/jlog/%j.err
#SBATCH -N {nodes}
#SBATCH --cpus-per-task={cpus_per_task}
#SBATCH --signal=B:TERM@60

module load cpu
module load intel-mixed/2023.2.0
module load cray-fftw/3.3.10.8

DB="{db_file}"
EID={ensemble_id}
OP="GLU_SMEAR"
SC={config_start}
EC={config_end}

mkdir -p "{ensemble_dir}/jlog" "{smear_dir}"

mdwf_db update \\
  --db-file="$DB" \\
  --ensemble-id=$EID \\
  --operation-type="$OP" \\
  --status=RUNNING \\
  --params="config_start=$SC config_end=$EC config_increment={config_inc} slurm_job=$SLURM_JOBID"

update_status() {{
  code=$?
  status=COMPLETED
  (( code!=0 )) && status=FAILED
  mdwf_db update \\
    --db-file="$DB" \\
    --ensemble-id=$EID \\
    --operation-type="$OP" \\
    --status=$status \\
    --params="slurm_job=$SLURM_JOBID runtime=$SECONDS host=$(hostname)"
  exit $code
}}
trap update_status EXIT TERM INT HUP QUIT
SECONDS=0

GLU="{glu_path}"
step={config_inc}
nsim={nsim}
let 'Nth={cpus_per_task}/nsim'
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
        
        in_cfg="{ensemble_dir}/cnfg/{config_prefix}${{c}}"
        out_cfg="{smear_dir}/{output_prefix}{SMEARTYPE}{SMITERS}_n${{c}}"
        "$GLU" -i "{inp}" -c "$in_cfg" -o "$out_cfg" &
    done
    wait
done

echo "Done in $SECONDS s"
"""
    with open(output_file,'w') as f: f.write(txt)
    os.chmod(output_file,0o755)
    return output_file

DEFAULT_GLU_PARAMS = {
    "Run name": {
        "name": "u_stout8"
    },
    "Directories": {
        "cnfg_dir": "../cnfg_stout8/"
    },
    "Configurations": {
        "first": "CFGNO",
        "last": "CFGNO",
        "step": "4"
    },
    "Random number generator": {
        "level": "0",
        "seed": "3993"
    },
    "Lattice parameters": {
        "Ls": "10",
        "M5": "1.0",
        "b": "1.75",
        "c": "0.75"
    },
    "Boundary conditions": {
        "type": "APeri"
    },
    "Witness": {
        "no_prop": "3",
        "no_solver": "2"
    },
    "Solver 0": {
        "solver": "CG",
        "nkv": "24",
        "isolv": "1",
        "nmr": "3",
        "ncy": "3",
        "nmx": "8000",
        "exact_deflation": "true"
    },
    "Solver 1": {
        "solver": "CG",
        "nkv": "24",
        "isolv": "1",
        "nmr": "3",
        "ncy": "3",
        "nmx": "8000",
        "exact_deflation": "false"
    },
    "Exact Deflation": {
        "Cheby_fine": "0.01,-1,24",
        "Cheby_smooth": "0,0,0",
        "Cheby_coarse": "0,0,0",
        "kappa": "0.125",
        "res": "1E-5",
        "nmx": "64",
        "Ns": "64"
    },
    "Propagator 0": {
        "Noise": "Z2xZ2",
        "Source": "Wall",
        "Dilution": "PS",
        "pos": "0 0 0 -1",
        "mom": "0 0 0 0",
        "twist": "0 0 0",
        "kappa": "KAPPA_L",
        "mu": "0.",
        "Seed": "12345",
        "idx_solver": "0",
        "res": "1E-12",
        "sloppy_res": "1E-4"
    },
    "Propagator 1": {
        "Noise": "Z2xZ2",
        "Source": "Wall",
        "Dilution": "PS",
        "pos": "0 0 0 -1",
        "mom": "0 0 0 0",
        "twist": "0 0 0",
        "kappa": "KAPPA_S",
        "mu": "0.",
        "Seed": "12345",
        "idx_solver": "1",
        "res": "1E-12",
        "sloppy_res": "1E-6"
    },
    "Propagator 2": {
        "Noise": "Z2xZ2",
        "Source": "Wall",
        "Dilution": "PS",
        "pos": "0 0 0 -1",
        "mom": "0 0 0 0",
        "twist": "0 0 0",
        "kappa": "KAPPA_C",
        "mu": "0.",
        "Seed": "12345",
        "idx_solver": "1",
        "res": "5E-15",
        "sloppy_res": "5E-15"
    },
    "AMA": {
        "NEXACT": "2",
        "SLOPPY_PREC": "1E-5",
        "NHITS": "1",
        "NT": "48"
    }
}