import os
from pathlib import Path
from MDWFutils.db            import get_ensemble_details
from MDWFutils.jobs.glu      import generate_glu_input
from MDWFutils.jobs.slurm_update_trap import get_slurm_update_trap_inline

def generate_wflow_sbatch(
    *,
    # output SBATCH script path; if None we auto-name
    output_file: str      = None,
    db_file: str,
    ensemble_id: int,
    ensemble_dir: str,
    run_dir: str = None,
    glu_path: str         = '/global/cfs/cdirs/m2986/cosmon/mdwf/software/install/GLU_ICC/bin/GLU',
    # SBATCH arguments
    account: str       = 'm2986',
    constraint: str    = 'cpu',
    queue: str         = 'regular',
    time_limit: str    = '01:00:00',
    job_name: str      = 'wflow',
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
    config_prefix: str = 'ckpoint_EODWF_lat.',
    output_prefix: str = 't0',
    SMEARTYPE:     str = 'ADAPTWFLOW_STOUT',
    SMITERS:       int = 250,
    alpha_values:  list = (0.02, 0.01, 0.005),
    config_inc:    int = 4,
    nsim:          int = 4,

    # any extra overrides for generate_glu_input
    custom_changes: dict = None
) -> str:
    """
    read L,T from DB
    write glu_smear.in under t0/
    write SBATCH script under slurm/
    """
    ensemble_dir = Path(ensemble_dir).expanduser().resolve()
    base_work    = Path(run_dir).expanduser().resolve() if run_dir else ensemble_dir
    db_file      = str(Path(db_file).expanduser().resolve())

    # fetch L,T
    ens = get_ensemble_details(db_file, ensemble_id)
    if not ens:
        raise RuntimeError(f"Ensemble {ensemble_id} not found")
    p = ens['parameters']
    L = int(p['L']); T = int(p['T'])

    # build GLU input
    t0_dir = base_work / f"t0"
    t0_dir.mkdir(parents=True, exist_ok=True)
    (t0_dir / "jlog").mkdir(parents=True, exist_ok=True)
    inp = t0_dir / "glu_smear.in"

    glu_overrides = {
        'CONFNO': str(config_start),
        'DIM_0': str(L),
        'DIM_1': str(L), 
        'DIM_2': str(L),
        'DIM_3': str(T),
        'SMEARTYPE': "ADAPTWFLOW_STOUT",
        'SMITERS': str(SMITERS),
    }
    
    # Add alpha values if provided
    if alpha_values:
        if len(alpha_values) >= 1:
            glu_overrides['ALPHA1'] = str(alpha_values[0])
        if len(alpha_values) >= 2:
            glu_overrides['ALPHA2'] = str(alpha_values[1])
        if len(alpha_values) >= 3:
            glu_overrides['ALPHA3'] = str(alpha_values[2])
    if custom_changes:
        glu_overrides.update(custom_changes)

    generate_glu_input(str(inp), glu_overrides)

    # build SBATCH - save in the t0 directory's slurm subfolder
    sbatch_dir = t0_dir / "slurm"
    sbatch_dir.mkdir(parents=True, exist_ok=True)

    if not output_file:
        fname = f"GLU_{SMEARTYPE}{SMITERS}_{config_start}_{config_end}.sh"
        output_file = str(sbatch_dir / fname)

    alpha_str = "[" + ",".join(map(str, alpha_values or [])) + "]"

    txt = f"""#!/usr/bin/env bash
#SBATCH -A {account}
#SBATCH -C {constraint}
#SBATCH -q {queue}
#SBATCH -t {time_limit}
#SBATCH -J {job_name}
#SBATCH --output={t0_dir}/jlog/%j.out
#SBATCH --error={t0_dir}/jlog/%j.err
#SBATCH -N {nodes}
#SBATCH --cpus-per-task={cpus_per_task}
#SBATCH --signal=B:TERM@60
{f"#SBATCH --mail-user={mail_user}" if mail_user else ""}

module load cpu
module load intel-mixed/2023.2.0
module load cray-fftw/3.3.10.8
module load conda
conda activate /global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf

# On many network filesystems WAL journal mode causes SQLite disk I/O errors.
# Default to DELETE unless the user overrides MDWF_DB_JOURNAL in the environment.
export MDWF_DB_JOURNAL="${{MDWF_DB_JOURNAL:-DELETE}}"

DB="{db_file}"
EID={ensemble_id}
OP="GLU_WFLOW"
SC={config_start}
EC={config_end}
IC={config_inc}
USER=$(whoami)
LOGFILE="/global/cfs/cdirs/m2986/cosmon/mdwf/mdwf_update.log"
RUN_DIR="{str(base_work)}"

mkdir -p "{t0_dir}/jlog"

# Source logging helper via process substitution
source <(python -m MDWFutils.jobs.slurm_update_trap)
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
        (( c>EC )) && break
        
        # Calculate CPU binding for physical and logical cores
        let 'lo=i*Nth/2'
        let 'hi=lo+Nth/2-1'
        let 'loh=128+i*Nth/2'
        let 'hih=loh+Nth/2-1'
        
        echo "Config $c: CPUs $lo-$hi $loh-$hih"
        export GOMP_CPU_AFFINITY="${{lo}}-${{hi}} ${{loh}}-${{hih}}"
        
        in_cfg="{str(base_work)}/cnfg/{config_prefix}${{c}}"
        out_cfg="{t0_dir}/{output_prefix}.${{c}}.out"
        $GLU -i "{inp}" -c "$in_cfg" > "$out_cfg" &
    done
    wait
done

echo "Done in $SECONDS s"
"""
    with open(output_file,'w') as f: f.write(txt)
    os.chmod(output_file,0o775)
    return output_file