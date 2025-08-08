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
    time_limit: str    = '01:00:00',
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
    config_prefix: str = 'ckpoint_EODWF_lat.',
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
        'CONFNO': str(config_start),
        'DIM_0': str(L),
        'DIM_1': str(L), 
        'DIM_2': str(L),
        'DIM_3': str(T),
        'SMEARTYPE': SMEARTYPE,
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
module load conda
conda activate /global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf

# On many network filesystems WAL journal mode causes SQLite disk I/O errors.
# Default to DELETE unless the user overrides MDWF_DB_JOURNAL in the environment.
export MDWF_DB_JOURNAL="${{MDWF_DB_JOURNAL:-DELETE}}"

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
        export GOMP_CPU_AFFINITY="${{lo}}-${{hi}} ${{loh}}-${{hih}}"
        
        in_cfg="{ensemble_dir}/cnfg/{config_prefix}${{c}}"
        out_cfg="{smear_dir}/{output_prefix}{SMEARTYPE}{SMITERS}_n${{c}}"
        $GLU -i "{inp}" -c "$in_cfg" -o "$out_cfg" &
    done
    wait
done

echo "Done in $SECONDS s"
"""
    with open(output_file,'w') as f: f.write(txt)
    os.chmod(output_file,0o755)
    return output_file