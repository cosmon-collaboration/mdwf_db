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
    glu_path: str,
    # SBATCH arguments
    account: str       = 'myaccount',
    constraint: str    = 'cpu',
    queue: str         = 'regular',
    time_limit: str    = '0:20:00',
    job_name: str      = 'glu_smear',
    nodes: int         = 1,
    cpus_per_task: int = 1,
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
#SBATCH --error ={ensemble_dir}/jlog/%j.err
#SBATCH -N {nodes}
#SBATCH --cpus-per-task={cpus_per_task}
#SBATCH --signal=B:TERM@60

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
export OMP_NUM_THREADS=$(( {cpus_per_task} / {nsim} ))
cd "{smear_dir}"

echo "Smearing configs $SC..$EC → {SMEARTYPE}{SMITERS}"
for (( c=$SC; c<$EC; c+= {config_inc}*{nsim} )); do
  for (( i=0; i< {nsim}; i++ )); do
    idx=$((c + {config_inc}*i))
    (( idx>=EC )) && break
    in_cfg="{ensemble_dir}/cnfg/{config_prefix}${{idx}}"
    out_cfg="{smear_dir}/{output_prefix}{SMEARTYPE}{SMITERS}_n${{idx}}"
    "$GLU" -i "{inp}" -c "$in_cfg" -o "$out_cfg" &
  done
  wait
done

echo "Done in $SECONDS s"
"""
    with open(output_file,'w') as f: f.write(txt)
    os.chmod(output_file,0o755)
    return output_file