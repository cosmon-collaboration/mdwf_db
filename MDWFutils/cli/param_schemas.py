"""Parameter schema definitions used by CLI commands."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional


@dataclass
class ParamDef:
    """Declarative parameter definition."""

    name: str
    type: type
    required: bool = False
    default: Optional[Any] = None
    help: str = ""
    choices: Optional[List[Any]] = None


COMMON_JOB_SCHEMA = [
    ParamDef("config_start", int, required=False, help="First configuration"),
    ParamDef("config_end", int, required=False, help="Last configuration"),
    ParamDef("config_inc", int, default=1, help="Configuration increment"),
    ParamDef("time_limit", str, default="01:00:00", help="SLURM time limit"),
    ParamDef("nodes", int, default=1, help="Number of nodes"),
    ParamDef("account", str, default="m0000", help="SLURM account"),
    ParamDef("queue", str, default="regular", help="SLURM queue/partition"),
    ParamDef("constraint", str, default="gpu", help="Node constraint"),
    ParamDef("gpus", int, default=4, help="GPUs per node"),
    ParamDef("gpu_bind", str, default="none", help="GPU binding policy"),
    ParamDef("cpus_per_task", int, default=32, help="CPUs per task"),
    ParamDef("ranks", int, default=4, help="MPI ranks"),
    ParamDef("bind_sh", str, default="bind.sh", help="CPU binding script"),
    ParamDef("mail_user", str, default="", help="User email for notifications"),
]

HMC_INPUT_SCHEMA = [
    ParamDef("Trajectories", int, required=True, help="Number of trajectories"),
    ParamDef("trajL", float, required=True, help="Trajectory length"),
]

SMEAR_INPUT_SCHEMA = [
    ParamDef(
        "SMEARTYPE",
        str,
        required=True,
        default="STOUT",
        choices=["STOUT", "APE", "HYP"],
        help="Smearing algorithm",
    ),
    ParamDef("SMITERS", int, required=True, default=8, help="Smearing iterations"),
    ParamDef("ALPHA1", float, default=0.75, help="Alpha1 parameter"),
    ParamDef("ALPHA2", float, default=0.4, help="Alpha2 parameter"),
    ParamDef("ALPHA3", float, default=0.2, help="Alpha3 parameter"),
]

WIT_INPUT_SCHEMA = [
    ParamDef("Configurations.first", int, required=True, help="First config"),
    ParamDef("Configurations.last", int, required=True, help="Last config"),
    ParamDef("Configurations.step", int, default=1, help="Step size"),
    ParamDef("Run_name.name", str, default="u_stout8", help="Run label"),
    ParamDef("Directories.cnfg_dir", str, default="../cnfg/", help="CNFG directory"),
]


