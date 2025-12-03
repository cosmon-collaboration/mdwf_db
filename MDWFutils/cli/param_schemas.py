"""Parameter schema definitions used by CLI commands.

ParamDef is kept for backward compatibility with legacy code,
but new code should use ContextParam from jobs.schema.
All job schemas are now defined in context builders.

The schemas below are ONLY used by input-file-generation commands
(hmc-xml, glu-input, wit-input) which have job_type=None and input_type="...".
These will be migrated to input builder classes in a future refactor.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional


@dataclass
class ParamDef:
    """Declarative parameter definition (legacy)."""

    name: str
    type: type
    required: bool = False
    default: Optional[Any] = None
    help: str = ""
    choices: Optional[List[Any]] = None


# Input schemas for input-file-generation commands only
# (Not used by job script commands - those get schemas from context builders)
HMC_INPUT_SCHEMA = [
    ParamDef("Trajectories", int, required=True, help="Number of trajectories"),
    ParamDef("trajL", float, required=True, help="Trajectory length"),
]

SMEAR_INPUT_SCHEMA = [
    ParamDef(
        "SMEARTYPE",
        str,
        required=False,
        choices=["STOUT", "APE", "HYP"],
        help="Smearing algorithm",
    ),
    ParamDef("SMITERS", int, required=False, help="Smearing iterations"),
    ParamDef("ALPHA1", float, help="Alpha1 parameter"),
    ParamDef("ALPHA2", float, help="Alpha2 parameter"),
    ParamDef("ALPHA3", float, help="Alpha3 parameter"),
]

WIT_INPUT_SCHEMA = [
    ParamDef("Configurations.first", int, required=True, help="First config"),
    ParamDef("Configurations.last", int, required=True, help="Last config"),
    ParamDef("Configurations.step", int, help="Step size"),
    ParamDef("Run_name.name", str, help="Run label"),
    ParamDef("Directories.cnfg_dir", str, help="CNFG directory"),
]


