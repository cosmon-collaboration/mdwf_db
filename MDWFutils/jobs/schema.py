"""Context parameter schema definitions for job builders."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional


@dataclass
class ContextParam:
    """Declarative parameter definition for context builders.
    
    Similar to ParamDef but used by context builders to define their own
    parameter schemas with defaults, types, and validation rules.
    """

    name: str
    type: type
    required: bool = False
    default: Optional[Any] = None
    help: str = ""
    choices: Optional[List[Any]] = None


def common_slurm_params() -> List[ContextParam]:
    """Helper for common SLURM parameters without defaults.
    
    Jobs can include these and override with job-specific defaults.
    """
    return [
        ContextParam("account", str, help="SLURM account"),
        ContextParam("queue", str, help="SLURM queue/partition"),
        ContextParam("time_limit", str, help="SLURM time limit"),
        ContextParam("nodes", int, help="Number of nodes"),
        ContextParam("cpus_per_task", int, help="CPUs per task"),
        ContextParam("ranks", int, help="MPI ranks"),
        ContextParam("mail_user", str, help="User email for notifications"),
        ContextParam("mail_type", str, help="Mail notification types"),
        ContextParam("job_name", str, help="Job name"),
    ]

