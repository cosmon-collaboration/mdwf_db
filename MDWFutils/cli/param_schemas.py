"""Parameter schema definitions used by CLI commands.

ParamDef is kept for backward compatibility with legacy code,
but new code should use ContextParam from jobs.schema.
All schemas are now defined in context builders.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional


@dataclass
class ParamDef:
    """Declarative parameter definition (legacy - use ContextParam for new code)."""

    name: str
    type: type
    required: bool = False
    default: Optional[Any] = None
    help: str = ""
    choices: Optional[List[Any]] = None


