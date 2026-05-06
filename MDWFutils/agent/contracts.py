"""Typed contracts for simple-model agent operation."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class RiskLevel(str, Enum):
    """Risk classes used by the agent runner before executing a tool."""

    READ = "read"
    WRITES_DB = "writes_db"
    WRITES_FILES = "writes_files"
    SUBMITS_JOBS = "submits_jobs"
    DESTRUCTIVE = "destructive"


class ToolSpec(BaseModel):
    """Machine-readable description of one safe tool surface."""

    name: str
    description: str
    input_schema: Dict[str, Any] = Field(default_factory=dict)
    output_schema: Dict[str, Any] = Field(default_factory=dict)
    risk: RiskLevel = RiskLevel.READ
    requires_confirmation: bool = False
    dry_run_supported: bool = False
    verify_tool: Optional[str] = None
    fixes: str = ""


class ToolResult(BaseModel):
    """Stable result envelope for agent tools."""

    ok: bool
    status: str
    summary: str
    data: Dict[str, Any] = Field(default_factory=dict)
    warnings: List[str] = Field(default_factory=list)
    effects: List[Dict[str, Any]] = Field(default_factory=list)
    next_actions: List[Dict[str, Any]] = Field(default_factory=list)


class ActionStep(BaseModel):
    """One deterministic action inside a plan."""

    id: str
    tool: str
    args: Dict[str, Any] = Field(default_factory=dict)
    risk: RiskLevel = RiskLevel.READ
    preconditions: List[str] = Field(default_factory=list)
    expected_effects: List[str] = Field(default_factory=list)
    requires_confirmation: bool = False
    verify_with: Optional[str] = None


class ActionPlan(BaseModel):
    """A workflow expanded into exact tool calls."""

    plan_id: str
    workflow: str
    target: Dict[str, Any] = Field(default_factory=dict)
    steps: List[ActionStep] = Field(default_factory=list)
    created_by: str = "mdwf_agent"
    created_at: datetime = Field(default_factory=datetime.utcnow)
    assumptions: List[str] = Field(default_factory=list)
