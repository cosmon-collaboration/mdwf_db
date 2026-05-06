"""Agent tool registry built around deterministic MDWFutils operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict

from .contracts import RiskLevel, ToolResult, ToolSpec
from .tools import (
    command_metadata,
    list_analysis_runs,
    list_recipes,
    record_analysis_run,
    record_curation_event,
    remote_doctor,
    remote_monitor,
    remote_storage_plan,
    remote_submit,
    remote_sync_plan,
    upsert_recipe,
)


ToolHandler = Callable[[dict], ToolResult]


@dataclass(frozen=True)
class AgentTool:
    """Registered agent tool and its executable handler."""

    spec: ToolSpec
    handler: ToolHandler


def _remote_schema(properties=None, required=None) -> dict:
    schema = {
        "type": "object",
        "properties": {
            "host": {"type": "string", "default": "perlmutter"},
            "config": {"type": "string"},
            "timeout": {"type": "integer"},
            "dry_run_transport": {"type": "boolean", "default": False},
        },
    }
    schema["properties"].update(properties or {})
    if required:
        schema["required"] = required
    return schema


def get_tool_registry() -> Dict[str, AgentTool]:
    """Return all agent tools available through `mdwf_db agent call`."""
    tools = [
        AgentTool(
            ToolSpec(
                name="metadata.commands",
                description="Return structured mdwf_db command and parameter metadata.",
                risk=RiskLevel.READ,
                fixes="Lets a small model choose valid commands without scraping help text.",
            ),
            command_metadata,
        ),
        AgentTool(
            ToolSpec(
                name="recipes.list",
                description="List saved parameter recipes for an ensemble or global scope.",
                input_schema={
                    "type": "object",
                    "properties": {
                        "ensemble_id": {"type": "integer"},
                        "operation_type": {"type": "string"},
                        "active_only": {"type": "boolean", "default": True},
                    },
                },
                risk=RiskLevel.READ,
                fixes="Makes reusable hyperparameter sets discoverable without table parsing.",
            ),
            list_recipes,
        ),
        AgentTool(
            ToolSpec(
                name="recipes.upsert",
                description="Create or update a reusable parameter recipe.",
                input_schema={
                    "type": "object",
                    "required": ["operation_type", "variant"],
                    "properties": {
                        "ensemble_id": {"type": "integer"},
                        "operation_type": {"type": "string"},
                        "variant": {"type": "string"},
                        "input_params": {"type": "string"},
                        "job_params": {"type": "string"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "notes": {"type": "string"},
                        "active": {"type": "boolean", "default": True},
                    },
                },
                risk=RiskLevel.WRITES_DB,
                requires_confirmation=True,
                dry_run_supported=True,
                verify_tool="recipes.list",
                fixes="Replaces opaque default parameter strings with auditable reusable recipes.",
            ),
            upsert_recipe,
        ),
        AgentTool(
            ToolSpec(
                name="curation_events.record",
                description="Append an audit event for a human or agent curation action.",
                risk=RiskLevel.WRITES_DB,
                requires_confirmation=True,
                fixes="Preserves provenance for ensemble and workflow curation changes.",
            ),
            record_curation_event,
        ),
        AgentTool(
            ToolSpec(
                name="analysis_runs.record",
                description="Record a query/export/analysis artifact and its provenance.",
                risk=RiskLevel.WRITES_DB,
                requires_confirmation=True,
                fixes="Connects analysis artifacts back to exact ensemble/config/query choices.",
            ),
            record_analysis_run,
        ),
        AgentTool(
            ToolSpec(
                name="analysis_runs.list",
                description="List recorded analysis runs.",
                risk=RiskLevel.READ,
                fixes="Lets an agent or practitioner inspect previous exports and analysis provenance.",
            ),
            list_analysis_runs,
        ),
        AgentTool(
            ToolSpec(
                name="remote.doctor",
                description="Run Perlmutter doctor through a configured SSH profile.",
                input_schema=_remote_schema(),
                risk=RiskLevel.READ,
                fixes="Lets a local agent discover real Perlmutter capabilities without running on Perlmutter.",
            ),
            remote_doctor,
        ),
        AgentTool(
            ToolSpec(
                name="remote.monitor",
                description="Run remote monitor through SSH; defaults to dry-run unless approve=true.",
                input_schema=_remote_schema({
                    "ensemble": {"type": "string"},
                    "source": {"type": "string", "enum": ["auto", "squeue", "sacct", "sqs", "jobstats"], "default": "auto"},
                    "dry_run": {"type": "boolean", "default": True},
                    "approve": {"type": "boolean", "default": False},
                }),
                risk=RiskLevel.WRITES_DB,
                requires_confirmation=True,
                dry_run_supported=True,
                fixes="Uses NERSC scheduler state from the correct host without a long-lived remote agent.",
            ),
            remote_monitor,
        ),
        AgentTool(
            ToolSpec(
                name="remote.submit",
                description="Submit a remote Slurm script through SSH.",
                input_schema=_remote_schema({
                    "ensemble": {"type": "string"},
                    "operation_type": {"type": "string"},
                    "script": {"type": "string"},
                    "params": {"type": "string"},
                    "user": {"type": "string"},
                    "dry_run": {"type": "boolean", "default": True},
                    "approve": {"type": "boolean", "default": False},
                }, required=["ensemble", "operation_type", "script"]),
                risk=RiskLevel.SUBMITS_JOBS,
                requires_confirmation=True,
                dry_run_supported=True,
                fixes="Keeps job submission explicit and records the remote operation through mdwf_db submit.",
            ),
            remote_submit,
        ),
        AgentTool(
            ToolSpec(
                name="remote.storage_plan",
                description="Run remote storage planning on Perlmutter.",
                input_schema=_remote_schema({
                    "ensemble": {"type": "string"},
                    "path": {"type": "string"},
                }),
                risk=RiskLevel.READ,
                fixes="Classifies Perlmutter paths from the Perlmutter environment.",
            ),
            remote_storage_plan,
        ),
        AgentTool(
            ToolSpec(
                name="remote.sync_plan",
                description="Plan code/config/result sync with remote Perlmutter.",
                input_schema={
                    "type": "object",
                    "required": ["remote_path"],
                    "properties": {
                        "host": {"type": "string", "default": "perlmutter"},
                        "action": {"type": "string", "enum": ["sync-code", "sync-config", "pull-results"], "default": "sync-code"},
                        "local": {"type": "string", "default": "."},
                        "remote_path": {"type": "string"},
                    },
                },
                risk=RiskLevel.READ,
                fixes="Makes file movement visible and keeps config/*.env out of sync plans.",
            ),
            remote_sync_plan,
        ),
    ]
    return {tool.spec.name: tool for tool in tools}
