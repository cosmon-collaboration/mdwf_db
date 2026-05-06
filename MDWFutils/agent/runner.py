"""Minimal execution support for agent action plans."""

from __future__ import annotations

from typing import Iterable, List

from .cli_bridge import run_cli_tool
from .contracts import ActionPlan, RiskLevel, ToolResult
from .registry import get_tool_registry


def run_plan(plan: ActionPlan, approved_steps: Iterable[str] = ()) -> List[ToolResult]:
    """Run a plan until a step fails or requires approval."""
    approved = set(approved_steps)
    registry = get_tool_registry()
    results: List[ToolResult] = []
    for step in plan.steps:
        if step.risk != RiskLevel.READ and step.id not in approved:
            result = ToolResult(
                ok=False,
                status="approval_required",
                summary=f"Step {step.id} requires approval before execution.",
                data={"step": step},
            )
            results.append(result)
            break
        result = None
        if step.tool.startswith("cli."):
            result = run_cli_tool(step.tool[4:], step.args)
        elif step.tool not in registry:
            result = ToolResult(
                ok=False,
                status="unsupported_tool",
                summary=f"Runner cannot execute {step.tool}; use the matching mdwf_db CLI command.",
                data={"step": step},
            )
        else:
            tool = registry[step.tool]
            result = tool.handler(step.args)
        results.append(result)
        if not result.ok:
            break
    return results
