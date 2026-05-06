"""Agent-facing tool manifest, calls, and deterministic workflow plans."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from ...agent.registry import get_tool_registry
from ...agent.runner import run_plan
from ...agent.workflows import plan_workflow
from ...agent.contracts import ActionPlan
from ..json_output import print_json


def register(subparsers):
    p = subparsers.add_parser(
        "agent",
        help="Agent-facing tool manifest, tool calls, and workflow plans",
        description="""
Expose stable JSON contracts for lightweight agents.

The agent interface is intentionally stricter than the shell-facing CLI:
models select named tools or named workflows and receive structured JSON.
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = p.add_subparsers(dest="action", required=True)

    tools = sub.add_parser("tools", help="List agent tool specs")
    tools.add_argument("--json", action="store_true", help="Print tool specs as JSON")
    tools.set_defaults(func=do_tools)

    call = sub.add_parser("call", help="Call a native agent tool")
    call.add_argument("tool", help="Tool name from `mdwf_db agent tools --json`")
    call.add_argument("--json-args", type=Path, help="JSON file containing tool arguments")
    call.add_argument("--arg", action="append", default=[], help="Single key=value argument")
    call.set_defaults(func=do_call)

    plan = sub.add_parser("plan", help="Create a deterministic workflow plan")
    plan.add_argument("workflow", help="Workflow name")
    plan.add_argument("--json-args", type=Path, help="JSON file containing workflow arguments")
    plan.add_argument("--arg", action="append", default=[], help="Single key=value argument")
    plan.set_defaults(func=do_plan)

    run = sub.add_parser("run", help="Run native-tool steps from a saved plan JSON")
    run.add_argument("--plan-file", type=Path, required=True, help="Path to ActionPlan JSON")
    run.add_argument("--approve", action="append", default=[], help="Step ID approved for mutation")
    run.set_defaults(func=do_run)


def do_tools(args):
    registry = get_tool_registry()
    payload = {
        name: tool.spec
        for name, tool in sorted(registry.items())
    }
    if args.json:
        print_json({"tools": payload})
    else:
        for name, tool in sorted(registry.items()):
            marker = "requires approval" if tool.spec.requires_confirmation else "read"
            print(f"{name}: {tool.spec.description} ({marker})")
    return 0


def do_call(args):
    registry = get_tool_registry()
    if args.tool not in registry:
        print(f"ERROR: Unknown agent tool: {args.tool}", file=sys.stderr)
        return 1
    tool_args = _load_args(args)
    try:
        result = registry[args.tool].handler(tool_args)
    except Exception as exc:
        print_json({
            "ok": False,
            "status": "error",
            "summary": str(exc),
            "data": {"tool": args.tool},
        })
        return 1
    print_json(result)
    return 0 if result.ok else 1


def do_plan(args):
    try:
        plan = plan_workflow(args.workflow, _load_args(args))
    except Exception as exc:
        print_json({
            "ok": False,
            "status": "error",
            "summary": str(exc),
        })
        return 1
    print_json(plan)
    return 0


def do_run(args):
    try:
        raw = json.loads(args.plan_file.read_text())
        plan = ActionPlan(**raw)
        results = run_plan(plan, approved_steps=args.approve)
    except Exception as exc:
        print_json({
            "ok": False,
            "status": "error",
            "summary": str(exc),
        })
        return 1
    print_json({"results": results})
    return 0 if all(result.ok for result in results) else 1


def _load_args(args) -> dict:
    payload = {}
    if getattr(args, "json_args", None):
        payload.update(json.loads(args.json_args.read_text()))
    for token in getattr(args, "arg", []) or []:
        if "=" not in token:
            raise ValueError(f"Malformed --arg token '{token}', expected key=value")
        key, value = token.split("=", 1)
        payload[key] = _coerce_value(value)
    return payload


def _coerce_value(value: str):
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered == "none":
        return None
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value
