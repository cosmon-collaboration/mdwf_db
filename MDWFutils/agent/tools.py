"""Concrete agent tool handlers."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Dict

from MDWFutils.backends import get_backend
from MDWFutils.cli.introspect import get_command_metadata
from MDWFutils.cli.components import ParameterManager
from MDWFutils.remote.commands import build_remote_command, get_remote_command_specs
from MDWFutils.remote.profiles import load_remote_profile
from MDWFutils.remote.transport import run_remote_command

from .contracts import ToolResult


def _backend():
    connection = os.getenv("MDWF_DB_URL")
    if not connection:
        raise RuntimeError("MDWF_DB_URL environment variable not set")
    return get_backend(connection)


def _param_hash(input_params: str, job_params: str) -> str:
    digest = hashlib.sha256()
    digest.update((input_params or "").encode("utf-8"))
    digest.update(b"\0")
    digest.update((job_params or "").encode("utf-8"))
    return digest.hexdigest()


def command_metadata(args: Dict) -> ToolResult:
    metadata = get_command_metadata()
    return ToolResult(
        ok=True,
        status="ok",
        summary=f"Loaded metadata for {len(metadata)} commands.",
        data={"commands": metadata},
    )


def list_recipes(args: Dict) -> ToolResult:
    backend = _backend()
    recipes = backend.list_recipes(
        ensemble_id=args.get("ensemble_id"),
        operation_type=args.get("operation_type"),
        active_only=args.get("active_only", True),
    )
    return ToolResult(
        ok=True,
        status="ok",
        summary=f"Found {len(recipes)} recipe(s).",
        data={"recipes": recipes},
    )


def upsert_recipe(args: Dict) -> ToolResult:
    input_params = args.get("input_params", "")
    job_params = args.get("job_params", "")
    parsed_params = {
        "input": ParameterManager.parse(input_params),
        "job": ParameterManager.parse(job_params),
    }
    payload = {
        "ensemble_id": args.get("ensemble_id"),
        "operation_type": args["operation_type"],
        "variant": args["variant"],
        "input_params": input_params,
        "job_params": job_params,
        "parsed_params": parsed_params,
        "schema_hash": args.get("schema_hash") or _param_hash(input_params, job_params),
        "tags": args.get("tags", []),
        "notes": args.get("notes"),
        "active": args.get("active", True),
    }
    if args.get("dry_run"):
        return ToolResult(
            ok=True,
            status="dry_run",
            summary="Recipe validated; no database write performed.",
            data={"recipe": payload},
            effects=[{"type": "would_upsert_recipe", "target": payload}],
        )

    backend = _backend()
    recipe_id = backend.upsert_recipe(**payload)
    return ToolResult(
        ok=True,
        status="ok",
        summary=f"Stored recipe {recipe_id}.",
        data={"recipe_id": recipe_id},
        effects=[{"type": "upsert_recipe", "recipe_id": recipe_id}],
    )


def record_curation_event(args: Dict) -> ToolResult:
    if args.get("dry_run"):
        return ToolResult(
            ok=True,
            status="dry_run",
            summary="Curation event validated; no database write performed.",
            data={"event": args},
            effects=[{"type": "would_record_curation_event"}],
        )
    backend = _backend()
    event_id = backend.add_curation_event(**args)
    return ToolResult(
        ok=True,
        status="ok",
        summary=f"Recorded curation event {event_id}.",
        data={"event_id": event_id},
    )


def record_analysis_run(args: Dict) -> ToolResult:
    if args.get("dry_run"):
        return ToolResult(
            ok=True,
            status="dry_run",
            summary="Analysis run validated; no database write performed.",
            data={"analysis_run": args},
            effects=[{"type": "would_record_analysis_run"}],
        )
    backend = _backend()
    run_id = backend.add_analysis_run(**args)
    return ToolResult(
        ok=True,
        status="ok",
        summary=f"Recorded analysis run {run_id}.",
        data={"analysis_run_id": run_id},
    )


def list_analysis_runs(args: Dict) -> ToolResult:
    backend = _backend()
    runs = backend.list_analysis_runs(
        ensemble_id=args.get("ensemble_id"),
        status=args.get("status"),
        limit=int(args.get("limit", 20)),
    )
    return ToolResult(
        ok=True,
        status="ok",
        summary=f"Found {len(runs)} analysis run(s).",
        data={"analysis_runs": runs},
    )


def remote_doctor(args: Dict) -> ToolResult:
    return _remote_tool("doctor", args, mutating=False)


def remote_monitor(args: Dict) -> ToolResult:
    payload = dict(args)
    if not payload.get("approve"):
        payload["dry_run"] = True
    return _remote_tool("monitor", payload, mutating=not payload.get("dry_run"))


def remote_submit(args: Dict) -> ToolResult:
    payload = dict(args)
    if not payload.get("dry_run") and not payload.get("approve"):
        return ToolResult(
            ok=False,
            status="approval_required",
            summary="remote.submit requires approve=true unless dry_run=true",
        )
    return _remote_tool("submit", payload, mutating=not payload.get("dry_run"))


def remote_storage_plan(args: Dict) -> ToolResult:
    return _remote_tool("storage-plan", args, mutating=False)


def remote_sync_plan(args: Dict) -> ToolResult:
    host = args.get("host", "perlmutter")
    remote_path = args.get("remote_path")
    local_path = args.get("local", ".")
    action = args.get("action", "sync-code")
    if not remote_path:
        return ToolResult(ok=False, status="error", summary="remote.sync_plan requires remote_path")
    return ToolResult(
        ok=True,
        status="planned",
        summary=f"Planned {action} for {host}.",
        data={
            "action": action,
            "host": host,
            "local": local_path,
            "remote_path": remote_path,
            "excludes": ["config/*.env", ".git/", "__pycache__/"],
        },
        effects=[{"type": "would_sync", "action": action, "host": host}],
        warnings=["This tool only plans sync; use mdwf_db remote sync-* --approve to run rsync."],
    )


def _remote_tool(command_name: str, args: Dict, mutating: bool) -> ToolResult:
    payload = dict(args or {})
    host = payload.pop("host", "perlmutter")
    config = payload.pop("config", None)
    timeout = payload.pop("timeout", None)
    dry_run_transport = payload.pop("dry_run_transport", False)
    payload.pop("approve", None)
    try:
        profile = load_remote_profile(host, Path(config) if config else None)
        spec = get_remote_command_specs()[command_name]
        argv = build_remote_command(command_name, payload, profile)
        result = run_remote_command(
            profile,
            spec,
            argv,
            timeout_seconds=timeout,
            dry_run_transport=dry_run_transport,
        )
    except Exception as exc:
        return ToolResult(ok=False, status="error", summary=str(exc))
    return ToolResult(
        ok=result.ok,
        status="ok" if result.ok else "error",
        summary=f"remote {command_name} exited with {result.returncode}",
        data=result.to_dict(),
        warnings=result.warnings,
        effects=[{"type": "remote_mutation", "command": command_name}] if mutating else [],
    )
