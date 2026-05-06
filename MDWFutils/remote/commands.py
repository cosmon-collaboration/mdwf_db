"""Allowlisted remote command templates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .profiles import RemoteProfile


@dataclass(frozen=True)
class RemoteCommandSpec:
    """A safe remote command template."""

    name: str
    risk: str = "read"
    requires_confirmation: bool = False
    expects_json: bool = True
    timeout_seconds: int = 60


def get_remote_command_specs() -> Dict[str, RemoteCommandSpec]:
    specs = [
        RemoteCommandSpec("doctor", timeout_seconds=60),
        RemoteCommandSpec("monitor", timeout_seconds=120),
        RemoteCommandSpec("status", timeout_seconds=120),
        RemoteCommandSpec("storage-plan", timeout_seconds=120),
        RemoteCommandSpec("stripe-plan", timeout_seconds=120),
        RemoteCommandSpec("submit", risk="submits_jobs", requires_confirmation=True, timeout_seconds=120),
        RemoteCommandSpec("query", timeout_seconds=300),
        RemoteCommandSpec("ingest-dry-run", timeout_seconds=300),
        RemoteCommandSpec("run", timeout_seconds=300),
    ]
    return {spec.name: spec for spec in specs}


def build_remote_command(
    command_name: str,
    args: Dict[str, Any],
    profile: RemoteProfile,
) -> List[str]:
    """Build remote argv for an allowlisted command."""
    args = dict(args or {})
    mdwf = profile.remote_mdwf_db
    if command_name == "doctor":
        _append_common([], args, allowed=set())
        return [mdwf, "perlmutter", "doctor", "--json"]
    if command_name == "monitor":
        argv = [mdwf, "monitor", "--json"]
        _append_common(argv, args, allowed={"ensemble", "source", "dry_run"})
        if "source" not in args:
            argv.extend(["--source", "auto"])
        return argv
    if command_name == "status":
        argv = [mdwf, "status", "--json"]
        _append_common(argv, args, allowed={"ensemble", "measurements", "missing", "measured", "cfg_range", "op", "dir"})
        return argv
    if command_name == "storage-plan":
        argv = [mdwf, "storage", "plan", "--json"]
        _append_common(argv, args, allowed={"ensemble", "path"})
        return argv
    if command_name == "stripe-plan":
        argv = [mdwf, "fs", "stripe-plan", "--json"]
        _append_common(argv, args, allowed={"path", "mode", "nodes"})
        return argv
    if command_name == "submit":
        argv = [mdwf, "submit", "--json"]
        _append_common(argv, args, allowed={"ensemble", "operation_type", "script", "params", "user", "dry_run"})
        return argv
    if command_name == "query":
        variant = str(args.pop("variant", args.pop("measurement_type", "all")))
        argv = [mdwf, "query", variant, "--json"]
        _append_common(argv, args, allowed={"ensemble", "output", "cfg_range", "cfg_inc", "cfg_list", "fields", "include_pretherm"})
        return argv
    if command_name == "ingest-dry-run":
        variant = str(args.pop("variant", args.pop("measurement_type", "all")))
        argv = [mdwf, "ingest", variant, "--dry-run", "--json"]
        _append_common(argv, args, allowed={"ensemble", "creader", "overwrite"})
        return argv
    if command_name == "run":
        # Run is intentionally narrow: execute a remote mdwf_db subcommand already
        # represented as argv, not an arbitrary shell command.
        argv = args.get("argv")
        if not isinstance(argv, list) or not argv:
            raise ValueError("remote run requires argv=[...]")
        if str(argv[0]) != mdwf and str(argv[0]) != "mdwf_db":
            raise ValueError("remote run argv must start with remote mdwf_db executable")
        clean_argv = [str(item) for item in argv]
        _validate_remote_run_argv(clean_argv)
        if "--json" not in clean_argv and "--help" not in clean_argv and "-h" not in clean_argv:
            clean_argv.append("--json")
        return clean_argv
    raise ValueError(f"Unknown remote command '{command_name}'")


def _append_common(argv: List[str], args: Dict[str, Any], allowed: set[str]) -> None:
    unknown = set(args) - allowed
    if unknown:
        raise ValueError(f"Unsupported remote argument(s): {', '.join(sorted(unknown))}")
    mapping = {
        "ensemble": "--ensemble",
        "operation_type": "--operation-type",
        "cfg_range": "--cfg-range",
        "cfg_inc": "--cfg-inc",
        "cfg_list": "--cfg-list",
        "include_pretherm": "--include-pretherm",
        "dry_run": "--dry-run",
    }
    for key, value in args.items():
        if value is None or value is False or value == "":
            continue
        flag = mapping.get(key, f"--{key.replace('_', '-')}")
        if value is True:
            argv.append(flag)
        elif isinstance(value, (list, tuple)):
            argv.append(flag)
            argv.extend(str(item) for item in value)
        else:
            argv.extend([flag, str(value)])


def _validate_remote_run_argv(argv: List[str]) -> None:
    if len(argv) < 2:
        raise ValueError("remote run requires a mdwf_db subcommand")
    sub = argv[1:]
    if sub[:2] == ["perlmutter", "doctor"]:
        return
    if sub[:2] == ["storage", "plan"]:
        return
    if sub[:2] == ["fs", "stripe-plan"]:
        return
    if sub[0] in {"monitor", "status", "query"}:
        return
    if sub[0] in {"submit", "ingest"} and "--dry-run" in sub:
        return
    raise ValueError(
        "remote run only allows doctor, monitor, status, storage plan, "
        "fs stripe-plan, query, submit --dry-run, and ingest --dry-run"
    )
