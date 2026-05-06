"""Operate Perlmutter through constrained short-lived SSH commands."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from ...remote.commands import build_remote_command, get_remote_command_specs
from ...remote.profiles import load_remote_profile, profile_template
from ...remote.transport import run_remote_command
from ..json_output import print_json


def register(subparsers):
    p = subparsers.add_parser("remote", help="Run allowlisted mdwf_db commands on a remote SSH target")
    sub = p.add_subparsers(dest="action", required=True)

    profile = sub.add_parser("profile-template", help="Print an example remote.yaml")
    profile.add_argument("--json", action="store_true")
    profile.set_defaults(func=do_profile_template)

    for name in ("doctor", "monitor", "status", "storage-plan", "stripe-plan", "submit", "query", "ingest-dry-run"):
        cmd = sub.add_parser(name, help=f"Run remote {name}")
        _add_remote_common(cmd)
        _add_command_args(cmd, name)
        cmd.set_defaults(func=do_remote_command, remote_command=name)

    run = sub.add_parser("run", help="Run an allowlisted remote mdwf_db argv")
    _add_remote_common(run)
    run.add_argument("argv", nargs="+", help="Remote mdwf_db argv, must start with mdwf_db or profile remote_mdwf_db")
    run.set_defaults(func=do_remote_command, remote_command="run")

    for name in ("sync-code", "sync-config", "pull-results"):
        sync = sub.add_parser(name, help=f"Plan or run {name}")
        _add_sync_args(sync, name)
        sync.set_defaults(func=do_sync, sync_action=name)


def _add_remote_common(parser):
    parser.add_argument("--host", default="perlmutter", help="Remote profile name or SSH host alias")
    parser.add_argument("--config", type=Path, help="Remote profile YAML path")
    parser.add_argument("--timeout", type=int, help="SSH command timeout in seconds")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    parser.add_argument("--dry-run-transport", action="store_true", help="Print SSH command without connecting")
    parser.add_argument("--approve", action="store_true", help="Allow mutating remote commands")


def _add_command_args(parser, name):
    if name in ("monitor", "status", "submit", "query", "ingest-dry-run", "storage-plan"):
        parser.add_argument("-e", "--ensemble")
    if name == "monitor":
        parser.add_argument("--source", default="auto")
        parser.add_argument("--dry-run", action="store_true")
    if name == "submit":
        parser.add_argument("-o", "--operation-type", required=True)
        parser.add_argument("--script", required=True)
        parser.add_argument("-p", "--params", default="")
        parser.add_argument("-u", "--user")
        parser.add_argument("--dry-run", action="store_true")
    if name == "storage-plan":
        parser.add_argument("--path")
    if name == "stripe-plan":
        parser.add_argument("--path", required=True)
        parser.add_argument("--mode", default="default")
        parser.add_argument("--nodes", type=int)
    if name == "query":
        parser.add_argument("measurement_type", nargs="?", default="all")
        parser.add_argument("-o", "--output")
        parser.add_argument("--fields", nargs="+")
        parser.add_argument("--cfg-range", nargs=2, type=int)
        parser.add_argument("--cfg-inc", type=int)
        parser.add_argument("--cfg-list", nargs="+", type=int)
        parser.add_argument("--include-pretherm", action="store_true")
    if name == "ingest-dry-run":
        parser.add_argument("measurement_type", nargs="?", default="all")
        parser.add_argument("--creader")
        parser.add_argument("--overwrite", action="store_true")


def _add_sync_args(parser, name):
    parser.add_argument("--host", default="perlmutter")
    parser.add_argument("--config", type=Path)
    default_local = Path("config") if name == "sync-config" else Path(".")
    parser.add_argument("--local", type=Path, default=default_local)
    parser.add_argument("--remote-path", required=True)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--approve", action="store_true", help="Required to run rsync")
    if name == "pull-results":
        parser.add_argument("--include", action="append", default=[])


def do_profile_template(args):
    if args.json:
        print_json(profile_template())
    else:
        print_json(profile_template())
    return 0


def do_remote_command(args):
    profile = load_remote_profile(args.host, args.config)
    specs = get_remote_command_specs()
    spec = specs[args.remote_command]
    command_args = _remote_args(args)
    if spec.requires_confirmation and not args.approve and not command_args.get("dry_run"):
        return _emit(args, {
            "ok": False,
            "status": "approval_required",
            "summary": f"remote {args.remote_command} requires --approve unless --dry-run is passed",
        }, code=1)
    try:
        argv = build_remote_command(args.remote_command, command_args, profile)
        result = run_remote_command(
            profile,
            spec,
            argv,
            timeout_seconds=args.timeout,
            dry_run_transport=args.dry_run_transport,
        )
    except Exception as exc:
        return _emit(args, {"ok": False, "status": "error", "summary": str(exc)}, code=1)
    payload = result.to_dict()
    payload["ok"] = result.ok
    return _emit(args, payload, code=0 if result.ok else result.returncode or 1)


def do_sync(args):
    profile = load_remote_profile(args.host, args.config)
    command = _sync_command(args, profile)
    payload = {
        "ok": True,
        "status": "planned" if not args.approve else "running",
        "command": command,
        "warnings": ["config/*.env is excluded from sync-config and sync-code commands"],
    }
    if not args.approve:
        payload["summary"] = "Sync command planned; pass --approve to run it."
        return _emit(args, payload)
    result = subprocess.run(command, check=False, text=True, capture_output=True)
    payload.update({
        "ok": result.returncode == 0,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    })
    return _emit(args, payload, code=0 if result.returncode == 0 else result.returncode)


def _remote_args(args) -> dict:
    command = args.remote_command
    if command == "run":
        return {"argv": args.argv}
    keys = (
        "ensemble", "source", "dry_run", "operation_type", "script", "params", "user",
        "path", "mode", "nodes", "measurement_type", "output", "fields", "cfg_range",
        "cfg_inc", "cfg_list", "include_pretherm", "creader", "overwrite",
    )
    return {
        key: getattr(args, key)
        for key in keys
        if hasattr(args, key) and getattr(args, key) not in (None, False, "")
    }


def _sync_command(args, profile):
    local_path = args.local.expanduser().resolve()
    local = str(local_path)
    remote_path = args.remote_path
    excludes = ["--exclude", "config/*.env", "--exclude", "*.env", "--exclude", ".git/", "--exclude", "__pycache__/"]
    if args.sync_action == "pull-results":
        command = ["rsync", "-av"]
        for pattern in args.include:
            command.extend(["--include", pattern])
        if args.include:
            command.extend(["--exclude", "*"])
        command.extend([f"{profile.host}:{remote_path}", local])
        return command
    if local_path.is_dir():
        local = f"{local}/"
        remote_path = f"{remote_path.rstrip('/')}/"
    remote = f"{profile.host}:{remote_path}"
    return ["rsync", "-av", *excludes, local, remote]


def _emit(args, payload, code=0):
    if args.json:
        print_json(payload)
    elif not payload.get("ok", True):
        print(f"ERROR: {payload.get('summary')}", file=sys.stderr)
    else:
        print_json(payload)
    return code
