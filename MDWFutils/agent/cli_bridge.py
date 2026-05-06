"""Constrained bridge from agent plan steps to mdwf_db CLI calls."""

from __future__ import annotations

import contextlib
import io
import sys
from typing import Any, Dict, List

from MDWFutils.cli.main import main

from .contracts import ToolResult


def run_cli_tool(command_name: str, args: Dict[str, Any]) -> ToolResult:
    """Execute one known mdwf_db command without exposing arbitrary shell."""
    argv = _build_argv(command_name, args)
    stdout = io.StringIO()
    stderr = io.StringIO()
    old_argv = sys.argv[:]
    try:
        sys.argv = ["mdwf_db", *argv]
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            try:
                code = main()
            except SystemExit as exc:
                code = int(exc.code or 0)
    finally:
        sys.argv = old_argv
    out = stdout.getvalue()
    err = stderr.getvalue()
    return ToolResult(
        ok=code == 0,
        status="ok" if code == 0 else "error",
        summary=f"mdwf_db {' '.join(argv)} exited with {code}",
        data={
            "argv": argv,
            "returncode": code,
            "stdout": out,
            "stderr": err,
        },
        warnings=[err.strip()] if err.strip() else [],
    )


def _build_argv(command_name: str, args: Dict[str, Any]) -> List[str]:
    args = dict(args or {})
    if command_name == "ingest":
        measurement_type = args.pop("measurement_type", args.pop("variant", "all"))
        argv = ["ingest", str(measurement_type)]
    elif command_name == "query_fields":
        measurement_type = args.pop("measurement_type", "all")
        argv = ["query", str(measurement_type), "--list-fields"]
    elif command_name == "query":
        measurement_type = args.pop("measurement_type", args.pop("variant", "all"))
        argv = ["query", str(measurement_type)]
    else:
        argv = [command_name]
    argv.extend(_args_to_flags(args))
    return argv


def _args_to_flags(args: Dict[str, Any]) -> List[str]:
    tokens: List[str] = []
    for key, value in args.items():
        if value is None:
            continue
        flag = f"--{key.replace('_', '-')}"
        if isinstance(value, bool):
            if value:
                tokens.append(flag)
            continue
        if isinstance(value, (list, tuple)):
            tokens.append(flag)
            tokens.extend(str(item) for item in value)
            continue
        tokens.extend([flag, str(value)])
    return tokens
