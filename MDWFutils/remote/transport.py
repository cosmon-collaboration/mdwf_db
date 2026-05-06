"""SSH transport for constrained remote commands."""

from __future__ import annotations

import json
import shlex
import subprocess
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .commands import RemoteCommandSpec
from .profiles import RemoteProfile


@dataclass
class RemoteResult:
    """Captured result from one remote command."""

    ok: bool
    returncode: int
    stdout: str
    stderr: str
    host: str
    command_name: str
    duration_seconds: float
    stdout_json: Optional[Any] = None
    warnings: List[str] = field(default_factory=list)
    argv: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "returncode": self.returncode,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "host": self.host,
            "command_name": self.command_name,
            "duration_seconds": self.duration_seconds,
            "stdout_json": self.stdout_json,
            "warnings": self.warnings,
            "argv": self.argv,
        }


def run_remote_command(
    profile: RemoteProfile,
    spec: RemoteCommandSpec,
    remote_argv: List[str],
    *,
    timeout_seconds: Optional[int] = None,
    dry_run_transport: bool = False,
) -> RemoteResult:
    """Run a safe remote command over SSH."""
    remote_script = _remote_script(profile, remote_argv)
    ssh_argv = ["ssh", profile.host, "bash", "-lc", remote_script]
    if dry_run_transport:
        return RemoteResult(
            ok=True,
            returncode=0,
            stdout="",
            stderr="",
            host=profile.host,
            command_name=spec.name,
            duration_seconds=0.0,
            stdout_json={"dry_run_transport": True, "ssh_argv": ssh_argv},
            argv=ssh_argv,
        )
    start = time.monotonic()
    try:
        completed = subprocess.run(
            ssh_argv,
            check=False,
            text=True,
            capture_output=True,
            timeout=timeout_seconds or spec.timeout_seconds,
        )
        duration = time.monotonic() - start
    except subprocess.TimeoutExpired as exc:
        duration = time.monotonic() - start
        return RemoteResult(
            ok=False,
            returncode=124,
            stdout=exc.stdout or "",
            stderr=exc.stderr or f"Remote command timed out after {timeout_seconds or spec.timeout_seconds}s",
            host=profile.host,
            command_name=spec.name,
            duration_seconds=duration,
            warnings=["timeout"],
            argv=ssh_argv,
        )
    parsed = _parse_json(completed.stdout) if spec.expects_json else None
    warnings = []
    if spec.expects_json and parsed is None and completed.stdout.strip():
        warnings.append("stdout was not valid JSON")
    if completed.stderr.strip():
        warnings.append(completed.stderr.strip())
    return RemoteResult(
        ok=completed.returncode == 0,
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        host=profile.host,
        command_name=spec.name,
        duration_seconds=duration,
        stdout_json=parsed,
        warnings=warnings,
        argv=ssh_argv,
    )


def _remote_script(profile: RemoteProfile, remote_argv: List[str]) -> str:
    parts = ["set -e"]
    if profile.workdir:
        parts.append(f"cd {shlex.quote(profile.workdir)}")
    if profile.python_env_setup:
        parts.append(profile.python_env_setup)
    parts.append(shlex.join(remote_argv))
    return " && ".join(parts)


def _parse_json(stdout: str):
    text = stdout.strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None
