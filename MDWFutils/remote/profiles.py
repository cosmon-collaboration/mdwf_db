"""Remote SSH profile loading for short-lived Perlmutter commands."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


DEFAULT_PROFILE_PATH = Path.home() / ".config" / "mdwf_db" / "remote.yaml"


@dataclass(frozen=True)
class RemoteProfile:
    """Connection and remote environment settings for one SSH target."""

    name: str
    host: str
    workdir: Optional[str] = None
    remote_mdwf_db: str = "mdwf_db"
    python_env_setup: Optional[str] = None
    project_root: Optional[str] = None
    default_account: Optional[str] = None
    default_qos: Optional[str] = None


def load_remote_profile(name: str, config_path: Optional[Path] = None) -> RemoteProfile:
    """Load a named SSH profile, with a host-name fallback."""
    path = config_path or DEFAULT_PROFILE_PATH
    data = _load_config(path)
    profiles = data.get("profiles", {})
    raw = profiles.get(name)
    if raw is None:
        # Make the common case zero-config: --host perlmutter means SSH host
        # "perlmutter" with mdwf_db already on PATH. If a profile file exists
        # and contains profiles, a missing profile is more likely a typo.
        if profiles:
            available = ", ".join(sorted(str(key) for key in profiles))
            raise ValueError(f"Remote profile '{name}' not found. Available profiles: {available}")
        raw = {"host": name}
    if not isinstance(raw, dict):
        raise ValueError(f"Remote profile '{name}' must be a mapping")
    host = raw.get("host")
    if not host:
        raise ValueError(f"Remote profile '{name}' is missing required field 'host'")
    return RemoteProfile(
        name=name,
        host=str(host),
        workdir=_optional_str(raw.get("workdir")),
        remote_mdwf_db=str(raw.get("remote_mdwf_db") or raw.get("mdwf_db_path") or "mdwf_db"),
        python_env_setup=_optional_str(raw.get("python_env_setup") or raw.get("python_env")),
        project_root=_optional_str(raw.get("project_root")),
        default_account=_optional_str(raw.get("default_account")),
        default_qos=_optional_str(raw.get("default_qos")),
    )


def profile_template() -> Dict[str, Any]:
    """Return an example profile payload for documentation/JSON output."""
    return {
        "profiles": {
            "perlmutter": {
                "host": "perlmutter",
                "workdir": "/global/cfs/cdirs/<project>/<user>/mdwf_db",
                "remote_mdwf_db": "mdwf_db",
                "python_env_setup": "module load python",
                "project_root": "/global/cfs/cdirs/<project>/<user>/mdwf_db",
                "default_account": "<nersc_account>",
                "default_qos": "regular",
            }
        }
    }


def _load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"Remote config must be a mapping: {path}")
    return loaded


def _optional_str(value) -> Optional[str]:
    return str(value) if value not in (None, "") else None
