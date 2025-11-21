"""Ensemble document schema definition."""

ENSEMBLE_SCHEMA = {
    "ensemble_id": int,
    "directory": str,
    "nickname": str | None,
    "status": str,
    "physics": {
        "beta": float,
        "b": float,
        "Ls": int,
        "mc": float,
        "ms": float,
        "ml": float,
        "L": int,
        "T": int,
    },
    "configurations": {
        "first": int | None,
        "last": int | None,
        "increment": int | None,
        "total": int | None,
    },
    "hmc_paths": {
        "exec_path": str | None,
        "bind_script_gpu": str | None,
        "bind_script_cpu": str | None,
    },
    "default_params": {},
    "tags": list[str],
    "notes": str | None,
}

ENSEMBLE_INDEXES = [
    ("directory", 1),
    ("nickname", 1),
    ("ensemble_id", 1),
    ("status", 1),
]


