"""Submit a generated SLURM script and create a PENDING operation record."""

from __future__ import annotations

import argparse
import subprocess
from getpass import getuser
from pathlib import Path

from ..ensemble_utils import get_backend_for_args
from ..json_output import print_json


def register(subparsers):
    p = subparsers.add_parser(
        "submit",
        help="Submit a SLURM script and record a PENDING operation",
        description="Run sbatch --parsable for a generated script and record the job ID in MongoDB.",
    )
    p.add_argument("-e", "--ensemble", required=True, help="Ensemble identifier")
    p.add_argument("-o", "--operation-type", required=True, help="Operation type label for tracking")
    p.add_argument("--script", required=True, help="Path to SLURM script")
    p.add_argument("-p", "--params", default="", help="Additional key=val operation params")
    p.add_argument("-u", "--user", default=None, help="Override username associated with the operation")
    p.add_argument("--dry-run", action="store_true", help="Report planned submit without invoking sbatch")
    p.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    p.set_defaults(func=do_submit)


def do_submit(args):
    script = Path(args.script).expanduser().resolve()
    params = _parse_params(args.params)
    payload = {
        "ensemble": args.ensemble,
        "operation_type": args.operation_type,
        "script": str(script),
        "user": args.user or getuser(),
        "params": params,
    }
    if args.dry_run:
        _emit(args, {
            "ok": True,
            "status": "dry_run",
            "summary": "Would submit script and create PENDING operation.",
            "effects": [
                {"type": "would_run", "command": ["sbatch", "--parsable", str(script)]},
                {"type": "would_create_operation", **payload},
            ],
        })
        return 0

    backend = get_backend_for_args(args)
    ensemble_id, ensemble = backend.resolve_ensemble_identifier(args.ensemble)
    if not script.exists():
        _emit(args, {"ok": False, "status": "error", "summary": f"Script not found: {script}"})
        return 1
    result = subprocess.run(
        ["sbatch", "--parsable", str(script)],
        check=False,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        _emit(args, {
            "ok": False,
            "status": "submit_failed",
            "summary": result.stderr.strip() or result.stdout.strip() or "sbatch failed",
            "returncode": result.returncode,
        })
        return result.returncode
    job_id = result.stdout.strip().split(";")[0]
    operation_id = backend.add_operation(
        ensemble_id,
        operation_type=args.operation_type,
        status="PENDING",
        user=args.user or getuser(),
        slurm_job_id=job_id,
        batch_script=str(script),
        **params,
    )
    _emit(args, {
        "ok": True,
        "status": "ok",
        "summary": f"Submitted job {job_id} and created operation {operation_id}.",
        "ensemble_id": ensemble_id,
        "operation_id": operation_id,
        "slurm_job_id": job_id,
        "script": str(script),
        "ensemble_directory": ensemble.get("directory"),
    })
    return 0


def _parse_params(param_string):
    parsed = {}
    for token in (param_string or "").split():
        if "=" in token:
            key, value = token.split("=", 1)
            parsed[key] = value
    return parsed


def _emit(args, payload):
    if args.json:
        print_json(payload)
    else:
        print(payload.get("summary", payload))
