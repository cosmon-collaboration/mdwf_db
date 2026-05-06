"""Monitor active operation records against SLURM scheduler state."""

from __future__ import annotations

import argparse
import shutil
import subprocess

from ..ensemble_utils import get_backend_for_args
from ..json_output import print_json

ACTIVE_STATUSES = {"PENDING", "RUNNING", "SUBMITTED"}


def register(subparsers):
    p = subparsers.add_parser(
        "monitor",
        help="Reconcile active operation records with SLURM state",
        description="Inspect PENDING/RUNNING operations and optionally update terminal statuses.",
    )
    p.add_argument("-e", "--ensemble", help="Optional ensemble identifier")
    p.add_argument(
        "--source",
        choices=["auto", "squeue", "sacct", "sqs", "jobstats"],
        default="auto",
        help="Scheduler source preference",
    )
    p.add_argument("--dry-run", action="store_true", help="Report status updates without writing")
    p.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    p.set_defaults(func=do_monitor)


def do_monitor(args):
    backend = get_backend_for_args(args)
    ensembles = _resolve_ensembles(backend, args.ensemble)
    warnings = []
    updates = []
    for ensemble_id, _ensemble in ensembles:
        for op in backend.list_operations(ensemble_id):
            current = str(op.get("status", "")).upper()
            job_id = op.get("slurm", {}).get("job_id")
            if current not in ACTIVE_STATUSES or not job_id:
                continue
            scheduler = _scheduler_state(job_id, source=args.source)
            if scheduler.get("warning"):
                warnings.append(scheduler["warning"])
            desired = _map_state(scheduler.get("state"), current)
            payload = {
                "ensemble_id": ensemble_id,
                "operation_id": op.get("operation_id"),
                "slurm_job_id": job_id,
                "current_status": current,
                "scheduler_state": scheduler.get("state"),
                "scheduler_source": scheduler.get("source"),
                "desired_status": desired,
                "would_update": desired != current or bool(scheduler.get("state")),
            }
            updates.append(payload)
            if not args.dry_run and payload["would_update"]:
                update_fields = {"slurm.slurm_status": scheduler.get("state")}
                if scheduler.get("exit_code") is not None:
                    update_fields["slurm.exit_code"] = scheduler["exit_code"]
                backend.update_operation_by_id(op["operation_id"], desired, **update_fields)

    result = {
        "ok": True,
        "status": "dry_run" if args.dry_run else "ok",
        "updates": updates,
        "warnings": sorted(set(warnings)),
    }
    if args.json:
        print_json(result)
    else:
        for update in updates:
            verb = "Would update" if args.dry_run else "Checked"
            print(
                f"{verb} op {update['operation_id']} job {update['slurm_job_id']}: "
                f"{update['current_status']} -> {update['desired_status']} "
                f"({update['scheduler_state'] or 'unknown'})"
            )
        for warning in sorted(set(warnings)):
            print(f"WARNING: {warning}")
    return 0


def _resolve_ensembles(backend, identifier):
    if identifier:
        ensemble_id, ensemble = backend.resolve_ensemble_identifier(identifier)
        return [(ensemble_id, ensemble)]
    return [(ens["ensemble_id"], ens) for ens in backend.list_ensembles(detailed=True)]


def _scheduler_state(job_id, source="auto"):
    if source in ("auto", "squeue", "sqs") and shutil.which("squeue"):
        result = subprocess.run(
            ["squeue", "-h", "-j", str(job_id), "-o", "%T"],
            check=False,
            text=True,
            capture_output=True,
        )
        state = result.stdout.strip().splitlines()
        if state:
            return {"state": state[0].strip(), "source": "squeue"}
    if source in ("auto", "sacct", "jobstats") and shutil.which("sacct"):
        result = subprocess.run(
            ["sacct", "-X", "-j", str(job_id), "-n", "-o", "State,ExitCode", "--parsable2"],
            check=False,
            text=True,
            capture_output=True,
        )
        lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        if lines:
            parts = lines[0].split("|")
            state = parts[0] if parts else None
            exit_code = None
            if len(parts) > 1 and ":" in parts[1]:
                try:
                    exit_code = int(parts[1].split(":", 1)[0])
                except ValueError:
                    exit_code = None
            return {"state": state, "exit_code": exit_code, "source": "sacct"}
    if source == "sqs":
        return {"state": None, "warning": "sqs summaries are not job-specific; falling back found no squeue state"}
    if source == "jobstats":
        return {"state": None, "warning": "jobstats is used for completed-job efficiency, not active state"}
    return {"state": None, "warning": "Neither squeue nor sacct produced scheduler state"}


def _map_state(state, current):
    if not state:
        return current
    upper = state.upper()
    if "RUNNING" in upper or upper == "R":
        return "RUNNING"
    if "PENDING" in upper or upper == "PD":
        return "PENDING"
    if "COMPLETED" in upper:
        return "COMPLETED"
    if "TIMEOUT" in upper:
        return "TIMEOUT"
    if "CANCEL" in upper or "PREEMPT" in upper:
        return "CANCELED"
    if "FAIL" in upper or "NODE_FAIL" in upper:
        return "FAILED"
    return current
