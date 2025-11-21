"""Operation document schema definition for SLURM tracking."""

OPERATION_SCHEMA = {
    "operation_id": int,
    "ensemble_id": int,
    "ensemble_directory": str,
    "operation_type": str,
    "status": str,
    "timing": {
        "creation_time": "datetime",
        "start_time": "datetime | None",
        "update_time": "datetime",
        "end_time": "datetime | None",
        "runtime_seconds": int | None,
    },
    "slurm": {
        "job_id": str | None,
        "user": str,
        "host": str | None,
        "batch_script": str | None,
        "output_log": str | None,
        "error_log": str | None,
        "exit_code": int | None,
        "slurm_status": str | None,
    },
    "execution": {
        "run_dir": str | None,
        "config_start": int | None,
        "config_end": int | None,
        "config_increment": int | None,
    },
    "chain": {
        "parent_operation_id": int | None,
        "attempt_number": int,
        "is_chain_member": bool,
    },
    "params": {},
}

OPERATION_INDEXES = [
    ("ensemble_id", 1),
    ("operation_type", 1),
    ("status", 1),
    ("slurm.job_id", 1),
    [("ensemble_id", 1), ("operation_type", 1), ("timing.creation_time", -1)],
]


