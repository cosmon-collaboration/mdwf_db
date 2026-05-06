"""Analysis run schema for export and analysis provenance."""

ANALYSIS_RUN_SCHEMA = {
    "analysis_run_id": int,
    "created_at": "datetime",
    "updated_at": "datetime",
    "status": str,
    "ensemble_ids": list[int],
    "measurement_types": list[str],
    "cfg_selection": {},
    "fields": list[str],
    "output_path": str | None,
    "query_args": {},
    "package_version": str | None,
    "quality_flags": {},
    "notes": str | None,
    "metadata": {},
}

ANALYSIS_RUN_INDEXES = [
    ("created_at", 1),
    ("ensemble_ids", 1),
    ("measurement_types", 1),
    ("status", 1),
]
