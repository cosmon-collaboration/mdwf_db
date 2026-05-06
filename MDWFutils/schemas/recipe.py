"""Recipe document schema for reusable operation parameter sets."""

RECIPE_SCHEMA = {
    "recipe_id": int,
    "ensemble_id": int | None,
    "scope": str,
    "operation_type": str,
    "variant": str,
    "input_params": str,
    "job_params": str,
    "parsed_params": {
        "input": {},
        "job": {},
    },
    "schema_hash": str | None,
    "tags": list[str],
    "notes": str | None,
    "active": bool,
    "created_at": "datetime",
    "updated_at": "datetime",
}

RECIPE_INDEXES = [
    [("ensemble_id", 1), ("operation_type", 1), ("variant", 1)],
    [("operation_type", 1), ("variant", 1)],
    ("active", 1),
]
