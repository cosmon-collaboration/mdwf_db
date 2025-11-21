"""Measurement document schema definition."""

MEASUREMENT_SCHEMA = {
    "measurement_id": "ObjectId",
    "ensemble_id": int,
    "config_number": int,
    "measurement_type": str,
    "measurement_time": "datetime",
    "data": {},
    "metadata": {
        "version": str | None,
        "parameters": {},
        "quality_flags": {},
    },
}

MEASUREMENT_INDEXES = [
    ("ensemble_id", 1),
    ("config_number", 1),
    ("measurement_type", 1),
    [
        ("ensemble_id", 1),
        ("config_number", 1),
        ("measurement_type", 1),
    ],
]


