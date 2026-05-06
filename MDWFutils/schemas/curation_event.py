"""Curation event schema for audit trails."""

CURATION_EVENT_SCHEMA = {
    "event_id": int,
    "timestamp": "datetime",
    "actor": str,
    "tool": str | None,
    "target": {
        "collection": str | None,
        "id": str | int | None,
    },
    "before": {},
    "after": {},
    "summary": str | None,
    "risk": str | None,
    "approval_id": str | None,
    "metadata": {},
}

CURATION_EVENT_INDEXES = [
    ("timestamp", 1),
    [("target.collection", 1), ("target.id", 1)],
    ("actor", 1),
]
