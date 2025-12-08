"""Database backend factory."""

from __future__ import annotations

from .base import DatabaseBackend
from .mongodb import MongoDBBackend
from ..exceptions import ConnectionError


def get_backend(connection_string: str) -> DatabaseBackend:
    """Return a MongoDB backend; SQLite is no longer supported."""
    if not connection_string.startswith(("mongodb://", "mongodb+srv://")):
        raise ConnectionError(
            "MongoDB connection required. Set MDWF_DB_URL environment variable.\n"
            "Example: export MDWF_DB_URL=mongodb://host:port/database"
        )
    return MongoDBBackend(connection_string)


__all__ = ["DatabaseBackend", "MongoDBBackend", "get_backend"]


