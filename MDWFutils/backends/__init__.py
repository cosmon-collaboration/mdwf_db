"""Database backend factory."""

from __future__ import annotations

from .base import DatabaseBackend
from .mongodb import MongoDBBackend
from .sqlite import SQLiteBackend


def get_backend(connection_string: str) -> DatabaseBackend:
    """Return the appropriate backend based on the connection string."""
    if connection_string.startswith(("mongodb://", "mongodb+srv://")):
        return MongoDBBackend(connection_string)
    return SQLiteBackend(connection_string)


__all__ = ["DatabaseBackend", "MongoDBBackend", "SQLiteBackend", "get_backend"]


