"""Runtime database connection helpers."""
import atexit
import os
from ..backends import get_backend
from ..backends.base import DatabaseBackend
from ..exceptions import DatabaseConnectionError

_BACKEND_CACHE: dict[tuple[int, str], tuple[DatabaseBackend, bool, bool]] = {}
_CLEANUP_REGISTERED = False


def get_default_db_connection() -> str | None:
    return os.getenv("MDWF_DB_URL")


def load_default_backend(
    *,
    validate_connection: bool = False,
    ensure_indexes: bool = False,
) -> DatabaseBackend:
    connection = get_default_db_connection()
    if not connection:
        raise DatabaseConnectionError("MDWF_DB_URL environment variable not set")

    _register_cleanup()
    cache_key = (os.getpid(), connection)
    entry = _BACKEND_CACHE.get(cache_key)
    if entry is None:
        backend = get_backend(
            connection,
            validate_connection=validate_connection,
            ensure_indexes=ensure_indexes,
        )
        _BACKEND_CACHE[cache_key] = (backend, validate_connection, ensure_indexes)
        return backend

    backend, already_validated, already_indexed = entry
    if validate_connection and not already_validated:
        backend.validate_connection()
        already_validated = True
    if ensure_indexes and not already_indexed:
        backend.ensure_indexes()
        already_indexed = True
    if already_validated != entry[1] or already_indexed != entry[2]:
        _BACKEND_CACHE[cache_key] = (backend, already_validated, already_indexed)
    return backend


def close_default_backends() -> None:
    """Close cached backends for this process."""
    for backend, _validated, _indexed in list(_BACKEND_CACHE.values()):
        backend.close()
    _BACKEND_CACHE.clear()


def _register_cleanup() -> None:
    global _CLEANUP_REGISTERED
    if _CLEANUP_REGISTERED:
        return
    atexit.register(close_default_backends)
    _CLEANUP_REGISTERED = True
