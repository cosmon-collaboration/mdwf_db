"""Runtime database connection helpers."""
import os
from ..backends import get_backend
from ..exceptions import DatabaseConnectionError

def get_default_db_connection() -> str | None:
    return os.getenv("MDWF_DB_URL")

def load_default_backend():
    connection = get_default_db_connection()
    if not connection:
        raise DatabaseConnectionError("MDWF_DB_URL environment variable not set")
    return get_backend(connection)
