import sqlite3
import datetime
import time
import getpass
import os
from pathlib import Path
from functools import wraps
from typing import Dict, List, Optional

from .base import DatabaseBackend

# -----------------------------------------------------------------------------
def get_current_user():
    """
    Get the current username for tracking operations.
    """
    try:
        return getpass.getuser()
    except Exception:
        return "unknown"


# -----------------------------------------------------------------------------
def get_connection(db_file, timeout=30.0):
    """
    Open sqlite3 in IMMEDIATE mode, set journal mode, and set a busy timeout.

    Journal mode defaults to WAL but can be overridden with the environment
    variable MDWF_DB_JOURNAL. Acceptable values include WAL, DELETE, TRUNCATE,
    MEMORY, OFF. On some networked filesystems WAL may cause 'disk I/O error'.
    Set MDWF_DB_JOURNAL=DELETE to improve compatibility.
    """
    conn = sqlite3.connect(db_file, timeout=timeout)
    conn.isolation_level = "IMMEDIATE"

    journal = os.getenv('MDWF_DB_JOURNAL', 'WAL').upper()
    try:
        conn.execute(f"PRAGMA journal_mode = {journal};")
    except sqlite3.OperationalError:
        # Fallback to DELETE if requested mode fails
        try:
            conn.execute("PRAGMA journal_mode = DELETE;")
        except sqlite3.OperationalError:
            pass

    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn


def retry_db(max_retries=3, retry_delay=1.0):
    """
    Decorator: retry on 'database is locked' errors.
    """
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            last = None
            for i in range(max_retries):
                try:
                    return fn(*args, **kwargs)
                except sqlite3.OperationalError as e:
                    last = e
                    if "locked" in str(e).lower() and i < max_retries-1:
                        time.sleep(retry_delay)
                        continue
                    raise
            raise last
        return wrapper
    return deco


# -----------------------------------------------------------------------------
@retry_db()
def init_database(db_file):
    """
    Create fresh schema:
      - ensembles (+ status TUNING|PRODUCTION)
      - ensemble_parameters (dynamic K/V)
      - operations  (no more config_*/exit_code/runtime columns)
      - operation_parameters (dynamic K/V)
    """
    p = Path(db_file)
    p.parent.mkdir(parents=True, exist_ok=True)

    conn = get_connection(db_file)
    c = conn.cursor()

    c.executescript("""
    CREATE TABLE IF NOT EXISTS ensembles (
      id            INTEGER PRIMARY KEY,
      directory     TEXT    UNIQUE,
      creation_time TEXT,
      description   TEXT,
      status        TEXT    NOT NULL
                         CHECK(status IN ('TUNING','PRODUCTION'))
                         DEFAULT 'TUNING'
    );

    CREATE TABLE IF NOT EXISTS ensemble_parameters (
      ensemble_id  INTEGER,
      name         TEXT,
      value        TEXT,
      PRIMARY KEY(ensemble_id,name),
      FOREIGN KEY(ensemble_id) REFERENCES ensembles(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS operations (
      id               INTEGER PRIMARY KEY,
      ensemble_id      INTEGER NOT NULL,
      operation_type   TEXT    NOT NULL,
      status           TEXT    NOT NULL,
      creation_time    TEXT    NOT NULL,
      update_time      TEXT    NOT NULL,
      user             TEXT    NOT NULL,
      FOREIGN KEY(ensemble_id) REFERENCES ensembles(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS operation_parameters (
      operation_id INTEGER,
      name         TEXT,
      value        TEXT,
      PRIMARY KEY(operation_id,name),
      FOREIGN KEY(operation_id) REFERENCES operations(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_ensemble_dir ON ensembles(directory);
    CREATE INDEX IF NOT EXISTS idx_ens_params_name_value ON ensemble_parameters(name,value);
    CREATE INDEX IF NOT EXISTS idx_ops_ensemble ON operations(ensemble_id);
    """)
    conn.commit()
    conn.close()
    return True


# -----------------------------------------------------------------------------
@retry_db()
def add_ensemble(db_file, directory, params, description=None):
    """
    Insert a new ensemble + its dynamic params.
    Returns (ensemble_id, created_flag).
    """
    conn = get_connection(db_file)
    c = conn.cursor()

    # if it already exists, return existing id
    c.execute("SELECT id FROM ensembles WHERE directory=?", (directory,))
    row = c.fetchone()
    if row:
        conn.close()
        return row[0], False

    now = datetime.datetime.now().isoformat()
    conn.execute("BEGIN")
    c.execute("""
      INSERT INTO ensembles (directory,creation_time,description,status)
           VALUES (?,?,?, 'TUNING')
    """, (directory, now, description))
    eid = c.lastrowid

    # insert dynamic parameters
    for k, v in params.items():
        c.execute("""
          INSERT INTO ensemble_parameters (ensemble_id,name,value)
               VALUES (?,?,?)
        """, (eid, k, str(v)))

    conn.commit()
    conn.close()
    return eid, True


# -----------------------------------------------------------------------------
@retry_db()
def find_ensemble_by_directory(db_file, directory):
    """
    Return ensemble.id if found, else None.
    """
    conn = get_connection(db_file)
    c = conn.cursor()
    c.execute("SELECT id FROM ensembles WHERE directory=?", (directory,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


# -----------------------------------------------------------------------------
@retry_db()
def get_ensemble_details(db_file, ensemble_id):
    """
    Return a dict with:
      id, directory, creation_time, description, status,
      parameters (dict), operation_count
    or None if not found.
    """
    conn = get_connection(db_file)
    c = conn.cursor()

    c.execute("""
      SELECT directory,creation_time,description,status
        FROM ensembles
       WHERE id=?
    """, (ensemble_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return None
    directory, ctime, desc, status = row

    c.execute("""
      SELECT name,value FROM ensemble_parameters
       WHERE ensemble_id=? ORDER BY name
    """, (ensemble_id,))
    params = dict(c.fetchall())

    c.execute("SELECT COUNT(*) FROM operations WHERE ensemble_id=?", (ensemble_id,))
    opcount = c.fetchone()[0]

    conn.close()
    return {
      'id'             : ensemble_id,
      'directory'      : directory,
      'creation_time'  : ctime,
      'description'    : desc,
      'status'         : status,
      'parameters'     : params,
      'operation_count': opcount
    }


# -----------------------------------------------------------------------------
@retry_db()
def list_ensembles(db_file, detailed=False):
    """
    Return a list of dicts for all ensembles.
    If detailed=True, include dynamic params and op counts.
    """
    conn = get_connection(db_file)
    c = conn.cursor()

    c.execute("""
      SELECT id,directory,creation_time,description,status
        FROM ensembles
       ORDER BY id
    """)
    out = []
    for eid, d, ct, desc, st in c.fetchall():
        entry = {
          'id'            : eid,
          'directory'     : d,
          'creation_time' : ct,
          'description'   : desc,
          'status'        : st
        }
        if detailed:
            c.execute("""
              SELECT name,value FROM ensemble_parameters
               WHERE ensemble_id=? ORDER BY name
            """, (eid,))
            entry['parameters'] = dict(c.fetchall())
            c.execute("SELECT COUNT(*) FROM operations WHERE ensemble_id=?", (eid,))
            entry['operation_count'] = c.fetchone()[0]
        out.append(entry)

    conn.close()
    return out



# -----------------------------------------------------------------------------
@retry_db()
def update_operation(db_file, ensemble_id,
                     operation_type, status,
                     operation_id=None, params=None, user=None):
    """
    Insert a new operation or update an existing one.
    All extra fields (config ranges, exit_code, runtime, etc.)
    must be provided in the `params` dict.
    Returns (op_id, created_flag, message).
    """
    if user is None:
        user = get_current_user()
    conn = get_connection(db_file)
    c = conn.cursor()

    # verify ensemble exists
    c.execute("SELECT 1 FROM ensembles WHERE id=?", (ensemble_id,))
    if not c.fetchone():
        conn.close()
        return None, False, "Ensemble not found"

    now = datetime.datetime.now().isoformat()
    conn.execute("BEGIN")

    if operation_id:
        # — UPDATE existing operation in‐place —
        oid = operation_id
        c.execute("""
          UPDATE operations
             SET status=?, update_time=?, user=?
           WHERE id=?
        """, (status, now, user, oid))
        created = False
        msg = "Updated"
    else:
        # Check if we can find an existing operation by slurm_job_id
        oid = None
        if params and 'slurm_job' in params:
            # Try to find existing operation with matching ensemble_id, operation_type, and slurm_job
            c.execute("""
              SELECT o.id FROM operations o
              JOIN operation_parameters op ON o.id = op.operation_id
              WHERE o.ensemble_id = ? AND o.operation_type = ? AND op.name = 'slurm_job' AND op.value = ?
            """, (ensemble_id, operation_type, params['slurm_job']))
            result = c.fetchone()
            if result:
                oid = result[0]
                # Update existing operation
                c.execute("""
                  UPDATE operations
                     SET status=?, update_time=?, user=?
                   WHERE id=?
                """, (status, now, user, oid))
                created = False
                msg = "Updated"
        
        if oid is None:
            # — INSERT brand‐new operation —
            c.execute("""
              INSERT INTO operations
                (ensemble_id,operation_type,status,creation_time,update_time,user)
              VALUES (?,?,?,?,?,?)
            """, (ensemble_id, operation_type, status, now, now, user))
            oid = c.lastrowid
            created = True
            msg = "Created"

    # stash *all* extra k/v pairs in operation_parameters
    if params:
        for name, val in params.items():
            c.execute("""
              INSERT OR REPLACE INTO operation_parameters
                (operation_id,name,value) VALUES (?,?,?)
            """, (oid, name, str(val)))

    conn.commit()
    conn.close()
    return oid, created, msg


# -----------------------------------------------------------------------------
@retry_db()
def remove_ensemble(db_file, ensemble_id):
    """
    Delete an ensemble and all its parameters and operations.
    """
    conn = get_connection(db_file)
    c = conn.cursor()
    conn.execute("BEGIN")
    c.execute("DELETE FROM operation_parameters WHERE operation_id IN "
              "(SELECT id FROM operations WHERE ensemble_id=?)",
              (ensemble_id,))
    c.execute("DELETE FROM operations WHERE ensemble_id=?", (ensemble_id,))
    c.execute("DELETE FROM ensemble_parameters WHERE ensemble_id=?", (ensemble_id,))
    c.execute("DELETE FROM ensembles WHERE id=?", (ensemble_id,))
    conn.commit()
    conn.close()
    return True


# -----------------------------------------------------------------------------
@retry_db()
def update_ensemble(db_file, ensemble_id, *, status=None, directory=None):
    """
    Update the ensemble.status and/or ensemble.directory fields.
    """
    if status is None and directory is None:
        return False

    conn = get_connection(db_file)
    c = conn.cursor()
    parts = []
    vals  = []

    if status:
        parts.append("status=?")
        vals.append(status)
    if directory:
        parts.append("directory=?")
        vals.append(directory)

    vals.append(ensemble_id)
    c.execute(f"UPDATE ensembles SET {','.join(parts)} WHERE id=?", vals)
    conn.commit()
    conn.close()
    return True


# -----------------------------------------------------------------------------
@retry_db()
def set_ensemble_parameter(db_file, ensemble_id, name, value):
    """
    Set a dynamic ensemble parameter (INSERT OR REPLACE semantics).
    """
    conn = get_connection(db_file)
    c = conn.cursor()
    c.execute(
        """
        INSERT OR REPLACE INTO ensemble_parameters (ensemble_id, name, value)
        VALUES (?, ?, ?)
        """,
        (ensemble_id, name, str(value))
    )
    conn.commit()
    conn.close()
    return True


# -----------------------------------------------------------------------------
@retry_db()
def delete_ensemble_parameter(db_file, ensemble_id, name):
    """
    Delete a dynamic ensemble parameter by name for a given ensemble.
    """
    conn = get_connection(db_file)
    c = conn.cursor()
    c.execute(
        "DELETE FROM ensemble_parameters WHERE ensemble_id=? AND name=?",
        (ensemble_id, name)
    )
    conn.commit()
    conn.close()
    return True


# -----------------------------------------------------------------------------
def set_configuration_range(db_file, ensemble_id,
                            first: int = None,
                            last: int = None,
                            increment: int = None,
                            total: int = None) -> bool:
    """
    Convenience helper to set one or more configuration range fields for an ensemble
    using the dynamic ensemble_parameters table. Any None values are skipped.

    Stored keys:
      - cfg_first
      - cfg_last
      - cfg_increment
      - cfg_total
    """
    if first is None and last is None and increment is None and total is None:
        return False

    # Reuse the existing setter to avoid duplicating transaction logic.
    if first is not None:
        set_ensemble_parameter(db_file, ensemble_id, 'cfg_first', str(first))
    if last is not None:
        set_ensemble_parameter(db_file, ensemble_id, 'cfg_last', str(last))
    if increment is not None:
        set_ensemble_parameter(db_file, ensemble_id, 'cfg_increment', str(increment))
    if total is not None:
        set_ensemble_parameter(db_file, ensemble_id, 'cfg_total', str(total))
    return True


# -----------------------------------------------------------------------------
def get_configuration_range(db_file, ensemble_id) -> dict:
    """
    Fetch configuration range fields for an ensemble. Returns a dict with keys
    'first', 'last', 'increment', 'total' if present (otherwise missing).
    """
    conn = get_connection(db_file)
    c = conn.cursor()
    c.execute(
        """
        SELECT name, value FROM ensemble_parameters
         WHERE ensemble_id=? AND name IN ('cfg_first','cfg_last','cfg_increment','cfg_total')
        """,
        (ensemble_id,)
    )
    rows = c.fetchall()
    conn.close()
    out = {}
    keymap = {
        'cfg_first': 'first',
        'cfg_last': 'last',
        'cfg_increment': 'increment',
        'cfg_total': 'total'
    }
    for name, val in rows:
        out[keymap.get(name, name)] = val
    return out

# -----------------------------------------------------------------------------
def get_ensemble_id_by_nickname(db_file, nickname):
    """
    Look up ensemble ID by nickname stored in ensemble_parameters.
    Returns int ID or None if not found.
    """
    conn = sqlite3.connect(db_file)
    cur  = conn.cursor()
    try:
        cur.execute(
            """
            SELECT ensemble_id FROM ensemble_parameters
             WHERE name='nickname' AND value=?
             LIMIT 1
            """,
            (nickname,)
        )
        row = cur.fetchone()
    except sqlite3.OperationalError:
        # Legacy DB without ensemble_parameters table
        row = None
    finally:
        conn.close()
    return row[0] if row else None


# -----------------------------------------------------------------------------
@retry_db()
def print_history(db_file, ensemble_id):
    """
    Pretty‐print all operations (with their parameters) for one ensemble.
    """
    conn = get_connection(db_file)
    c = conn.cursor()
    c.execute("""
      SELECT id,operation_type,status,creation_time,update_time,user
        FROM operations
       WHERE ensemble_id=?
    ORDER BY creation_time, id
    """, (ensemble_id,))
    rows = c.fetchall()
    if not rows:
        print("No operations recorded")
        conn.close()
        return

    for oid, op, st, ct, ut, user in rows:
        print(f"Op {oid}: {op} [{st}]")
        print(f"  Created: {ct} (by {user})")
        print(f"  Updated: {ut}")

        # dump every param for this operation
        c2 = conn.cursor()
        c2.execute("""
          SELECT name,value FROM operation_parameters
           WHERE operation_id=?
           ORDER BY name
        """, (oid,))
        for name, val in c2.fetchall():
            print(f"    {name} = {val}")

    conn.close()


# -----------------------------------------------------------------------------
@retry_db()
def clear_ensemble_history(db_file, ensemble_id):
    """
    Delete all operations and their parameters for a specific ensemble.
    The ensemble itself remains untouched.
    Returns (deleted_count, success).
    """
    conn = get_connection(db_file)
    c = conn.cursor()
    
    # Check if ensemble exists
    c.execute("SELECT 1 FROM ensembles WHERE id=?", (ensemble_id,))
    if not c.fetchone():
        conn.close()
        return 0, False
    
    # Count operations before deletion
    c.execute("SELECT COUNT(*) FROM operations WHERE ensemble_id=?", (ensemble_id,))
    op_count = c.fetchone()[0]
    
    # Delete all operations and their parameters for this ensemble
    conn.execute("BEGIN")
    c.execute("""
        DELETE FROM operation_parameters 
        WHERE operation_id IN (SELECT id FROM operations WHERE ensemble_id=?)
    """, (ensemble_id,))
    c.execute("DELETE FROM operations WHERE ensemble_id=?", (ensemble_id,))
    conn.commit()
    conn.close()
    
    return op_count, True


# -----------------------------------------------------------------------------
def get_ensemble_id_by_directory(db_file, directory):
    """
    Look up the ensemble ID in the DB given the exact directory path.
    Returns int ID or None if not found.
    """
    conn = sqlite3.connect(db_file)
    cur  = conn.cursor()
    cur.execute("SELECT id FROM ensembles WHERE directory = ?", (directory,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


# -----------------------------------------------------------------------------
@retry_db()
def resolve_ensemble_identifier(db_file, identifier):
    """
    Resolve an ensemble identifier that can be either:
    1. An integer ensemble ID
    2. A string path to an ensemble directory
    
    Returns (ensemble_id, ensemble_details) tuple, or (None, None) if not found.
    
    Args:
        db_file: Path to the database file
        identifier: Either an integer ID or a string path
    
    Returns:
        tuple: (ensemble_id, ensemble_details) or (None, None)
    """
    from pathlib import Path
    
    # If it's already an integer, treat as ensemble ID
    if isinstance(identifier, int):
        ensemble_id = identifier
    else:
        # Try to parse as integer first
        try:
            ensemble_id = int(identifier)
        except (ValueError, TypeError):
            # Not an integer; try as filesystem path first
            try:
                abs_path = str(Path(identifier).resolve())
                ensemble_id = get_ensemble_id_by_directory(db_file, abs_path)
                if ensemble_id is None:
                    ensemble_id = get_ensemble_id_by_directory(db_file, identifier)
            except Exception:
                ensemble_id = None

            # If not found as a path, try resolving as a nickname
            if ensemble_id is None and isinstance(identifier, str):
                nid = get_ensemble_id_by_nickname(db_file, identifier)
                if nid is not None:
                    ensemble_id = nid
    
    if ensemble_id is None:
        return None, None
    
    # Get ensemble details to verify it exists
    ensemble_details = get_ensemble_details(db_file, ensemble_id)
    if ensemble_details is None:
        return None, None
    
    return ensemble_id, ensemble_details


class SQLiteBackend(DatabaseBackend):
    """Thin adapter around the legacy SQLite helper functions (read-only)."""

    def __init__(self, connection_string: str):
        super().__init__(connection_string)
        self.db_file = connection_string

    # The methods below provide minimal compatibility primarily for migration
    # and read-only access. Mutation methods raise NotImplementedError because
    # the modern workflow is MongoDB-based.

    def add_ensemble(self, directory: str, physics: dict, **kwargs) -> int:
        raise NotImplementedError("SQLite backend is read-only for migration")

    def get_ensemble(self, ensemble_id: int):
        return get_ensemble_details(self.db_file, ensemble_id)

    def resolve_ensemble_identifier(self, identifier):
        eid, data = resolve_ensemble_identifier(self.db_file, identifier)
        if eid is None:
            raise ValueError(f"Ensemble not found: {identifier}")
        return eid, data

    def update_ensemble(self, ensemble_id: int, **updates) -> bool:
        raise NotImplementedError("SQLite backend is read-only for migration")

    def list_ensembles(self, detailed: bool = False):
        raise NotImplementedError("SQLite backend is read-only for migration")

    def delete_ensemble(self, ensemble_id: int) -> bool:
        raise NotImplementedError("SQLite backend is read-only for migration")

    def get_default_params(self, ensemble_id: int, job_type: str, variant: str):
        raise NotImplementedError("Default params stored outside SQLite backend")

    def set_default_params(
        self,
        ensemble_id: int,
        job_type: str,
        variant: str,
        input_params: str,
        job_params: str,
    ) -> bool:
        raise NotImplementedError("Default params stored outside SQLite backend")

    def delete_default_params(self, ensemble_id: int, job_type: str, variant: str) -> bool:
        raise NotImplementedError("Default params stored outside SQLite backend")

    def add_operation(
        self,
        ensemble_id: int,
        operation_type: str,
        status: str,
        user: str,
        **params,
    ) -> int:
        raise NotImplementedError("SQLite backend is read-only for migration")

    def update_operation_by_id(self, operation_id: int, status: str, **updates) -> bool:
        raise NotImplementedError("SQLite backend is read-only for migration")

    def update_operation_by_slurm_id(
        self, slurm_job_id: str, status: str, ensemble_id: int, operation_type: str, **updates
    ) -> bool:
        raise NotImplementedError("SQLite backend is read-only for migration")

    def clear_ensemble_history(self, ensemble_id: int) -> int:
        raise NotImplementedError("SQLite backend is read-only for migration")

    def list_operations(self, ensemble_id: int):
        raise NotImplementedError("SQLite backend is read-only for migration")

    def add_measurement(
        self,
        ensemble_id: int,
        config_number: int,
        measurement_type: str,
        data: dict,
        metadata=None,
    ) -> str:
        raise NotImplementedError("SQLite backend is read-only for migration")

    def query_measurements(
        self,
        ensemble_id: int,
        measurement_type: str,
        config_start: int | None = None,
        config_end: int | None = None,
    ):
        raise NotImplementedError("SQLite backend is read-only for migration")
