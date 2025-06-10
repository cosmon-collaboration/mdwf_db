import sqlite3
import datetime
import time
from pathlib import Path
from functools import wraps

# -----------------------------------------------------------------------------
def get_connection(db_file, timeout=30.0):
    """
    Open sqlite3 in IMMEDIATE mode, enable WAL, and set a busy timeout.
    """
    conn = sqlite3.connect(db_file, timeout=timeout)
    conn.isolation_level = "IMMEDIATE"
    conn.execute("PRAGMA journal_mode = WAL;")
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
                     operation_id=None, params=None):
    """
    Insert a new operation or update an existing one.
    All extra fields (config ranges, exit_code, runtime, etc.)
    must be provided in the `params` dict.
    Returns (op_id, created_flag, message).
    """
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
             SET status=?, update_time=?
           WHERE id=?
        """, (status, now, oid))
        created = False
        msg = "Updated"
    else:
        # — INSERT brand‐new operation —
        c.execute("""
          INSERT INTO operations
            (ensemble_id,operation_type,status,creation_time,update_time)
          VALUES (?,?,?,?,?)
        """, (ensemble_id, operation_type, status, now, now))
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
def print_history(db_file, ensemble_id):
    """
    Pretty‐print all operations (with their parameters) for one ensemble.
    """
    conn = get_connection(db_file)
    c = conn.cursor()
    c.execute("""
      SELECT id,operation_type,status,creation_time,update_time
        FROM operations
       WHERE ensemble_id=?
    ORDER BY creation_time, id
    """, (ensemble_id,))
    rows = c.fetchall()
    if not rows:
        print("No operations recorded")
        conn.close()
        return

    for oid, op, st, ct, ut in rows:
        print(f"Op {oid}: {op} [{st}]")
        print(f"  Created: {ct}")
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