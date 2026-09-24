"""SQLite bookkeeping database (controller side only).

Only the controller (login node) writes this database. Workers never touch it:
they write a JSON summary per attempt, which `sync` ingests. This avoids
concurrent SQLite writes over a network filesystem.

Tables
------
meta        key/value campaign metadata
tasks       one row per (stage, task_id): job range, parent range, status
attempts    one row per submission of a task (attempt = 1, 2, ...)
submissions one row per slurm array
files       one row per output file registered from a successful attempt
events      (job_id, event_id) -> file, for every event in every output file
"""

import sqlite3
import time

SCHEMA_VERSION = 1

# Task status
NEW = "new"              # defined, never submitted
SUBMITTED = "submitted"  # in the scheduler queue
RUNNING = "running"
DONE = "done"
FAILED = "failed"        # latest attempt failed; eligible for recovery
ABANDONED = "abandoned"  # given up on (manually); downstream proceeds without it

TASK_STATES = (NEW, SUBMITTED, RUNNING, DONE, FAILED, ABANDONED)
ACTIVE_STATES = (SUBMITTED, RUNNING)
TERMINAL_STATES = (DONE, ABANDONED)

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
CREATE TABLE IF NOT EXISTS tasks (
    stage        TEXT    NOT NULL,
    task_id      INTEGER NOT NULL,
    first_job    INTEGER NOT NULL,
    last_job     INTEGER NOT NULL,
    first_parent INTEGER,            -- parent-stage task range (NULL for root)
    last_parent  INTEGER,
    status       TEXT    NOT NULL DEFAULT 'new',
    n_attempts   INTEGER NOT NULL DEFAULT 0,
    n_events     INTEGER,
    note         TEXT,
    updated      REAL,
    PRIMARY KEY (stage, task_id)
);
CREATE INDEX IF NOT EXISTS tasks_status ON tasks (stage, status);
CREATE INDEX IF NOT EXISTS tasks_jobs   ON tasks (stage, first_job, last_job);

CREATE TABLE IF NOT EXISTS submissions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    stage         TEXT NOT NULL,
    kind          TEXT NOT NULL,     -- normal | recovery
    site          TEXT NOT NULL,
    array_job_id  TEXT,
    n_tasks       INTEGER NOT NULL,
    manifest      TEXT NOT NULL,
    script        TEXT NOT NULL,
    submit_time   REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS attempts (
    stage         TEXT    NOT NULL,
    task_id       INTEGER NOT NULL,
    attempt       INTEGER NOT NULL,
    submission_id INTEGER NOT NULL,
    array_index   INTEGER NOT NULL,
    sched_state   TEXT,              -- raw scheduler state (PENDING, RUNNING, ...)
    state         TEXT NOT NULL,     -- submitted | running | done | failed | cancelled
    exit_code     TEXT,
    node          TEXT,
    start_time    REAL,
    end_time      REAL,
    elapsed_s     REAL,              -- scheduler wall time
    wall_s        REAL,              -- worker-measured wall time
    max_rss_mb    REAL,              -- worker-measured peak memory
    avg_rss_mb    REAL,              -- time-averaged RSS of the job's process tree
    gpu_util_pct  REAL,              -- time-averaged GPU utilization (GPU jobs)
    gpu_mem_used_mb REAL,            -- time-averaged GPU memory used (GPU jobs)
    gpu_mem_max_mb REAL,             -- max sampled GPU memory used (GPU jobs)
    uncounted     INTEGER DEFAULT 0, -- 1: not counted toward max_attempts (dprod reset-attempts)
    reason        TEXT,
    summary_path  TEXT,
    PRIMARY KEY (stage, task_id, attempt)
);
CREATE INDEX IF NOT EXISTS attempts_state ON attempts (state);
CREATE INDEX IF NOT EXISTS attempts_sub   ON attempts (submission_id);

CREATE TABLE IF NOT EXISTS files (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    stage     TEXT    NOT NULL,
    task_id   INTEGER NOT NULL,
    role      TEXT    NOT NULL,      -- e.g. edepsim, sensor, step, hits
    path      TEXT    NOT NULL UNIQUE,
    size      INTEGER,
    n_events  INTEGER,
    first_job INTEGER,
    last_job  INTEGER
);
CREATE INDEX IF NOT EXISTS files_task ON files (stage, task_id);

CREATE TABLE IF NOT EXISTS events (
    file_id  INTEGER NOT NULL,
    job_id   INTEGER NOT NULL,
    event_id INTEGER NOT NULL,
    PRIMARY KEY (file_id, job_id, event_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS events_id ON events (job_id, event_id);
"""


# columns added after schema version 1 was first deployed: (table, column, type)
MIGRATIONS = (("attempts", "max_rss_mb", "REAL"), ("attempts", "avg_rss_mb", "REAL"),
              ("attempts", "gpu_util_pct", "REAL"), ("attempts", "gpu_mem_used_mb", "REAL"),
              ("attempts", "gpu_mem_max_mb", "REAL"),
              ("attempts", "uncounted", "INTEGER DEFAULT 0"))


def connect(path):
    con = sqlite3.connect(path, timeout=60)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    for table, col, typ in MIGRATIONS:
        cols = [r["name"] for r in con.execute("PRAGMA table_info(%s)" % table)]
        if cols and col not in cols:
            with con:
                con.execute("ALTER TABLE %s ADD COLUMN %s %s" % (table, col, typ))
    return con


def create(path, meta):
    con = connect(path)
    with con:
        con.executescript(SCHEMA)
        set_meta(con, "schema_version", SCHEMA_VERSION)
        for k, v in meta.items():
            set_meta(con, k, v)
    return con


def set_meta(con, key, value):
    con.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, str(value)))


def get_meta(con, key, default=None):
    row = con.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def now():
    return time.time()


def add_tasks(con, stage, rows):
    """rows: iterable of (task_id, first_job, last_job, first_parent, last_parent)."""
    t = now()
    con.executemany(
        "INSERT INTO tasks (stage, task_id, first_job, last_job, first_parent, last_parent,"
        " status, updated) VALUES (?, ?, ?, ?, ?, ?, 'new', ?)",
        [(stage,) + tuple(r) + (t,) for r in rows])


def set_task_status(con, stage, task_id, status, note=None, n_events=None):
    con.execute(
        "UPDATE tasks SET status = ?, note = COALESCE(?, note),"
        " n_events = COALESCE(?, n_events), updated = ? WHERE stage = ? AND task_id = ?",
        (status, note, n_events, now(), stage, task_id))


def tasks(con, stage, statuses=None, task_ids=None):
    q = "SELECT * FROM tasks WHERE stage = ?"
    args = [stage]
    if statuses:
        q += " AND status IN (%s)" % ",".join("?" * len(statuses))
        args += list(statuses)
    q += " ORDER BY task_id"
    rows = con.execute(q, args).fetchall()
    if task_ids is not None:
        wanted = set(task_ids)
        rows = [r for r in rows if r["task_id"] in wanted]
    return rows


def replace_task_outputs(con, stage, task_id, files):
    """Register a successful attempt's output files and their events.

    files: list of dicts {path, role, size, events: {job_id: [event_id, ...]}}
    Any previously registered outputs of the task are replaced.
    """
    old = [r["id"] for r in con.execute(
        "SELECT id FROM files WHERE stage = ? AND task_id = ?", (stage, task_id))]
    if old:
        marks = ",".join("?" * len(old))
        con.execute("DELETE FROM events WHERE file_id IN (%s)" % marks, old)
        con.execute("DELETE FROM files WHERE id IN (%s)" % marks, old)
    for f in files:
        evs = [(int(j), int(e)) for j, elist in f["events"].items() for e in elist]
        jobs = [j for j, _ in evs]
        cur = con.execute(
            "INSERT INTO files (stage, task_id, role, path, size, n_events, first_job, last_job)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (stage, task_id, f["role"], f["path"], f.get("size"), len(evs),
             min(jobs) if jobs else None, max(jobs) if jobs else None))
        fid = cur.lastrowid
        con.executemany("INSERT OR IGNORE INTO events (file_id, job_id, event_id) VALUES (?, ?, ?)",
                        [(fid, j, e) for j, e in evs])


def task_files(con, stage, task_id):
    return con.execute("SELECT * FROM files WHERE stage = ? AND task_id = ? ORDER BY role, path",
                       (stage, task_id)).fetchall()


def lookup_event(con, job_id, event_id, stage=None):
    q = ("SELECT f.stage, f.role, f.path, f.task_id FROM events e JOIN files f ON f.id = e.file_id"
         " WHERE e.job_id = ? AND e.event_id = ?")
    args = [job_id, event_id]
    if stage:
        q += " AND f.stage = ?"
        args.append(stage)
    return con.execute(q + " ORDER BY f.stage, f.role", args).fetchall()
