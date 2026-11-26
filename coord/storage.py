"""Persistent coordination service for out-of-order, retry-prone downstream calls.

Design contract
---------------
1. A business *operation* (task) owns a strictly increasing ``attempt_no`` fence.
   Only a callback whose ``attempt_no`` equals the task's current attempt may
   move the task to a terminal state; stale callbacks from older attempts are
   recorded but never overwrite newer state ("old success must not overwrite a
   newer failure").
2. A local timeout is *not* a verdict.  The attempt goes to TIMEOUT and a
   reentrant reconciliation (reconcile) runs: if the downstream actually
   applied the effect, an idempotent compensation is issued; only a confirmed
   downstream failure allows a new attempt.
3. Callbacks may carry incremental detail fragments (ordered by seq,
   de-duplicated); the terminal callback carries a checksum and the task only
   converges once every fragment is present and the checksum verifies.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS task (
    id              TEXT PRIMARY KEY,
    op_type         TEXT NOT NULL,
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL,
    current_attempt INTEGER NOT NULL DEFAULT 0,
    version         INTEGER NOT NULL DEFAULT 0,
    detail_checksum TEXT,
    detail_n        INTEGER,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS attempt (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id           TEXT NOT NULL REFERENCES task(id),
    attempt_no        INTEGER NOT NULL,
    status            TEXT NOT NULL,
    downstream_ref    TEXT,
    timeout_at        REAL,
    error_code        TEXT,
    created_at        REAL NOT NULL,
    updated_at        REAL NOT NULL,
    UNIQUE(task_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS callback_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id      TEXT NOT NULL,
    attempt_no   INTEGER NOT NULL,
    callback_id  TEXT NOT NULL,
    kind         TEXT NOT NULL,
    status       TEXT NOT NULL,
    is_stale     INTEGER NOT NULL DEFAULT 0,
    accepted     INTEGER NOT NULL,
    raw          TEXT NOT NULL,
    received_at  REAL NOT NULL,
    UNIQUE(attempt_no, callback_id)
);

CREATE TABLE IF NOT EXISTS detail_fragment (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id    TEXT NOT NULL REFERENCES task(id),
    attempt_no INTEGER NOT NULL,
    seq        INTEGER NOT NULL,
    payload    TEXT NOT NULL,
    received_at REAL NOT NULL,
    UNIQUE(task_id, attempt_no, seq)
);

CREATE TABLE IF NOT EXISTS compensation (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id       TEXT NOT NULL,
    attempt_no    INTEGER NOT NULL,
    idem_key      TEXT NOT NULL UNIQUE,
    status        TEXT NOT NULL,
    attempts_done INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS reconcile_job (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     TEXT NOT NULL,
    attempt_no  INTEGER NOT NULL,
    kind        TEXT NOT NULL,           -- 'timeout'
    status      TEXT NOT NULL,           -- PENDING / RUNNING / DONE
    runs        INTEGER NOT NULL DEFAULT 0,
    last_error  TEXT,
    due_at      REAL NOT NULL,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL,
    UNIQUE(task_id, attempt_no, kind)
);

CREATE INDEX IF NOT EXISTS idx_attempt_task ON attempt(task_id);
CREATE INDEX IF NOT EXISTS idx_job_claim ON reconcile_job(status, due_at);
CREATE INDEX IF NOT EXISTS idx_frag_task ON detail_fragment(task_id);
"""


class Storage:
    """SQLite-backed storage.

    Each thread opens its own connection (check_same_thread=False is avoided on
    purpose).  Writes run in BEGIN IMMEDIATE transactions so concurrent writers
    serialize and fencing updates cannot interleave.
    """

    def __init__(self, path: str):
        self.path = path
        # A lock per logical database; sqlite itself serializes writers, the
        # lock keeps multi-statement jobs atomic from the Python side as well.
        self._writelock = threading.RLock()
        self._tls = threading.local()
        self._init()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path, timeout=30, isolation_level=None, cached_statements=32
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    @property
    def conn(self) -> sqlite3.Connection:
        c = getattr(self._tls, "conn", None)
        if c is None:
            c = self._connect()
            self._tls.conn = c
        return c

    def _init(self) -> None:
        with self._writelock:
            self.conn.executescript(SCHEMA)

    @contextmanager
    def write_tx(self) -> Iterator[sqlite3.Connection]:
        """Serialized immediate-write transaction."""
        conn = self.conn
        with self._writelock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    @contextmanager
    def read_tx(self) -> Iterator[sqlite3.Connection]:
        conn = self.conn
        conn.execute("BEGIN")
        try:
            yield conn
        finally:
            conn.execute("COMMIT")

    # ---- generic helpers -------------------------------------------------

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM task WHERE id=?", (task_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_attempt(self, task_id: str, attempt_no: int) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM attempt WHERE task_id=? AND attempt_no=?",
            (task_id, attempt_no),
        ).fetchone()
        return dict(row) if row else None

    def list_attempts(self, task_id: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM attempt WHERE task_id=? ORDER BY attempt_no", (task_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    def list_fragments(self, task_id: str, attempt_no: int) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM detail_fragment WHERE task_id=? AND attempt_no=?"
            " ORDER BY seq",
            (task_id, attempt_no),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_compensation(self, idem_key: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM compensation WHERE idem_key=?", (idem_key,)
        ).fetchone()
        return dict(row) if row else None

    def list_callbacks(self, task_id: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM callback_log WHERE task_id=? ORDER BY id", (task_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    def dump_state(self, task_id: str) -> dict[str, Any]:
        with self.read_tx():
            task = self.get_task(task_id)
            if not task:
                return {}
            return {
                "task": task,
                "attempts": self.list_attempts(task_id),
                "fragments": self.list_fragments(task_id, task["current_attempt"]),
                "callbacks": self.list_callbacks(task_id),
                "compensations": [
                    dict(r)
                    for r in self.conn.execute(
                        "SELECT * FROM compensation WHERE task_id=? ORDER BY id",
                        (task_id,),
                    ).fetchall()
                ],
                "reconcile_jobs": [
                    dict(r)
                    for r in self.conn.execute(
                        "SELECT * FROM reconcile_job WHERE task_id=? ORDER BY id",
                        (task_id,),
                    ).fetchall()
                ],
            }


def dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)
