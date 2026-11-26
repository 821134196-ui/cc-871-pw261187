"""Coordinator: the state machine that fences attempts and drives compensation.

All public mutating methods are concurrency-safe: they run inside
``BEGIN IMMEDIATE`` transactions and every state transition is guarded by the
task's ``current_attempt`` fence and a monotonically increasing row ``version``.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Protocol

from .models import (
    AttemptStatus,
    CallbackResult,
    CompStatus,
    JobStatus,
    TaskStatus,
    TERMINAL_TASK,
)
from .storage import Storage, dumps


class DownstreamGateway(Protocol):
    """Contract a real downstream adapter must implement.

    Every method must be safe to call repeatedly (idempotent / reentrant):
    the coordinator may call them again after a local timeout or a crash.
    """

    def dispatch(self, task_id: str, attempt_no: int, payload: dict) -> str:
        """Send the operation. Returns the downstream reference id."""

    def query_status(self, task_id: str, attempt_no: int) -> str:
        """Reconcile after a local timeout.

        Returns one of: APPLIED / NOT_APPLIED / UNKNOWN.
        """

    def compensate(self, task_id: str, attempt_no: int, idem_key: str) -> bool:
        """Idempotent reverse operation. True once the effect is undone."""

    def fetch_detail(self, task_id: str, attempt_no: int) -> dict:
        """Re-fetch the full detail envelope for checksum verification."""


@dataclass
class CallbackEvent:
    task_id: str
    attempt_no: int
    callback_id: str
    status: str                      # SUCCESS / FAILED / PROGRESS
    detail_n: int | None = None     # total fragments declared by downstream
    checksum: str | None = None     # sha256 over the concatenated fragments
    fragment_seq: int | None = None
    fragment: Any = None
    error_code: str | None = None
    downstream_ref: str | None = None
    raw: dict = field(default_factory=dict)


def _now() -> float:
    return time.time()


def detail_checksum(fragments: list[Any]) -> str:
    h = hashlib.sha256()
    for frag in fragments:
        h.update(dumps(frag).encode("utf-8"))
        h.update(b"\x1e")
    return h.hexdigest()


class Coordinator:
    def __init__(
        self,
        storage: Storage,
        gateway: DownstreamGateway,
        *,
        timeout_seconds: float = 2.0,
        max_attempts: int = 3,
        max_reconcile_runs: int = 5,
        max_comp_attempts: int = 5,
    ):
        self.db = storage
        self.gw = gateway
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.max_reconcile_runs = max_reconcile_runs
        self.max_comp_attempts = max_comp_attempts

    # ------------------------------------------------------------------ #
    # Task creation / dispatch
    # ------------------------------------------------------------------ #

    def create_task(self, task_id: str, op_type: str, payload: dict) -> dict:
        """Create a task and dispatch attempt #1 through the gateway.

        Dispatch happens *before* the state is committed: if the process dies
        between the two, the reconcile sweeper still picks the attempt up at
        its timeout and discovers whether the downstream applied it.
        """
        now = _now()
        ref = self.gw.dispatch(task_id, 1, payload)
        with self.db.write_tx() as conn:
            existing = conn.execute(
                "SELECT id FROM task WHERE id=?", (task_id,)
            ).fetchone()
            if existing:
                return dict(
                    conn.execute(
                        "SELECT * FROM task WHERE id=?", (task_id,)
                    ).fetchone()
                )
            conn.execute(
                "INSERT INTO task(id, op_type, payload, status, current_attempt,"
                " version, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?)",
                (
                    task_id, op_type, dumps(payload), TaskStatus.IN_FLIGHT.value,
                    1, 1, now, now,
                ),
            )
            conn.execute(
                "INSERT INTO attempt(task_id, attempt_no, status, downstream_ref,"
                " timeout_at, created_at, updated_at) VALUES(?,?,?,?,?,?,?)",
                (
                    task_id, 1, AttemptStatus.DISPATCHED.value, ref,
                    now + self.timeout_seconds, now, now,
                ),
            )
        return self.db.get_task(task_id)

    def _dispatch_new_attempt(self, conn, task_id: str, reason: str) -> int:
        """Must run inside a write transaction. Returns new attempt_no."""
        task = conn.execute("SELECT * FROM task WHERE id=?", (task_id,)).fetchone()
        next_no = task["current_attempt"] + 1
        now = _now()
        conn.execute(
            "UPDATE task SET status=?, current_attempt=?, version=version+1,"
            " updated_at=? WHERE id=?",
            (TaskStatus.IN_FLIGHT.value, next_no, now, task_id),
        )
        conn.execute(
            "INSERT INTO attempt(task_id, attempt_no, status, timeout_at,"
            " created_at, updated_at) VALUES(?,?,?,?,?,?)",
            (
                task_id, next_no, AttemptStatus.DISPATCHED.value,
                now + self.timeout_seconds, now, now,
            ),
        )
        return next_no

    # ------------------------------------------------------------------ #
    # Callback intake — the hot path
    # ------------------------------------------------------------------ #

    def record_callback(self, ev: CallbackEvent) -> tuple[CallbackResult, dict]:
        """Persist a callback and apply it if it belongs to the current attempt.

        Returns (result_code, task_snapshot).
        """
        now = _now()
        with self.db.write_tx() as conn:
            task = conn.execute(
                "SELECT * FROM task WHERE id=?", (ev.task_id,)
            ).fetchone()
            if task is None:
                return CallbackResult.UNKNOWN_TASK, {}

            # 1) idempotent intake: same callback_id on the same attempt is a
            #    redelivery, never applied twice.
            dup = conn.execute(
                "SELECT id FROM callback_log WHERE attempt_no=? AND callback_id=?",
                (ev.attempt_no, ev.callback_id),
            ).fetchone()
            if dup is not None:
                return CallbackResult.DUPLICATE, dict(task)

            attempt = conn.execute(
                "SELECT * FROM attempt WHERE task_id=? AND attempt_no=?",
                (ev.task_id, ev.attempt_no),
            ).fetchone()

            is_stale = ev.attempt_no < task["current_attempt"]
            accepted = 0
            result = CallbackResult.REJECTED

            # 2) persist any detail fragment first (deduplicated by seq), even
            #    for stale callbacks — the audit trail keeps everything.
            if ev.fragment_seq is not None:
                conn.execute(
                    "INSERT OR IGNORE INTO detail_fragment"
                    "(task_id, attempt_no, seq, payload, received_at)"
                    " VALUES(?,?,?,?,?)",
                    (
                        ev.task_id, ev.attempt_no, ev.fragment_seq,
                        dumps(ev.fragment), now,
                    ),
                )

            if is_stale or attempt is None:
                # 3) STALE FENCE: an older attempt can never move the task.
                #    Record the attempt's own terminal verdict for forensics,
                #    but the task state is untouched — this is the guarantee
                #    that an old success cannot overwrite a newer failure.
                if attempt is not None and ev.status in (
                    "SUCCESS", "FAILED"
                ) and attempt["status"] == AttemptStatus.DISPATCHED.value:
                    conn.execute(
                        "UPDATE attempt SET status=?, downstream_ref=COALESCE(?,"
                        " downstream_ref), updated_at=? WHERE id=?",
                        (
                            AttemptStatus[ev.status].value, ev.downstream_ref,
                            now, attempt["id"],
                        ),
                    )
                result = (
                    CallbackResult.STALE_SUCCESS_RECORDED
                    if ev.status == "SUCCESS"
                    else CallbackResult.STALE_IGNORED
                )
                conn.execute(
                    "INSERT INTO callback_log(task_id, attempt_no, callback_id,"
                    " kind, status, is_stale, accepted, raw, received_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        ev.task_id, ev.attempt_no, ev.callback_id,
                        "stale", ev.status, 1, 0, dumps(ev.raw), now,
                    ),
                )
                return result, dict(
                    conn.execute(
                        "SELECT * FROM task WHERE id=?", (ev.task_id,)
                    ).fetchone()
                )

            # 4) CURRENT ATTEMPT. Terminal tasks still log but never move.
            if task["status"] in {s.value for s in TERMINAL_TASK}:
                conn.execute(
                    "INSERT INTO callback_log(task_id, attempt_no, callback_id,"
                    " kind, status, is_stale, accepted, raw, received_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        ev.task_id, ev.attempt_no, ev.callback_id,
                        "late", ev.status, 0, 0, dumps(ev.raw), now,
                    ),
                )
                return CallbackResult.STALE_IGNORED, dict(task)

            if ev.status == "PROGRESS":
                accepted = 1
                result = CallbackResult.ACCEPTED
            elif ev.status == "SUCCESS":
                ok, reason = self._verify_detail(conn, ev, now)
                if ok:
                    conn.execute(
                        "UPDATE attempt SET status=?, downstream_ref=COALESCE(?,"
                        " downstream_ref), updated_at=? WHERE id=?",
                        (
                            AttemptStatus.SUCCESS.value, ev.downstream_ref,
                            now, attempt["id"],
                        ),
                    )
                    conn.execute(
                        "UPDATE task SET status=?, version=version+1,"
                        " detail_checksum=?, detail_n=?, updated_at=? WHERE id=?",
                        (
                            TaskStatus.SUCCESS.value, ev.checksum,
                            ev.detail_n, now, ev.task_id,
                        ),
                    )
                    accepted = 1
                    result = CallbackResult.ACCEPTED
                else:
                    # Terminal verdict arrived but details are incomplete or
                    # corrupt: hold instead of declaring success, and reconcile.
                    conn.execute(
                        "UPDATE attempt SET status=?, updated_at=? WHERE id=?",
                        (AttemptStatus.DETAIL_HELD.value, now, attempt["id"]),
                    )
                    conn.execute(
                        "UPDATE task SET status=?, version=version+1,"
                        " detail_checksum=?, detail_n=?, updated_at=? WHERE id=?",
                        (
                            TaskStatus.DETAIL_HELD.value, ev.checksum,
                            ev.detail_n, now, ev.task_id,
                        ),
                    )
                    self._enqueue_job(
                        conn, ev.task_id, ev.attempt_no, "detail", now
                    )
                    result = CallbackResult.DETAIL_HELD
            elif ev.status == "FAILED":
                conn.execute(
                    "UPDATE attempt SET status=?, error_code=COALESCE(?,"
                    " error_code), downstream_ref=COALESCE(?, downstream_ref),"
                    " updated_at=? WHERE id=?",
                    (
                        AttemptStatus.FAILED.value, ev.error_code,
                        ev.downstream_ref, now, attempt["id"],
                    ),
                )
                if task["current_attempt"] >= self.max_attempts:
                    conn.execute(
                        "UPDATE task SET status=?, version=version+1,"
                        " updated_at=? WHERE id=?",
                        (TaskStatus.FAILED.value, now, ev.task_id),
                    )
                else:
                    # Failure is confirmed by the downstream itself, so a fresh
                    # attempt is safe; the fence moves forward immediately.
                    self._dispatch_new_attempt(conn, ev.task_id, "callback-fail")
                accepted = 1
                result = CallbackResult.ACCEPTED
            else:
                result = CallbackResult.REJECTED

            conn.execute(
                "INSERT INTO callback_log(task_id, attempt_no, callback_id,"
                " kind, status, is_stale, accepted, raw, received_at)"
                " VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    ev.task_id, ev.attempt_no, ev.callback_id,
                    "current", ev.status, 0, accepted, dumps(ev.raw), now,
                ),
            )
            return result, dict(
                conn.execute(
                    "SELECT * FROM task WHERE id=?", (ev.task_id,)
                ).fetchone()
            )

    def _verify_detail(self, conn, ev: CallbackEvent, now: float) -> tuple[bool, str]:
        """Check fragments 0..n-1 are all present and match the checksum."""
        if ev.detail_n is None or ev.checksum is None:
            # Downstream opted out of detail verification.
            return True, "no-checksum"
        rows = conn.execute(
            "SELECT seq, payload FROM detail_fragment WHERE task_id=? AND"
            " attempt_no=? ORDER BY seq",
            (ev.task_id, ev.attempt_no),
        ).fetchall()
        if len(rows) != ev.detail_n:
            return False, f"want {ev.detail_n} fragments, have {len(rows)}"
        seqs = [r["seq"] for r in rows]
        if seqs != list(range(ev.detail_n)):
            return False, f"non-contiguous seqs: {seqs}"
        materialized = [json.loads(r["payload"]) for r in rows]
        if detail_checksum(materialized) != ev.checksum:
            return False, "checksum-mismatch"
        return True, "ok"

    # ------------------------------------------------------------------ #
    # Timeout sweep -> reentrant reconciliation
    # ------------------------------------------------------------------ #

    def sweep_timeouts(self) -> int:
        """Mark every timed-out, still-dispatched attempt and enqueue reconcile.

        A local timeout is never treated as failure or success: it only creates
        a reconcile job. Re-running the sweep is harmless (unique job key).
        """
        now = _now()
        enqueued = 0
        with self.db.write_tx() as conn:
            rows = conn.execute(
                "SELECT a.task_id, a.attempt_no FROM attempt a JOIN task t"
                " ON t.id=a.task_id WHERE a.status=? AND a.timeout_at<=?",
                (AttemptStatus.DISPATCHED.value, now),
            ).fetchall()
            for r in rows:
                conn.execute(
                    "UPDATE attempt SET status=?, updated_at=? WHERE task_id=?"
                    " AND attempt_no=?",
                    (AttemptStatus.TIMEOUT.value, now, r["task_id"], r["attempt_no"]),
                )
                conn.execute(
                    "UPDATE task SET status=?, version=version+1, updated_at=?"
                    " WHERE id=? AND status=?",
                    (
                        TaskStatus.TIMEOUT_PENDING.value, now, r["task_id"],
                        TaskStatus.IN_FLIGHT.value,
                    ),
                )
                self._enqueue_job(conn, r["task_id"], r["attempt_no"], "timeout", now)
                enqueued += 1
        return enqueued

    def _enqueue_job(self, conn, task_id: str, attempt_no: int, kind: str,
                     now: float) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO reconcile_job(task_id, attempt_no, kind,"
            " status, due_at, created_at, updated_at) VALUES(?,?,?,?,?,?,?)",
            (
                task_id, attempt_no, kind, JobStatus.PENDING.value, now, now, now,
            ),
        )

    def recover_stale_jobs(self) -> int:
        """Requeue RUNNING jobs abandoned by a crashed worker process.

        Compensation rows are PENDING-only (claim never mutates status), so
        they need no recovery; reconcile jobs are the only RUNNING state.
        """
        now = _now()
        with self.db.write_tx() as conn:
            cur = conn.execute(
                "UPDATE reconcile_job SET status=?, due_at=?, updated_at=?"
                " WHERE status=?",
                (JobStatus.PENDING.value, now, now, JobStatus.RUNNING.value),
            )
            return cur.rowcount

    def claim_job(self) -> dict | None:
        """Atomically claim one due reconcile job (reentrant after crashes)."""
        now = _now()
        with self.db.write_tx() as conn:
            row = conn.execute(
                "SELECT * FROM reconcile_job WHERE status=? AND due_at<=?"
                " ORDER BY id LIMIT 1",
                (JobStatus.PENDING.value, now),
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE reconcile_job SET status=?, updated_at=? WHERE id=?",
                (JobStatus.RUNNING.value, now, row["id"]),
            )
            return dict(row)

    def run_reconcile_job(self, job: dict) -> dict:
        """Execute one claimed reconcile job. Safe to retry the same job."""
        now = _now()
        task_id, attempt_no, kind = job["task_id"], job["attempt_no"], job["kind"]
        try:
            if kind == "timeout":
                outcome = self._reconcile_timeout(task_id, attempt_no)
            elif kind == "detail":
                outcome = self._reconcile_detail(task_id, attempt_no)
            else:
                outcome = f"unknown-kind:{kind}"
            with self.db.write_tx() as conn:
                conn.execute(
                    "UPDATE reconcile_job SET status=?, runs=runs+1, last_error=?,"
                    " updated_at=? WHERE id=?",
                    (JobStatus.DONE.value, None, now, job["id"]),
                )
            return {"job_id": job["id"], "outcome": outcome}
        except Exception as exc:  # gateway error etc. — requeue with backoff
            with self.db.write_tx() as conn:
                runs = job["runs"] + 1
                if runs >= self.max_reconcile_runs:
                    conn.execute(
                        "UPDATE reconcile_job SET status=?, runs=?, last_error=?,"
                        " updated_at=? WHERE id=?",
                        (JobStatus.DONE.value, runs, str(exc), now, job["id"]),
                    )
                    conn.execute(
                        "UPDATE task SET status=?, version=version+1, updated_at=?"
                        " WHERE id=?",
                        (TaskStatus.SUSPENDED.value, now, task_id),
                    )
                    return {"job_id": job["id"], "outcome": "suspended",
                            "error": str(exc)}
                backoff = min(30.0, 0.2 * (2 ** runs))
                conn.execute(
                    "UPDATE reconcile_job SET status=?, runs=?, last_error=?,"
                    " due_at=?, updated_at=? WHERE id=?",
                    (
                        JobStatus.PENDING.value, runs, str(exc), now + backoff,
                        now, job["id"],
                    ),
                )
            return {"job_id": job["id"], "outcome": "retried", "error": str(exc)}

    def _reconcile_timeout(self, task_id: str, attempt_no: int) -> str:
        verdict = self.gw.query_status(task_id, attempt_no)
        now = _now()
        with self.db.write_tx() as conn:
            task = conn.execute(
                "SELECT * FROM task WHERE id=?", (task_id,)
            ).fetchone()
            # A late callback may already have resolved this task.
            if task["status"] in {s.value for s in TERMINAL_TASK}:
                return "already-terminal"
            if verdict == "APPLIED":
                # Downstream effect exists although we timed out locally.
                # Never declare success blindly; undo via idempotent compensation.
                idem_key = f"comp:{task_id}:{attempt_no}"
                conn.execute(
                    "INSERT OR IGNORE INTO compensation(task_id, attempt_no,"
                    " idem_key, status, created_at, updated_at)"
                    " VALUES(?,?,?,?,?,?)",
                    (
                        task_id, attempt_no, idem_key, CompStatus.PENDING.value,
                        now, now,
                    ),
                )
                conn.execute(
                    "UPDATE task SET status=?, version=version+1, updated_at=?"
                    " WHERE id=?",
                    (TaskStatus.COMPENSATING.value, now, task_id),
                )
                return "applied->compensating"
            if verdict == "NOT_APPLIED":
                if task["current_attempt"] >= self.max_attempts:
                    conn.execute(
                        "UPDATE task SET status=?, version=version+1, updated_at=?"
                        " WHERE id=?",
                        (TaskStatus.FAILED.value, now, task_id),
                    )
                    return "not-applied->failed-exhausted"
                next_no = self._dispatch_new_attempt(conn, task_id, "reconcile")
                return f"not-applied->retry-{next_no}"
            # UNKNOWN: raise so the caller's exception path requeues with
            # backoff instead of closing the job out prematurely.
            raise RuntimeError("downstream status UNKNOWN")

    def _reconcile_detail(self, task_id: str, attempt_no: int) -> str:
        envelope = self.gw.fetch_detail(task_id, attempt_no)
        now = _now()
        with self.db.write_tx() as conn:
            task = conn.execute(
                "SELECT * FROM task WHERE id=?", (task_id,)
            ).fetchone()
            if task["status"] != TaskStatus.DETAIL_HELD.value:
                return "already-resolved"
            for i, frag in enumerate(envelope["fragments"]):
                conn.execute(
                    "INSERT OR IGNORE INTO detail_fragment(task_id, attempt_no,"
                    " seq, payload, received_at) VALUES(?,?,?,?,?)",
                    (task_id, attempt_no, i, dumps(frag), now),
                )
            n = len(envelope["fragments"])
            if envelope.get("checksum") != detail_checksum(envelope["fragments"]):
                raise RuntimeError("downstream detail still fails checksum")
            conn.execute(
                "UPDATE task SET status=?, detail_checksum=?, detail_n=?,"
                " version=version+1, updated_at=? WHERE id=?",
                (
                    TaskStatus.SUCCESS.value, envelope["checksum"], n, now, task_id,
                ),
            )
            conn.execute(
                "UPDATE attempt SET status=?, updated_at=? WHERE task_id=?"
                " AND attempt_no=?",
                (AttemptStatus.SUCCESS.value, now, task_id, attempt_no),
            )
            return "detail-repaired->success"

    # ------------------------------------------------------------------ #
    # Reentrant compensation
    # ------------------------------------------------------------------ #

    def claim_compensation(self) -> dict | None:
        now = _now()
        with self.db.write_tx() as conn:
            row = conn.execute(
                "SELECT * FROM compensation WHERE status=? ORDER BY id LIMIT 1",
                (CompStatus.PENDING.value,),
            ).fetchone()
            if row is None:
                return None
            return dict(row)

    def run_compensation(self, comp: dict) -> dict:
        """Invoke the idempotent undo. Reentrant: same idem_key every retry."""
        now = _now()
        try:
            done = self.gw.compensate(
                comp["task_id"], comp["attempt_no"], comp["idem_key"]
            )
        except Exception as exc:
            done = False
            err = str(exc)
        else:
            err = None
        with self.db.write_tx() as conn:
            row = conn.execute(
                "SELECT * FROM compensation WHERE id=?", (comp["id"],)
            ).fetchone()
            attempts = row["attempts_done"] + 1
            if done:
                conn.execute(
                    "UPDATE compensation SET status=?, attempts_done=?, last_error=?,"
                    " updated_at=? WHERE id=?",
                    (CompStatus.DONE.value, attempts, None, now, comp["id"]),
                )
                conn.execute(
                    "UPDATE task SET status=?, version=version+1, updated_at=?"
                    " WHERE id=?",
                    (TaskStatus.COMPENSATED.value, now, comp["task_id"]),
                )
                return {"idem_key": comp["idem_key"], "outcome": "compensated"}
            if attempts >= self.max_comp_attempts:
                conn.execute(
                    "UPDATE compensation SET status=?, attempts_done=?, last_error=?,"
                    " updated_at=? WHERE id=?",
                    (CompStatus.FAILED.value, attempts, err, now, comp["id"]),
                )
                conn.execute(
                    "UPDATE task SET status=?, version=version+1, updated_at=?"
                    " WHERE id=?",
                    (TaskStatus.SUSPENDED.value, now, comp["task_id"]),
                )
                return {"idem_key": comp["idem_key"], "outcome": "suspended",
                        "error": err}
            conn.execute(
                "UPDATE compensation SET attempts_done=?, last_error=?,"
                " updated_at=? WHERE id=?",
                (attempts, err, now, comp["id"]),
            )
            return {"idem_key": comp["idem_key"], "outcome": "retry",
                    "attempts_done": attempts, "error": err}

    # ------------------------------------------------------------------ #
    # Read side
    # ------------------------------------------------------------------ #

    def get_task(self, task_id: str) -> dict | None:
        return self.db.get_task(task_id)

    def materialized_detail(self, task_id: str) -> list[Any]:
        task = self.db.get_task(task_id)
        if not task:
            return []
        rows = self.db.list_fragments(task_id, task["current_attempt"])
        return [json.loads(r["payload"]) for r in rows]

    def full_state(self, task_id: str) -> dict:
        return self.db.dump_state(task_id)

    @staticmethod
    def new_id() -> str:
        return uuid.uuid4().hex
