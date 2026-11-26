"""Simulated downstream service.

Behaviour modelled:

* Each ``(task_id, attempt_no)`` dispatch is idempotent: redelivery returns the
  same downstream reference instead of creating a second effect.
* The downstream decides its own verdict per attempt (driven by a fault-injection
  policy) and schedules callback delivery on background threads with per-callback
  jitter, so callbacks arrive **out of order** and are sometimes redelivered.
* A dispatch can be *lost*: the effect is applied but the callback never leaves
  the building — the coordinator must discover it via ``query_status`` and run
  reentrant compensation.
* Compensation is idempotent under an idem_key and the effect state is durable,
  so a replay cannot undo the same effect twice.
"""

from __future__ import annotations

import json
import random
import sqlite3
import threading
import time
from typing import Any, Callable

from .coordinator import detail_checksum

SCHEMA = """
CREATE TABLE IF NOT EXISTS effect (
    task_id      TEXT NOT NULL,
    attempt_no   INTEGER NOT NULL,
    ref          TEXT NOT NULL,
    state        TEXT NOT NULL,          -- APPLIED / FAILED / COMPENSATED
    detail       TEXT NOT NULL,
    checksum     TEXT NOT NULL,
    PRIMARY KEY (task_id, attempt_no)
);
CREATE TABLE IF NOT EXISTS undo_log (
    idem_key TEXT PRIMARY KEY,
    task_id  TEXT NOT NULL,
    done_at  REAL NOT NULL
);
"""


class MockDownstream:
    def __init__(
        self,
        path: str,
        *,
        delivery: Callable[[dict], None],
        rng: random.Random | None = None,
        jitter: float = 0.05,
    ):
        """delivery: sink that receives callback envelopes (the coordinator's
        intake, normally wrapped to build CallbackEvent)."""
        self.conn = sqlite3.connect(path, timeout=30, isolation_level=None,
                                    check_same_thread=False)
        # All access is serialized by self._lock; the connection crosses
        # coordinator/worker/delivery threads.
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(SCHEMA)
        self._delivery = delivery
        self._rng = rng or random.Random(0xC0FFEE)
        self._jitter = jitter
        # Fault policy per (task_id, attempt_no) -> behaviour.
        #   "ok"        success, complete callbacks (possibly delayed/shuffled)
        #   "fail"      downstream rejects, FAILED callback
        #   "lost"      effect APPLIED but no callback at all (timeout path)
        #   "flaky"     first attempts fail, the last one succeeds
        #   "unknown"   callbacks swallowed AND query_status stays UNKNOWN
        #               (persistent ambiguity -> backoff + suspend)
        self.policy: dict[tuple[str, int], str] = {}
        self._lock = threading.RLock()

    def set_policy(self, task_id: str, attempt_no: int, behaviour: str) -> None:
        self.policy[(task_id, attempt_no)] = behaviour

    # ------------- gateway interface used by the coordinator ------------- #

    def dispatch(self, task_id: str, attempt_no: int, payload: dict) -> str:
        with self._lock:
            existing = self.conn.execute(
                "SELECT ref FROM effect WHERE task_id=? AND attempt_no=?",
                (task_id, attempt_no),
            ).fetchone()
            if existing:  # idempotent redelivery
                return existing["ref"]

            ref = f"ref-{task_id}-{attempt_no}"
            behaviour = self.policy.get((task_id, attempt_no), "ok")
            # Detail fragments are derived deterministically from the payload.
            fragments = [
                {"part": i, "data": f"{payload.get('biz','x')}::{task_id}::{i}"}
                for i in range(3)
            ]
            checksum = detail_checksum(fragments)
            if behaviour == "fail":
                state = "FAILED"
            else:
                state = "APPLIED"
            self.conn.execute(
                "INSERT INTO effect(task_id, attempt_no, ref, state, detail,"
                " checksum) VALUES(?,?,?,?,?,?)",
                (
                    task_id, attempt_no, ref, state,
                    json.dumps(fragments), checksum,
                ),
            )

            if behaviour == "fail":
                self._schedule(
                    self._cb(task_id, attempt_no, ref, "FAILED", fragments,
                             checksum, error_code="DOWN_REJECTED")
                )
            elif behaviour in ("lost", "unknown"):
                # Effect applied but the callback never reaches the coordinator
                # ("unknown" additionally keeps reconcile answers ambiguous for
                # a while — see query_status).
                pass
            else:  # ok / flaky -> success envelope, shuffled delivery
                self._schedule_success(task_id, attempt_no, ref, fragments,
                                       checksum)
            return ref

    def query_status(self, task_id: str, attempt_no: int) -> str:
        behaviour = self.policy.get((task_id, attempt_no), "ok")
        if behaviour == "unknown":
            # Persistently ambiguous downstream (e.g. replica lag): the
            # coordinator must back off and eventually suspend, never guess.
            return "UNKNOWN"
        row = self.conn.execute(
            "SELECT state FROM effect WHERE task_id=? AND attempt_no=?",
            (task_id, attempt_no),
        ).fetchone()
        if row is None:
            return "NOT_APPLIED"
        if row["state"] == "FAILED":
            return "NOT_APPLIED"
        return "APPLIED"  # APPLIED or COMPENSATED both mean "an effect existed"

    def compensate(self, task_id: str, attempt_no: int, idem_key: str) -> bool:
        with self._lock:
            already = self.conn.execute(
                "SELECT 1 FROM undo_log WHERE idem_key=?", (idem_key,)
            ).fetchone()
            if already:  # reentrant: undo already happened
                return True
            row = self.conn.execute(
                "SELECT state FROM effect WHERE task_id=? AND attempt_no=?",
                (task_id, attempt_no),
            ).fetchone()
            if row is None:
                return True  # nothing to undo is also a completed compensation
            self.conn.execute(
                "UPDATE effect SET state='COMPENSATED' WHERE task_id=?"
                " AND attempt_no=?",
                (task_id, attempt_no),
            )
            self.conn.execute(
                "INSERT INTO undo_log(idem_key, task_id, done_at) VALUES(?,?,?)",
                (idem_key, task_id, time.time()),
            )
            return True

    def fetch_detail(self, task_id: str, attempt_no: int) -> dict:
        row = self.conn.execute(
            "SELECT detail, checksum FROM effect WHERE task_id=? AND attempt_no=?",
            (task_id, attempt_no),
        ).fetchone()
        if row is None:
            raise RuntimeError("no effect to fetch")
        return {
            "fragments": json.loads(row["detail"]),
            "checksum": row["checksum"],
        }

    # --------------------------- test helpers ---------------------------- #

    def effect_state(self, task_id: str, attempt_no: int) -> str | None:
        row = self.conn.execute(
            "SELECT state FROM effect WHERE task_id=? AND attempt_no=?",
            (task_id, attempt_no),
        ).fetchone()
        return row["state"] if row else None

    def all_effects(self, task_id: str) -> list[dict]:
        return [
            dict(r)
            for r in self.conn.execute(
                "SELECT attempt_no, ref, state FROM effect WHERE task_id=?"
                " ORDER BY attempt_no",
                (task_id,),
            ).fetchall()
        ]

    # ----------------------- callback scheduling ------------------------- #

    def _cb(self, task_id, attempt_no, ref, status, fragments, checksum,
            error_code=None, cid_suffix: str = "") -> list[dict]:
        cid = f"cb-{task_id}-{attempt_no}-{status}{cid_suffix}"
        n = len(fragments)
        envelopes = [
            {
                "callback_id": f"{cid}-f{i}",
                "task_id": task_id,
                "attempt_no": attempt_no,
                "status": "PROGRESS",
                "fragment_seq": i,
                "fragment": fragments[i],
                "downstream_ref": ref,
            }
            for i in range(n)
        ]
        envelopes.append({
            "callback_id": cid,
            "task_id": task_id,
            "attempt_no": attempt_no,
            "status": status,
            "detail_n": n,
            "checksum": checksum,
            "error_code": error_code,
            "downstream_ref": ref,
        })
        return envelopes

    def _schedule_success(self, task_id, attempt_no, ref, fragments,
                          checksum) -> None:
        envelopes = self._cb(task_id, attempt_no, ref, "SUCCESS",
                             fragments, checksum)
        # Out-of-order: shuffle, and randomly redeliver one fragment.
        self._rng.shuffle(envelopes)
        for env in envelopes:
            self._schedule(env)
        if self._rng.random() < 0.5:
            dup = dict(envelopes[0])
            self._schedule(dup)

    def _schedule(self, envelope: dict) -> None:
        delay = self._rng.uniform(0.0, self._jitter)

        def runner():
            time.sleep(delay)
            try:
                self._delivery(envelope)
            except Exception:
                # A delivery crash must not kill the simulator; the sweeper /
                # reconcile path is the safety net for swallowed callbacks.
                pass

        threading.Thread(target=runner, daemon=True).start()
