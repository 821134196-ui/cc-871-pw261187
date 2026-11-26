"""Tests for the persistent coordination service."""

from __future__ import annotations

import json
import os
import random
import tempfile
import threading
import time
import unittest

from coord import (
    CallbackEvent,
    CallbackResult,
    Coordinator,
    MockDownstream,
    Storage,
    TaskStatus,
    Worker,
    detail_checksum,
)


class Harness:
    def __init__(self, seed=1, timeout=0.25, **coord_kw):
        self.tmp = tempfile.mkdtemp(prefix="coord-test-")
        self.coord_db = os.path.join(self.tmp, "coord.db")
        self.down_db = os.path.join(self.tmp, "down.db")
        self.received = []
        self.storage = Storage(self.coord_db)
        self.down = MockDownstream(
            self.down_db, delivery=self._deliver,
            rng=random.Random(seed), jitter=0.01,
        )
        self.coord = Coordinator(
            self.storage, self.down,
            timeout_seconds=timeout, max_attempts=3, **coord_kw,
        )
        self.worker = Worker(self.coord, interval=0.01)

    def _deliver(self, env):
        self.received.append(env)
        ev = CallbackEvent(
            task_id=env["task_id"], attempt_no=env["attempt_no"],
            callback_id=env["callback_id"], status=env["status"],
            detail_n=env.get("detail_n"), checksum=env.get("checksum"),
            fragment_seq=env.get("fragment_seq"), fragment=env.get("fragment"),
            error_code=env.get("error_code"),
            downstream_ref=env.get("downstream_ref"), raw=env,
        )
        self.coord.record_callback(ev)

    def start(self):
        self.worker.start()

    def stop(self):
        self.worker.stop()

    def wait(self, tid, timeout=5.0):
        deadline = time.time() + timeout
        terminal = {
            TaskStatus.SUCCESS.value, TaskStatus.FAILED.value,
            TaskStatus.COMPENSATED.value, TaskStatus.SUSPENDED.value,
        }
        while time.time() < deadline:
            t = self.coord.get_task(tid)
            if t and t["status"] in terminal:
                return t
            time.sleep(0.01)
        raise TimeoutError(f"not terminal: {self.coord.get_task(tid)}")


class FencingTests(unittest.TestCase):
    def test_stale_success_cannot_overwrite_newer_failure(self):
        h = Harness(seed=1)
        tid = "T1"
        h.down.set_policy(tid, 1, "fail")
        h.down.set_policy(tid, 2, "fail")
        h.down.set_policy(tid, 3, "fail")
        h.start()
        h.coord.create_task(tid, "op", {})
        final = h.wait(tid)
        h.stop()
        # All three attempts failed -> FAILED. Now an ancient success arrives.
        result, task = h.coord.record_callback(CallbackEvent(
            task_id=tid, attempt_no=1, callback_id="late-success",
            status="SUCCESS", raw={},
        ))
        self.assertEqual(result, CallbackResult.STALE_SUCCESS_RECORDED)
        self.assertEqual(task["status"], TaskStatus.FAILED.value)
        state = h.coord.full_state(tid)
        stale = [c for c in state["callbacks"]
                 if c["callback_id"] == "late-success"][0]
        self.assertEqual(stale["is_stale"], 1)
        self.assertEqual(stale["accepted"], 0)
        self.assertEqual(len(state["attempts"]), 3)

    def test_stale_success_arriving_mid_flight_never_lands(self):
        # The literal race: attempt #1 failed locally, attempt #2 is in flight
        # (fence already advanced), then attempt #1's late success shows up.
        h = Harness(seed=12, timeout=10)
        tid = "T1B"
        h.coord.create_task(tid, "op", {})
        # Fail attempt 1 -> attempt 2 dispatched automatically.
        r, _ = h.coord.record_callback(CallbackEvent(
            task_id=tid, attempt_no=1, callback_id="fail-1",
            status="FAILED", error_code="E", raw={},
        ))
        self.assertEqual(r, CallbackResult.ACCEPTED)
        task = h.coord.get_task(tid)
        self.assertEqual(task["current_attempt"], 2)
        # Attempt 2 then fails as well, but the stale success arrives *before*
        # that failure is processed.
        r, _ = h.coord.record_callback(CallbackEvent(
            task_id=tid, attempt_no=1, callback_id="late-success-1",
            status="SUCCESS", raw={},
        ))
        self.assertEqual(r, CallbackResult.STALE_SUCCESS_RECORDED)
        r, _ = h.coord.record_callback(CallbackEvent(
            task_id=tid, attempt_no=2, callback_id="fail-2",
            status="FAILED", error_code="E", raw={},
        ))
        self.assertEqual(r, CallbackResult.ACCEPTED)
        task = h.coord.get_task(tid)
        self.assertEqual(task["current_attempt"], 3)
        self.assertNotEqual(task["status"], TaskStatus.SUCCESS.value)
        # Forensics: the stale success is preserved in the audit log even
        # though the attempt row keeps its first terminal verdict (FAILED).
        a1 = [a for a in h.coord.full_state(tid)["attempts"]
              if a["attempt_no"] == 1][0]
        self.assertEqual(a1["status"], "FAILED")
        stale_log = [c for c in h.coord.full_state(tid)["callbacks"]
                     if c["callback_id"] == "late-success-1"][0]
        self.assertEqual(stale_log["is_stale"], 1)
        self.assertEqual(stale_log["status"], "SUCCESS")
        self.assertEqual(stale_log["accepted"], 0)

    def test_duplicate_callback_id_is_idempotent(self):
        h = Harness(seed=2)
        tid = "T2"
        h.down.set_policy(tid, 1, "ok")
        h.coord.create_task(tid, "op", {})
        # Two identical terminal callbacks, no worker needed for intake rules.
        ev = CallbackEvent(task_id=tid, attempt_no=1, callback_id="cb-x",
                           status="FAILED", error_code="E", raw={})
        r1, _ = h.coord.record_callback(ev)
        r2, _ = h.coord.record_callback(ev)
        self.assertEqual(r1, CallbackResult.ACCEPTED)
        self.assertEqual(r2, CallbackResult.DUPLICATE)
        logs = h.coord.full_state(tid)["callbacks"]
        self.assertEqual(len([c for c in logs if c["callback_id"] == "cb-x"]), 1)

    def test_fragments_dedup_and_reorder_then_checksum(self):
        h = Harness(seed=3)
        tid = "T3"
        h.coord.create_task(tid, "op", {})
        frags = [{"k": i} for i in range(4)]
        cs = detail_checksum(frags)
        # Deliver fragments in reverse order, seq=2 twice (redelivery).
        for i in (3, 2, 2, 1, 0):
            h.coord.record_callback(CallbackEvent(
                task_id=tid, attempt_no=1,
                callback_id=f"frag-{i}-{time.time_ns()}",
                status="PROGRESS", fragment_seq=i, fragment=frags[i], raw={},
            ))
        result, task = h.coord.record_callback(CallbackEvent(
            task_id=tid, attempt_no=1, callback_id="term",
            status="SUCCESS", detail_n=4, checksum=cs, raw={},
        ))
        self.assertEqual(result, CallbackResult.ACCEPTED)
        self.assertEqual(task["status"], TaskStatus.SUCCESS.value)
        stored = h.coord.full_state(tid)["fragments"]
        self.assertEqual([f["seq"] for f in stored], [0, 1, 2, 3])
        self.assertEqual(h.coord.materialized_detail(tid), frags)

    def test_corrupt_checksum_holds_then_reconcile_repairs(self):
        h = Harness(seed=4)
        tid = "T4"
        h.coord.create_task(tid, "op", {"biz": "z"})
        frags = [{"k": 0}, {"k": 1}, {"k": 2}]
        for i, f in enumerate(frags):
            h.coord.record_callback(CallbackEvent(
                task_id=tid, attempt_no=1, callback_id=f"f{i}",
                status="PROGRESS", fragment_seq=i, fragment=f, raw={},
            ))
        result, task = h.coord.record_callback(CallbackEvent(
            task_id=tid, attempt_no=1, callback_id="term",
            status="SUCCESS", detail_n=3, checksum="deadbeef", raw={},
        ))
        self.assertEqual(result, CallbackResult.DETAIL_HELD)
        self.assertEqual(task["status"], TaskStatus.DETAIL_HELD.value)
        # Worker runs the detail reconcile job, fetching authoritative data.
        h.worker.tick()
        self.assertEqual(
            h.coord.get_task(tid)["status"], TaskStatus.SUCCESS.value
        )


class TimeoutCompensationTests(unittest.TestCase):
    def test_lost_effect_is_compensated_reentrantly(self):
        h = Harness(seed=5)
        tid = "T5"
        h.down.set_policy(tid, 1, "lost")
        h.start()
        h.coord.create_task(tid, "op", {})
        final = h.wait(tid)
        h.stop()
        self.assertEqual(final["status"], TaskStatus.COMPENSATED.value)
        self.assertEqual(h.down.effect_state(tid, 1), "COMPENSATED")
        comp = h.coord.full_state(tid)["compensations"][0]
        # Replay the same compensation: still one undo row downstream.
        h.coord.run_compensation(dict(comp))
        undos = h.down.conn.execute("SELECT COUNT(*) c FROM undo_log").fetchone()
        self.assertEqual(undos["c"], 1)

    def test_not_applied_timeout_retries_with_advancing_fence(self):
        h = Harness(seed=6)
        tid = "T6"
        # No effect row exists at all -> query_status NOT_APPLIED -> retry.
        # Simulate by overriding dispatch to skip downstream registration.
        orig = h.down.dispatch
        def quiet_dispatch(task_id, attempt_no, payload):
            ref = orig(task_id, attempt_no, payload)
            h.down.conn.execute(
                "DELETE FROM effect WHERE task_id=? AND attempt_no=?",
                (task_id, attempt_no),
            )
            return ref
        h.down.dispatch = quiet_dispatch
        h.start()
        h.coord.create_task(tid, "op", {})
        final = h.wait(tid)
        h.stop()
        self.assertEqual(final["status"], TaskStatus.FAILED.value)
        self.assertEqual(final["current_attempt"], 3)
        attempts = h.coord.full_state(tid)["attempts"]
        self.assertEqual(
            [a["status"] for a in attempts],
            ["TIMEOUT", "TIMEOUT", "TIMEOUT"],
        )


class PersistenceTests(unittest.TestCase):
    def test_state_survives_coordinator_restart(self):
        h = Harness(seed=7)
        tid = "T7"
        h.down.set_policy(tid, 1, "lost")
        h.coord.create_task(tid, "op", {})
        time.sleep(h.coord.timeout_seconds + 0.05)
        h.coord.sweep_timeouts()  # job enqueued, then "process restarts"
        # Brand new coordinator/worker over the same database files.
        coord2 = Coordinator(Storage(h.coord_db), h.down,
                             timeout_seconds=0.25, max_attempts=3)
        worker2 = Worker(coord2, interval=0.01)
        worker2.start()
        deadline = time.time() + 5
        while time.time() < deadline:
            t = coord2.get_task(tid)
            if t["status"] == TaskStatus.COMPENSATED.value:
                break
            time.sleep(0.01)
        else:
            self.fail("did not compensate after restart")
        worker2.stop()

    def test_running_job_left_by_crash_is_reclaimable(self):
        h = Harness(seed=8)
        tid = "T8"
        h.down.set_policy(tid, 1, "lost")
        h.coord.create_task(tid, "op", {})
        time.sleep(h.coord.timeout_seconds + 0.05)
        h.coord.sweep_timeouts()
        job = h.coord.claim_job()
        self.assertIsNotNone(job)
        # Simulate crash: job stays RUNNING. A restart flips stale RUNNING
        # rows back to PENDING (recovery hook).
        h.coord.recover_stale_jobs()
        again = h.coord.claim_job()
        self.assertIsNotNone(again)
        self.assertEqual(again["id"], job["id"])


class ConcurrencyTests(unittest.TestCase):
    def test_parallel_callbacks_never_corrupt_fence(self):
        h = Harness(seed=9, timeout=10)
        tid = "T9"
        h.coord.create_task(tid, "op", {})
        errors = []

        def hammer(attempt_no, status, n):
            for i in range(n):
                try:
                    h.coord.record_callback(CallbackEvent(
                        task_id=tid, attempt_no=attempt_no,
                        callback_id=f"cb-{attempt_no}-{status}-{i}-{time.time_ns()}",
                        status=status,
                        error_code="E" if status == "FAILED" else None,
                        raw={},
                    ))
                except Exception as exc:  # pragma: no cover - failure signal
                    errors.append(exc)

        threads = [
            threading.Thread(target=hammer, args=(1, "SUCCESS", 20)),
            threading.Thread(target=hammer, args=(1, "FAILED", 20)),
            threading.Thread(target=hammer, args=(99, "SUCCESS", 20)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertFalse(errors)
        task = h.coord.get_task(tid)
        # First terminal callback on attempt 1 wins deterministically; either
        # way the unknown attempt 99 never affected the row.
        self.assertIn(task["status"],
                      {TaskStatus.SUCCESS.value, TaskStatus.FAILED.value})
        self.assertEqual(task["current_attempt"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
