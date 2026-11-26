"""End-to-end demonstration of the coordination guarantees.

Scenarios
  A. Out-of-order callbacks, including a stale success from attempt #1 after
     attempt #2 has already FAILED — the old success must not win.
  B. Local timeout while the downstream actually applied the effect — the
     reentrant reconcile/compensate path undoes it exactly once.
  C. Detail fragments arrive incrementally and the final checksum is verified
     before the task converges.

Run:  python3 -m demo.scenarios
"""

from __future__ import annotations

import os
import random
import time

from coord import (
    CallbackEvent,
    CallbackResult,
    Coordinator,
    MockDownstream,
    Storage,
    TaskStatus,
    Worker,
)

DB_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def build(rng_seed: int, tag: str):
    os.makedirs(DB_DIR, exist_ok=True)
    coord_db = os.path.join(DB_DIR, f"coord-{tag}.db")
    down_db = os.path.join(DB_DIR, f"down-{tag}.db")
    for p in (coord_db, down_db):
        if os.path.exists(p):
            os.remove(p)

    events: list[dict] = []

    def delivery(env: dict) -> None:
        events.append(env)
        ev = CallbackEvent(
            task_id=env["task_id"],
            attempt_no=env["attempt_no"],
            callback_id=env["callback_id"],
            status=env["status"],
            detail_n=env.get("detail_n"),
            checksum=env.get("checksum"),
            fragment_seq=env.get("fragment_seq"),
            fragment=env.get("fragment"),
            error_code=env.get("error_code"),
            downstream_ref=env.get("downstream_ref"),
            raw=env,
        )
        result, _ = coord.record_callback(ev)
        print(f"  <- callback {env['callback_id']:30s} "
              f"attempt={env['attempt_no']} {env['status']:9s} -> {result.value}")

    storage = Storage(coord_db)
    downstream = MockDownstream(down_db, delivery=delivery,
                                rng=random.Random(rng_seed), jitter=0.02)
    coord = Coordinator(storage, downstream, timeout_seconds=0.3,
                        max_attempts=3)
    worker = Worker(coord, interval=0.02)
    return coord, downstream, worker, events


def wait_terminal(coord: Coordinator, task_id: str, timeout: float = 5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        t = coord.get_task(task_id)
        if t["status"] in {s.value for s in TaskStatus
                           if s in (TaskStatus.SUCCESS, TaskStatus.FAILED,
                                    TaskStatus.COMPENSATED,
                                    TaskStatus.SUSPENDED)}:
            return t
        time.sleep(0.02)
    raise TimeoutError(f"task {task_id} did not finish: {coord.get_task(task_id)}")


def scenario_a():
    print("\n=== A. 乱序回调 + 旧成功不得覆盖新失败 ===")
    coord, down, worker, _ = build(11, "a")
    tid = "T-A"
    # All attempts fail so the task reaches FAILED; then a stale success from
    # attempt #1 turns up late and must not resurrect it.
    for attempt in (1, 2, 3):
        down.set_policy(tid, attempt, "fail")
    coord.create_task(tid, "transfer", {"biz": "pay"})
    worker.start()
    final = wait_terminal(coord, tid)
    print(f"  task reached terminal: {final['status']}"
          f" (fence={final['current_attempt']})")
    # Now the ancient, out-of-order success callback finally arrives.
    result, task = coord.record_callback(CallbackEvent(
        task_id=tid, attempt_no=1,
        callback_id="cb-injected-stale-success", status="SUCCESS",
        raw={"note": "delivered after the task already failed"},
    ))
    print(f"  <- injected stale success attempt=1 -> {result.value}")
    assert result == CallbackResult.STALE_SUCCESS_RECORDED
    worker.stop()
    state = coord.full_state(tid)
    print(f"  final status: {task['status']}, fence={task['current_attempt']}")
    assert task["status"] == TaskStatus.FAILED.value
    assert task["current_attempt"] == 3
    assert all(cb["is_stale"] == 1 for cb in state["callbacks"]
               if cb["callback_id"] == "cb-injected-stale-success")
    print("  OK: stale success logged but never moved the task")


def scenario_b():
    print("\n=== B. 本地超时但下游已生效 -> 可重入补偿 ===")
    coord, down, worker, _ = build(22, "b")
    tid = "T-B"
    down.set_policy(tid, 1, "lost")          # applied, no callback
    coord.create_task(tid, "transfer", {"biz": "pay"})
    worker.start()
    final = wait_terminal(coord, tid)
    worker.stop()
    state = coord.full_state(tid)
    print(f"  final status: {final['status']}")
    assert final["status"] == TaskStatus.COMPENSATED.value
    assert down.effect_state(tid, 1) == "COMPENSATED"
    comp = state["compensations"][0]
    assert comp["idem_key"] == f"comp:{tid}:1"
    # Reentrancy: replaying compensate with the same key undoes nothing twice.
    assert down.compensate(tid, 1, comp["idem_key"]) is True
    assert len(down.conn.execute(
        "SELECT * FROM undo_log").fetchall()) == 1
    print("  OK: reconcile saw APPLIED, idempotent compensation ran exactly once")


def scenario_c():
    print("\n=== C. 回调明细逐步放入 + checksum 最终核对 ===")
    coord, down, worker, _ = build(33, "c")
    tid = "T-C"
    down.set_policy(tid, 1, "ok")
    coord.create_task(tid, "report", {"biz": "rpt"})
    worker.start()
    final = wait_terminal(coord, tid)
    worker.stop()
    detail = coord.materialized_detail(tid)
    print(f"  final status: {final['status']}, fragments={len(detail)}")
    assert final["status"] == TaskStatus.SUCCESS.value
    assert len(detail) == 3 and [f["part"] for f in detail] == [0, 1, 2]
    assert final["detail_checksum"]
    print("  OK: shuffled fragments reordered by seq and checksum verified")


def scenario_d():
    print("\n=== D. 持续 UNKNOWN 的对账 -> 退避后挂起人工介入 ===")
    coord, down, worker, _ = build(44, "d")
    tid = "T-D"
    down.set_policy(tid, 1, "unknown")   # query_status always UNKNOWN
    coord.max_reconcile_runs = 3
    coord.create_task(tid, "transfer", {"biz": "pay"})
    worker.start()
    final = wait_terminal(coord, tid)
    worker.stop()
    print(f"  final status: {final['status']}")
    assert final["status"] == TaskStatus.SUSPENDED.value
    jobs = coord.full_state(tid)["reconcile_jobs"]
    assert jobs[0]["runs"] >= 3
    print("  OK: reconcile backed off and the task was suspended, not guessed")


if __name__ == "__main__":
    scenario_a()
    scenario_b()
    scenario_c()
    scenario_d()
    print("\nAll scenarios passed.")
