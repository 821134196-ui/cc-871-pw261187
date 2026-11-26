"""Status enums and value objects shared by the coordinator."""

from __future__ import annotations

import enum


class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"                       # created, nothing dispatched yet
    IN_FLIGHT = "IN_FLIGHT"                   # current attempt dispatched
    RETRYING = "RETRYING"                     # a newer attempt superseded the last
    TIMEOUT_PENDING = "TIMEOUT_PENDING"       # local timeout, awaiting reconcile
    COMPENSATING = "COMPENSATING"             # downstream applied, undo in progress
    COMPENSATED = "COMPENSATED"               # terminal: effect undone
    SUCCESS = "SUCCESS"                       # terminal: verified success
    FAILED = "FAILED"                         # terminal: exhausted / confirmed fail
    DETAIL_HELD = "DETAIL_HELD"               # terminal cb received, details unverified
    SUSPENDED = "SUSPENDED"                   # reconcile unknown too often, needs human


class AttemptStatus(str, enum.Enum):
    DISPATCHED = "DISPATCHED"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"                       # local verdict only; reconcile pending
    DETAIL_HELD = "DETAIL_HELD"


class JobStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"


class CompStatus(str, enum.Enum):
    PENDING = "PENDING"
    DONE = "DONE"
    FAILED = "FAILED"


# Result codes returned by record_callback().
class CallbackResult(str, enum.Enum):
    ACCEPTED = "ACCEPTED"                                   # state advanced
    DUPLICATE = "DUPLICATE"                                 # callback_id seen before
    STALE_IGNORED = "STALE_IGNORED"                         # old attempt, logged only
    STALE_SUCCESS_RECORDED = "STALE_SUCCESS_RECORDED"       # old success cannot win
    DETAIL_HELD = "DETAIL_HELD"                             # terminal but checksum bad
    UNKNOWN_TASK = "UNKNOWN_TASK"
    REJECTED = "REJECTED"


# Final states in which no further automatic transition happens.
TERMINAL_TASK = {
    TaskStatus.SUCCESS,
    TaskStatus.FAILED,
    TaskStatus.COMPENSATED,
    TaskStatus.SUSPENDED,
}
