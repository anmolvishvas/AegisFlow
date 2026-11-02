"""Immutable event models for event-sourced durability (Write-Ahead Log)."""

from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _gen_uuid() -> str:
    return str(uuid4())


class BaseEvent(BaseModel):
    """Base class for all immutable events in AegisFlow."""

    event_id: str = Field(default_factory=_gen_uuid)
    workflow_id: str
    timestamp: datetime = Field(default_factory=_utc_now)
    event_type: str

    model_config = {
        "frozen": True,
        "extra": "forbid",
    }


# Workflow Events
class WorkflowInitiated(BaseEvent):
    """Recorded when a workflow is submitted and validated."""

    event_type: Literal["WorkflowInitiated"] = "WorkflowInitiated"
    workflow_name: str
    input_data: dict[str, Any] = Field(default_factory=dict)


class WorkflowRunning(BaseEvent):
    """Recorded when the orchestrator begins executing ready tasks."""

    event_type: Literal["WorkflowRunning"] = "WorkflowRunning"


class WorkflowCompleted(BaseEvent):
    """Recorded when all tasks in the DAG finish successfully."""

    event_type: Literal["WorkflowCompleted"] = "WorkflowCompleted"
    output_data: Any = None


class WorkflowFailed(BaseEvent):
    """Recorded when a non-retryable task fails and the workflow aborts."""

    event_type: Literal["WorkflowFailed"] = "WorkflowFailed"
    error: str
    traceback: str | None = None


class WorkflowCompensating(BaseEvent):
    """Recorded when the engine initiates saga compensation rollbacks."""

    event_type: Literal["WorkflowCompensating"] = "WorkflowCompensating"
    reason: str


class WorkflowCompensated(BaseEvent):
    """Recorded when all registered compensations finish successfully."""

    event_type: Literal["WorkflowCompensated"] = "WorkflowCompensated"


# Task Events
class TaskScheduled(BaseEvent):
    """Recorded when a task's dependencies are satisfied and it is queued."""

    event_type: Literal["TaskScheduled"] = "TaskScheduled"
    task_id: str
    task_name: str
    input_args: tuple[Any, ...] = ()
    input_kwargs: dict[str, Any] = Field(default_factory=dict)


class TaskLeased(BaseEvent):
    """Recorded when an active worker acquires an exclusive execution lease."""

    event_type: Literal["TaskLeased"] = "TaskLeased"
    task_id: str
    worker_id: str
    fencing_token: int
    lease_expires_at: datetime


class TaskStarted(BaseEvent):
    """Recorded when a worker begins executing the task callable."""

    event_type: Literal["TaskStarted"] = "TaskStarted"
    task_id: str
    worker_id: str


class TaskHeartbeat(BaseEvent):
    """Recorded periodically by a running worker to extend its lease."""

    event_type: Literal["TaskHeartbeat"] = "TaskHeartbeat"
    task_id: str
    worker_id: str
    fencing_token: int
    new_expires_at: datetime


class TaskCompleted(BaseEvent):
    """Recorded when a task returns a successful result."""

    event_type: Literal["TaskCompleted"] = "TaskCompleted"
    task_id: str
    result: Any = None


class TaskFailed(BaseEvent):
    """Recorded when a task execution raises an unhandled exception."""

    event_type: Literal["TaskFailed"] = "TaskFailed"
    task_id: str
    error: str
    traceback: str | None = None
    is_retryable: bool = False


class TaskRetried(BaseEvent):
    """Recorded when a failed task is scheduled for a future retry attempt."""

    event_type: Literal["TaskRetried"] = "TaskRetried"
    task_id: str
    attempt: int
    next_retry_at: datetime


class TaskCompensated(BaseEvent):
    """Recorded when a completed task's compensation routine executes successfully."""

    event_type: Literal["TaskCompensated"] = "TaskCompensated"
    task_id: str
    result: Any = None


EventUnion = (
    WorkflowInitiated
    | WorkflowRunning
    | WorkflowCompleted
    | WorkflowFailed
    | WorkflowCompensating
    | WorkflowCompensated
    | TaskScheduled
    | TaskLeased
    | TaskStarted
    | TaskHeartbeat
    | TaskCompleted
    | TaskFailed
    | TaskRetried
    | TaskCompensated
)
