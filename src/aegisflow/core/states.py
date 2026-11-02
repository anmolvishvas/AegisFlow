"""Workflow and Task lifecycle states for AegisFlow."""

from enum import StrEnum


class WorkflowState(StrEnum):
    """Lifecycle states of an AegisFlow workflow."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"
    CANCELLED = "CANCELLED"

    @property
    def is_terminal(self) -> bool:
        """Return True if the workflow is in a final, immutable state."""
        return self in (
            WorkflowState.COMPLETED,
            WorkflowState.FAILED,
            WorkflowState.COMPENSATED,
            WorkflowState.CANCELLED,
        )


class TaskState(StrEnum):
    """Lifecycle states of an individual task node in a DAG."""

    PENDING = "PENDING"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    COMPENSATED = "COMPENSATED"
    SKIPPED = "SKIPPED"

    @property
    def is_terminal(self) -> bool:
        """Return True if the task has reached a terminal state."""
        return self in (
            TaskState.COMPLETED,
            TaskState.FAILED,
            TaskState.COMPENSATED,
            TaskState.SKIPPED,
        )

    @property
    def is_active(self) -> bool:
        """Return True if the task is actively being scheduled or running."""
        return self in (TaskState.SCHEDULED, TaskState.RUNNING, TaskState.RETRYING)
