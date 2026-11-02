"""Domain-specific exception hierarchy for AegisFlow."""

from typing import Any


class AegisFlowError(Exception):
    """Base exception for all errors raised by AegisFlow."""

    def __init__(self, message: str, *args: Any, **context: Any) -> None:
        super().__init__(message, *args)
        self.message = message
        self.context = context

    def __str__(self) -> str:
        if self.context:
            ctx_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            return f"{self.message} ({ctx_str})"
        return self.message


# Workflow Exceptions
class WorkflowError(AegisFlowError):
    """Base exception for workflow-related errors."""


class WorkflowNotFoundError(WorkflowError):
    """Raised when a requested workflow run does not exist."""


class WorkflowAlreadyExistsError(WorkflowError):
    """Raised when attempting to create a workflow with a duplicate ID."""


class WorkflowExecutionError(WorkflowError):
    """Raised when a workflow fails execution unrecoverably."""


class SagaCompensationError(WorkflowError):
    """Raised when a saga compensating transaction fails execution."""


class CycleDetectedError(WorkflowError):
    """Raised when a circular dependency is detected in the DAG."""

    def __init__(self, cycle: list[str]) -> None:
        cycle_str = " -> ".join(cycle)
        super().__init__(
            f"Circular dependency detected in workflow DAG: {cycle_str}",
            cycle=cycle,
        )


# Task Exceptions
class TaskError(AegisFlowError):
    """Base exception for task execution errors."""


class TaskNotFoundError(TaskError):
    """Raised when a task definition or instance cannot be found."""


class TaskLeaseExpiredError(TaskError):
    """Raised when a worker attempts to complete a task whose lease expired."""


class TaskExecutionError(TaskError):
    """Raised when a task callable raises an unhandled exception."""


class MaxRetriesExceededError(TaskError):
    """Raised when a task exhausts all configured retry attempts."""


# Storage & Concurrency Exceptions
class StorageError(AegisFlowError):
    """Base exception for storage and persistence errors."""


class EventStoreUnavailableError(StorageError):
    """Raised when the event store database is unreachable."""


class ConcurrencyConflictError(StorageError):
    """Raised when an operation is rejected due to an outdated fencing token or concurrency race."""
