"""AegisFlow: Lightweight, durable event-driven workflow orchestration engine for Python."""

from aegisflow.core.exceptions import (
    AegisFlowError,
    ConcurrencyConflictError,
    CycleDetectedError,
    MaxRetriesExceededError,
    StorageError,
    TaskError,
    TaskExecutionError,
    TaskLeaseExpiredError,
    TaskNotFoundError,
    WorkflowAlreadyExistsError,
    WorkflowError,
    WorkflowExecutionError,
    WorkflowNotFoundError,
)
from aegisflow.core.models import RetryPolicy, TaskDefinition, WorkflowDefinition
from aegisflow.core.states import TaskState, WorkflowState
from aegisflow.dsl.graph import DAG, DAGBuilder
from aegisflow.dsl.task import TaskWrapper, task
from aegisflow.dsl.workflow import Workflow, workflow

__version__ = "0.1.0"

__all__ = [
    # Top-level DSL
    "task",
    "workflow",
    "Workflow",
    "TaskWrapper",
    "DAG",
    "DAGBuilder",
    # Core States & Models
    "WorkflowState",
    "TaskState",
    "RetryPolicy",
    "TaskDefinition",
    "WorkflowDefinition",
    # Domain Exceptions
    "AegisFlowError",
    "WorkflowError",
    "WorkflowNotFoundError",
    "WorkflowAlreadyExistsError",
    "WorkflowExecutionError",
    "CycleDetectedError",
    "TaskError",
    "TaskNotFoundError",
    "TaskLeaseExpiredError",
    "TaskExecutionError",
    "MaxRetriesExceededError",
    "StorageError",
    "ConcurrencyConflictError",
]
