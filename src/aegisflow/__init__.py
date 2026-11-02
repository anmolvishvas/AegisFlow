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
from aegisflow.engine.lease import HeartbeatMonitor, LeaseReaper
from aegisflow.engine.orchestrator import WorkflowOrchestrator
from aegisflow.engine.replay import replay_workflow
from aegisflow.engine.state_machine import WorkflowExecutionState
from aegisflow.storage.base import EventStoreInterface, LeaseInfo, LeaseManagerInterface
from aegisflow.storage.sqlite import SQLiteStorage
from aegisflow.workers.async_runner import AsyncRunner
from aegisflow.workers.base_runner import BaseRunner, TaskExecutionResult
from aegisflow.workers.daemon import WorkerSupervisorDaemon
from aegisflow.workers.process_runner import ProcessRunner

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
    # Storage & Leases
    "EventStoreInterface",
    "LeaseManagerInterface",
    "LeaseInfo",
    "SQLiteStorage",
    # Engine & Workers
    "HeartbeatMonitor",
    "LeaseReaper",
    "WorkflowExecutionState",
    "WorkflowOrchestrator",
    "replay_workflow",
    "BaseRunner",
    "AsyncRunner",
    "ProcessRunner",
    "TaskExecutionResult",
    "WorkerSupervisorDaemon",
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
