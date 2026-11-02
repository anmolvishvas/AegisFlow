"""AegisFlow: Lightweight, durable event-driven workflow orchestration engine for Python."""

from aegisflow._version import __version__
from aegisflow.cli.main import app as cli_app
from aegisflow.core.exceptions import (
    AegisFlowError,
    ConcurrencyConflictError,
    CycleDetectedError,
    MaxRetriesExceededError,
    SagaCompensationError,
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
from aegisflow.dashboard.app import create_dashboard_app
from aegisflow.dsl.graph import DAG, DAGBuilder
from aegisflow.dsl.task import TaskWrapper, task
from aegisflow.dsl.workflow import Workflow, workflow
from aegisflow.engine.dlq import DeadLetterManager
from aegisflow.engine.lease import HeartbeatMonitor, LeaseReaper
from aegisflow.engine.orchestrator import WorkflowOrchestrator
from aegisflow.engine.replay import replay_workflow
from aegisflow.engine.retry import RetryScheduler
from aegisflow.engine.saga import SagaCoordinator
from aegisflow.engine.state_machine import WorkflowExecutionState
from aegisflow.observability.telemetry import AegisTelemetry, telemetry
from aegisflow.storage.base import EventStoreInterface, LeaseInfo, LeaseManagerInterface
from aegisflow.storage.sqlite import SQLiteStorage
from aegisflow.workers.async_runner import AsyncRunner
from aegisflow.workers.base_runner import BaseRunner, TaskExecutionResult
from aegisflow.workers.daemon import WorkerSupervisorDaemon
from aegisflow.workers.process_runner import ProcessRunner

__all__ = [
    "__version__",
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
    "RetryScheduler",
    "SagaCoordinator",
    "DeadLetterManager",
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
    "SagaCompensationError",
    "CycleDetectedError",
    "TaskError",
    "TaskNotFoundError",
    "TaskLeaseExpiredError",
    "TaskExecutionError",
    "MaxRetriesExceededError",
    "StorageError",
    "ConcurrencyConflictError",
    # Observability, CLI & Dashboard
    "telemetry",
    "AegisTelemetry",
    "cli_app",
    "create_dashboard_app",
]
