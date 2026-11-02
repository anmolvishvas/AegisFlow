"""Abstract storage interfaces for durable event sourcing and distributed task leasing."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from aegisflow.core.events import BaseEvent
from aegisflow.core.states import WorkflowState


@dataclass(frozen=True)
class LeaseInfo:
    """Represents an active task execution lease."""

    task_id: str
    workflow_id: str
    worker_id: str
    fencing_token: int
    expires_at: datetime
    acquired_at: datetime


class EventStoreInterface(ABC):
    """Abstract interface for the append-only Write-Ahead Log (WAL) event store."""

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize database schema, tables, and WAL configuration."""

    @abstractmethod
    async def create_workflow(
        self,
        workflow_id: str,
        name: str,
        status: WorkflowState = WorkflowState.PENDING,
        input_data: dict[str, Any] | None = None,
    ) -> None:
        """Register a new workflow in the store."""

    @abstractmethod
    async def update_workflow_status(
        self,
        workflow_id: str,
        status: WorkflowState,
        output_data: Any = None,
    ) -> None:
        """Update workflow lifecycle status and optional terminal output."""

    @abstractmethod
    async def get_workflow_record(self, workflow_id: str) -> dict[str, Any] | None:
        """Retrieve workflow metadata record."""

    @abstractmethod
    async def append_event(self, event: BaseEvent) -> None:
        """Append an immutable event to the workflow's event log."""

    @abstractmethod
    async def get_events(self, workflow_id: str) -> list[BaseEvent]:
        """Retrieve all recorded events for a workflow in chronological order."""

    @abstractmethod
    async def get_active_workflows(self) -> list[str]:
        """Retrieve IDs of all workflows currently in non-terminal states."""

    @abstractmethod
    async def close(self) -> None:
        """Close connection pools and release resources."""


class LeaseManagerInterface(ABC):
    """Abstract interface for distributed task leasing, heartbeats, and fencing tokens."""

    @abstractmethod
    async def acquire_lease(
        self,
        task_id: str,
        workflow_id: str,
        worker_id: str,
        ttl_seconds: float,
    ) -> LeaseInfo:
        """Acquire an exclusive execution lease with a monotonic fencing token.

        Raises:
            ConcurrencyConflictError: If the task is already leased by an active worker.
        """

    @abstractmethod
    async def renew_lease(
        self,
        task_id: str,
        worker_id: str,
        fencing_token: int,
        ttl_seconds: float,
    ) -> LeaseInfo:
        """Renew an existing lease (heartbeat).

        Raises:
            ConcurrencyConflictError: If the fencing token is outdated or lease was lost.
            TaskLeaseExpiredError: If the lease record no longer exists.
        """

    @abstractmethod
    async def release_lease(
        self,
        task_id: str,
        worker_id: str,
        fencing_token: int,
    ) -> None:
        """Release a lease upon task completion or failure."""

    @abstractmethod
    async def reap_expired_leases(self) -> list[LeaseInfo]:
        """Find and remove all expired leases, making tasks available for re-scheduling."""
