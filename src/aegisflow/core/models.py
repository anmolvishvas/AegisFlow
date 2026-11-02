"""Core domain definitions for tasks, workflows, and retry policies."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, Field


class RetryPolicy(BaseModel):
    """Configuration for automatic task retries with backoff and jitter."""

    max_retries: int = Field(default=3, ge=0)
    initial_interval_seconds: float = Field(default=1.0, gt=0)
    backoff_coefficient: float = Field(default=2.0, ge=1.0)
    max_interval_seconds: float = Field(default=60.0, gt=0)
    jitter: bool = True

    def calculate_delay(self, attempt: int) -> float:
        """Calculate the backoff delay in seconds for a given attempt number (1-indexed)."""
        import random

        delay = self.initial_interval_seconds * (self.backoff_coefficient ** (attempt - 1))
        delay = min(delay, self.max_interval_seconds)

        if self.jitter:
            delay = random.uniform(0.0, delay)

        return delay


@dataclass(frozen=True)
class TaskDefinition:
    """Immutable metadata and configuration for a task in a DAG."""

    task_id: str
    name: str
    fn: Callable[..., Any]
    is_async: bool
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    timeout_seconds: float | None = None
    compensate_fn: Callable[..., Any] | None = None
    dependencies: set[str] = field(default_factory=set)


@dataclass
class WorkflowDefinition:
    """Immutable representation of a compiled Directed Acyclic Graph of tasks."""

    name: str
    tasks: dict[str, TaskDefinition] = field(default_factory=dict)

    def get_task(self, task_id: str) -> TaskDefinition:
        """Retrieve task definition by ID or raise TaskNotFoundError."""
        from aegisflow.core.exceptions import TaskNotFoundError

        if task_id not in self.tasks:
            raise TaskNotFoundError(f"Task '{task_id}' not found in workflow '{self.name}'")
        return self.tasks[task_id]

    @property
    def task_ids(self) -> set[str]:
        """Return all task IDs in this workflow."""
        return set(self.tasks.keys())
