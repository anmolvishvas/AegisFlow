"""Base execution runner models and abstractions."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from aegisflow.core.models import TaskDefinition


@dataclass(frozen=True)
class TaskExecutionResult:
    """Outcome of a task execution attempt."""

    task_id: str
    success: bool
    result: Any = None
    error: str | None = None
    traceback: str | None = None
    duration_seconds: float = 0.0


class BaseRunner(ABC):
    """Abstract runner responsible for executing task callables."""

    @abstractmethod
    async def run(
        self,
        task_def: TaskDefinition,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> TaskExecutionResult:
        """Execute a task definition with given arguments and enforce timeouts."""

    @abstractmethod
    async def close(self) -> None:
        """Release runner resources, thread pools, or process workers."""
