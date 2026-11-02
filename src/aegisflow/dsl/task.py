"""Task decorator and invocation wrapper for AegisFlow."""

import asyncio
from collections.abc import Callable
from functools import wraps
from typing import Any, ParamSpec, TypeVar, overload

from aegisflow.core.models import RetryPolicy, TaskDefinition

P = ParamSpec("P")
R = TypeVar("R")


class TaskWrapper:
    """Wraps a user callable with task execution metadata and dependency chaining."""

    def __init__(
        self,
        fn: Callable[..., Any],
        name: str | None = None,
        retries: int = 3,
        initial_interval: float = 1.0,
        backoff: float = 2.0,
        max_interval: float = 60.0,
        jitter: bool = True,
        timeout: float | None = None,
        compensate: Callable[..., Any] | None = None,
    ) -> None:
        self.fn = fn
        self.name = name or fn.__name__
        self.task_id = self.name
        self.is_async = asyncio.iscoroutinefunction(fn)
        self.retry_policy = RetryPolicy(
            max_retries=retries,
            initial_interval_seconds=initial_interval,
            backoff_coefficient=backoff,
            max_interval_seconds=max_interval,
            jitter=jitter,
        )
        self.timeout = timeout
        self.compensate = compensate
        self._dependencies: set[str] = set()

        wraps(fn)(self)

    @property
    def dependencies(self) -> set[str]:
        """Return the set of task IDs this task depends on."""
        return set(self._dependencies)

    def after(self, *tasks: "TaskWrapper | str") -> "TaskWrapper":
        """Declare that this task must execute after the specified tasks.

        Args:
            *tasks: Upstream tasks (TaskWrapper instances or string task names).

        Returns:
            self for fluent chaining.
        """
        for t in tasks:
            if isinstance(t, TaskWrapper):
                self._dependencies.add(t.task_id)
            elif isinstance(t, str):
                self._dependencies.add(t)
            else:
                raise TypeError(f"Expected TaskWrapper or str, got {type(t).__name__}")
        return self

    def to_definition(self) -> TaskDefinition:
        """Export the task wrapper as an immutable TaskDefinition."""
        return TaskDefinition(
            task_id=self.task_id,
            name=self.name,
            fn=self.fn,
            is_async=self.is_async,
            retry_policy=self.retry_policy,
            timeout_seconds=self.timeout,
            compensate_fn=self.compensate,
            dependencies=set(self._dependencies),
        )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Allow direct local invocation for debugging and testing."""
        return self.fn(*args, **kwargs)


@overload
def task(fn: Callable[P, R]) -> TaskWrapper: ...


@overload
def task(
    *,
    name: str | None = None,
    retries: int = 3,
    initial_interval: float = 1.0,
    backoff: float = 2.0,
    max_interval: float = 60.0,
    jitter: bool = True,
    timeout: float | None = None,
    compensate: Callable[..., Any] | None = None,
) -> Callable[[Callable[P, R]], TaskWrapper]: ...


def task(
    fn: Callable[P, R] | None = None,
    *,
    name: str | None = None,
    retries: int = 3,
    initial_interval: float = 1.0,
    backoff: float = 2.0,
    max_interval: float = 60.0,
    jitter: bool = True,
    timeout: float | None = None,
    compensate: Callable[..., Any] | None = None,
) -> TaskWrapper | Callable[[Callable[P, R]], TaskWrapper]:
    """Decorator to mark a function as an AegisFlow task.

    Can be used bare (@task) or with arguments (@task(retries=5)).
    """

    def decorator(func: Callable[P, R]) -> TaskWrapper:
        return TaskWrapper(
            fn=func,
            name=name,
            retries=retries,
            initial_interval=initial_interval,
            backoff=backoff,
            max_interval=max_interval,
            jitter=jitter,
            timeout=timeout,
            compensate=compensate,
        )

    if fn is not None:
        return decorator(fn)
    return decorator
