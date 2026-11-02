"""Workflow decorator and composition wrapper for AegisFlow."""

from collections.abc import Callable
from typing import Any

from aegisflow.core.models import WorkflowDefinition
from aegisflow.dsl.graph import DAG, DAGBuilder
from aegisflow.dsl.task import TaskWrapper


class Workflow:
    """Represents a defined workflow containing multiple tasks."""

    def __init__(self, name: str, tasks: list[TaskWrapper] | None = None) -> None:
        self.name = name
        self._tasks: dict[str, TaskWrapper] = {}
        if tasks:
            for t in tasks:
                self.add_task(t)

    def add_task(self, task: TaskWrapper) -> "Workflow":
        """Add a task to the workflow."""
        self._tasks[task.task_id] = task
        return self

    def compile(self) -> DAG:
        """Compile and validate the tasks into a DAG."""
        builder = DAGBuilder(name=self.name)
        for task_wrapper in self._tasks.values():
            builder.add_task(task_wrapper.to_definition())
        return builder.build()

    def to_definition(self) -> WorkflowDefinition:
        """Compile and return an immutable WorkflowDefinition."""
        return self.compile().to_workflow_definition()


def workflow(
    name_or_fn: str | Callable[..., Any] | None = None,
    *,
    tasks: list[TaskWrapper] | None = None,
) -> Any:
    """Decorator or constructor for an AegisFlow workflow."""
    if isinstance(name_or_fn, str):
        return Workflow(name=name_or_fn, tasks=tasks)

    if callable(name_or_fn):
        fn = name_or_fn
        return Workflow(name=fn.__name__, tasks=tasks)

    def decorator(fn: Callable[..., Any]) -> Workflow:
        return Workflow(name=fn.__name__, tasks=tasks)

    return decorator
