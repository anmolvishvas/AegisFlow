"""Directed Acyclic Graph (DAG) builder, cycle detector, and topological resolver."""

from collections import deque

from aegisflow.core.exceptions import CycleDetectedError, TaskNotFoundError
from aegisflow.core.models import TaskDefinition, WorkflowDefinition


class DAG:
    """Represents a validated Directed Acyclic Graph of tasks."""

    def __init__(self, name: str, tasks: dict[str, TaskDefinition]) -> None:
        self.name = name
        self.tasks = tasks
        self._validate()

    def _validate(self) -> None:
        """Verify that all dependency IDs exist and the graph contains no cycles."""
        for task_id, task in self.tasks.items():
            for dep_id in task.dependencies:
                if dep_id not in self.tasks:
                    raise TaskNotFoundError(
                        f"Task '{task_id}' depends on undefined task '{dep_id}'"
                    )

        # Detect cycles using DFS with 3-color marking
        self.detect_cycles()

    def detect_cycles(self) -> None:
        """Detect circular dependencies using three-color depth-first search.

        Raises:
            CycleDetectedError: If a cycle is detected.
        """
        # 0 = WHITE (unvisited), 1 = GRAY (visiting), 2 = BLACK (visited)
        visited: dict[str, int] = {t_id: 0 for t_id in self.tasks}

        def dfs(node: str, stack: list[str]) -> None:
            visited[node] = 1  # Mark GRAY
            stack.append(node)

            for dep in self.tasks[node].dependencies:
                if visited[dep] == 1:
                    # Found back-edge to a node currently in the recursion stack
                    cycle_start_idx = stack.index(dep)
                    cycle_path = stack[cycle_start_idx:] + [dep]
                    raise CycleDetectedError(cycle_path)
                if visited[dep] == 0:
                    dfs(dep, stack)

            visited[node] = 2  # Mark BLACK
            stack.pop()

        for task_id in self.tasks:
            if visited[task_id] == 0:
                dfs(task_id, [])

    def topological_sort(self) -> list[str]:
        """Compute execution order using Kahn's algorithm.

        Returns:
            A list of task IDs in valid execution order where parents precede children.
        """
        # in-degree represents how many dependencies a task must wait for
        in_degree: dict[str, int] = {
            t_id: len(self.tasks[t_id].dependencies) for t_id in self.tasks
        }

        # Build adjacency list: parent -> list of dependents (children)
        dependents: dict[str, list[str]] = {t_id: [] for t_id in self.tasks}
        for t_id, task in self.tasks.items():
            for dep in task.dependencies:
                dependents[dep].append(t_id)

        # Queue nodes with 0 dependencies
        queue: deque[str] = deque([t_id for t_id, deg in in_degree.items() if deg == 0])
        sorted_order: list[str] = []

        while queue:
            node = queue.popleft()
            sorted_order.append(node)

            for dependent in dependents[node]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if len(sorted_order) != len(self.tasks):
            # Fallback if a cycle slipped through
            self.detect_cycles()

        return sorted_order

    def get_ready_tasks(
        self,
        completed_tasks: set[str],
        running_or_failed_tasks: set[str] | None = None,
    ) -> set[str]:
        """Return task IDs whose upstream dependencies are all completed.

        Args:
            completed_tasks: Set of task IDs that have completed successfully.
            running_or_failed_tasks: Set of task IDs already scheduled, running, or failed.

        Returns:
            Set of task IDs eligible to be scheduled immediately.
        """
        excluded = completed_tasks | (running_or_failed_tasks or set())
        ready: set[str] = set()

        for t_id, task in self.tasks.items():
            if t_id in excluded:
                continue
            if task.dependencies.issubset(completed_tasks):
                ready.add(t_id)

        return ready

    def to_workflow_definition(self) -> WorkflowDefinition:
        """Convert this validated DAG into a WorkflowDefinition."""
        return WorkflowDefinition(name=self.name, tasks=self.tasks)


class DAGBuilder:
    """Builder for constructing and validating a DAG incrementally."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._tasks: dict[str, TaskDefinition] = {}

    def add_task(self, task: TaskDefinition) -> "DAGBuilder":
        """Add a task definition to the builder."""
        self._tasks[task.task_id] = task
        return self

    def add_dependency(self, child_task_id: str, parent_task_id: str) -> "DAGBuilder":
        """Add an explicit dependency: child_task_id depends on parent_task_id."""
        if child_task_id not in self._tasks:
            raise TaskNotFoundError(f"Child task '{child_task_id}' not found in builder")
        task = self._tasks[child_task_id]
        new_deps = set(task.dependencies) | {parent_task_id}
        self._tasks[child_task_id] = TaskDefinition(
            task_id=task.task_id,
            name=task.name,
            fn=task.fn,
            is_async=task.is_async,
            retry_policy=task.retry_policy,
            timeout_seconds=task.timeout_seconds,
            compensate_fn=task.compensate_fn,
            dependencies=new_deps,
        )
        return self

    def build(self) -> DAG:
        """Validate and return the compiled DAG."""
        return DAG(name=self.name, tasks=self._tasks)
