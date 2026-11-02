"""Unit tests for DAG construction, cycle detection, and topological sorting."""

import pytest

from aegisflow.core.exceptions import CycleDetectedError, TaskNotFoundError
from aegisflow.core.models import TaskDefinition
from aegisflow.dsl.graph import DAG, DAGBuilder


def _dummy_fn() -> None:
    pass


def _make_task(task_id: str, deps: set[str] | None = None) -> TaskDefinition:
    return TaskDefinition(
        task_id=task_id,
        name=task_id,
        fn=_dummy_fn,
        is_async=False,
        dependencies=deps or set(),
    )


def test_linear_dag_topological_sort() -> None:
    tasks = {
        "A": _make_task("A"),
        "B": _make_task("B", deps={"A"}),
        "C": _make_task("C", deps={"B"}),
    }
    dag = DAG(name="LinearDAG", tasks=tasks)
    order = dag.topological_sort()
    assert order == ["A", "B", "C"]


def test_diamond_fan_out_fan_in() -> None:
    # A -> B, A -> C, (B, C) -> D
    tasks = {
        "A": _make_task("A"),
        "B": _make_task("B", deps={"A"}),
        "C": _make_task("C", deps={"A"}),
        "D": _make_task("D", deps={"B", "C"}),
    }
    dag = DAG(name="DiamondDAG", tasks=tasks)
    order = dag.topological_sort()

    assert order[0] == "A"
    assert order[-1] == "D"
    assert set(order[1:3]) == {"B", "C"}


def test_cycle_detection_simple() -> None:
    # A -> B -> A
    tasks = {
        "A": _make_task("A", deps={"B"}),
        "B": _make_task("B", deps={"A"}),
    }
    with pytest.raises(CycleDetectedError) as exc_info:
        DAG(name="CycleDAG", tasks=tasks)
    assert "Circular dependency detected" in str(exc_info.value)


def test_missing_dependency_raises_error() -> None:
    tasks = {
        "A": _make_task("A", deps={"NonExistent"}),
    }
    with pytest.raises(TaskNotFoundError):
        DAG(name="InvalidDAG", tasks=tasks)


def test_get_ready_tasks() -> None:
    tasks = {
        "A": _make_task("A"),
        "B": _make_task("B", deps={"A"}),
        "C": _make_task("C", deps={"A"}),
        "D": _make_task("D", deps={"B", "C"}),
    }
    dag = DAG(name="ReadyDAG", tasks=tasks)

    # Initial state: only A has 0 dependencies
    assert dag.get_ready_tasks(completed_tasks=set()) == {"A"}

    # After A completes: B and C become ready
    assert dag.get_ready_tasks(completed_tasks={"A"}) == {"B", "C"}

    # If B completes but C is not: D is still not ready
    assert dag.get_ready_tasks(completed_tasks={"A", "B"}) == {"C"}

    # After both B and C complete: D becomes ready
    assert dag.get_ready_tasks(completed_tasks={"A", "B", "C"}) == {"D"}

    # Once D finishes: no more tasks are ready
    assert dag.get_ready_tasks(completed_tasks={"A", "B", "C", "D"}) == set()
