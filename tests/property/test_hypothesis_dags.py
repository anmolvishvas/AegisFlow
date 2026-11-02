"""Property-based tests for AegisFlow DAG compilation and topological ordering using Hypothesis."""

from hypothesis import given, settings
from hypothesis import strategies as st

from aegisflow.core.exceptions import CycleDetectedError
from aegisflow.core.models import TaskDefinition
from aegisflow.dsl.graph import DAGBuilder


@st.composite
def dag_nodes_and_edges(draw: st.DrawFn) -> tuple[list[str], list[tuple[str, str]]]:
    """Generate a random valid Directed Acyclic Graph (DAG).

    By enforcing edges only from lower-indexed nodes to higher-indexed nodes (u < v),
    the graph is guaranteed to be acyclic.
    """
    num_nodes = draw(st.integers(min_value=2, max_value=30))
    node_names = [f"task_{i}" for i in range(num_nodes)]

    edges: list[tuple[str, str]] = []
    # For each pair (i, j) with i < j, randomly decide whether to add an edge i -> j
    for i in range(num_nodes):
        for j in range(i + 1, num_nodes):
            if draw(st.booleans()):
                edges.append((node_names[i], node_names[j]))

    return node_names, edges


class TestHypothesisDAGProperties:
    """Hypothesis property tests validating DAG engine invariants."""

    @given(dag_nodes_and_edges())
    @settings(max_examples=100, deadline=None)
    def test_topological_sort_invariants(
        self, graph_data: tuple[list[str], list[tuple[str, str]]]
    ) -> None:
        """Property: Every dependency must precede dependent tasks in the topological ordering."""
        node_names, edges = graph_data
        builder = DAGBuilder(name="hypothesis_dag")

        # Map dependencies for each node
        deps_map: dict[str, set[str]] = {name: set() for name in node_names}
        for u, v in edges:
            deps_map[v].add(u)

        for name in node_names:
            builder.add_task(
                TaskDefinition(
                    task_id=name,
                    name=name,
                    fn=lambda: None,
                    is_async=False,
                    dependencies=deps_map[name],
                )
            )

        dag = builder.build()
        order = dag.topological_sort()

        # Invariant 1: All nodes must be present in the topological order exactly once
        assert set(order) == set(node_names)
        assert len(order) == len(node_names)

        # Invariant 2: For every edge u -> v, index(u) < index(v) in topological sort
        pos = {task_id: idx for idx, task_id in enumerate(order)}
        for u, v in edges:
            assert pos[u] < pos[v], f"Topological violation: {u} should precede {v} in {order}"

    @given(st.integers(min_value=2, max_value=15))
    @settings(max_examples=50, deadline=None)
    def test_cycle_detection_property(self, num_nodes: int) -> None:
        """Property: Any cycle (direct or indirect) must be detected and rejected."""
        builder = DAGBuilder(name="cyclic_graph")
        node_names = [f"node_{i}" for i in range(num_nodes)]

        # Construct a cycle: node_0 -> node_1 -> ... -> node_{n-1} -> node_0
        for i in range(num_nodes):
            upstream = node_names[(i - 1) % num_nodes]
            builder.add_task(
                TaskDefinition(
                    task_id=node_names[i],
                    name=node_names[i],
                    fn=lambda: None,
                    is_async=False,
                    dependencies={upstream},
                )
            )

        try:
            builder.build()
            raise AssertionError("Expected CycleDetectedError but build succeeded")
        except CycleDetectedError as exc:
            cycle = exc.context.get("cycle")
            assert cycle is not None
            assert len(cycle) >= 2

    @given(dag_nodes_and_edges())
    @settings(max_examples=50, deadline=None)
    def test_state_machine_transition_invariants(
        self, graph_data: tuple[list[str], list[tuple[str, str]]]
    ) -> None:
        """Property: Tasks only become ready once all upstream dependencies are strictly completed."""
        from aegisflow.core.events import (
            TaskCompleted,
            TaskScheduled,
            TaskStarted,
            WorkflowCompleted,
            WorkflowInitiated,
        )
        from aegisflow.core.states import WorkflowState
        from aegisflow.engine.state_machine import initial_state, transition

        node_names, edges = graph_data
        builder = DAGBuilder(name="state_prop_dag")
        deps_map: dict[str, set[str]] = {name: set() for name in node_names}
        for u, v in edges:
            deps_map[v].add(u)

        for name in node_names:
            builder.add_task(
                TaskDefinition(
                    task_id=name,
                    name=name,
                    fn=lambda: None,
                    is_async=False,
                    dependencies=deps_map[name],
                )
            )

        dag = builder.build()
        wf_def = dag.to_workflow_definition()
        w_id = "wf_prop_test"

        state = initial_state(w_id, wf_def.name)
        state, ready = transition(
            state, WorkflowInitiated(workflow_id=w_id, workflow_name=wf_def.name), wf_def
        )

        completed_tasks: set[str] = set()

        while ready:
            next_task_id = ready.pop()
            # Invariant: all dependencies of next_task_id MUST already be in completed_tasks
            task_deps = wf_def.tasks[next_task_id].dependencies
            assert task_deps.issubset(completed_tasks), (
                f"Task {next_task_id} became ready before dependencies {task_deps - completed_tasks} completed"
            )

            # Transition: Schedule -> Start -> Complete
            state, _ = transition(
                state,
                TaskScheduled(workflow_id=w_id, task_id=next_task_id, task_name=next_task_id),
                wf_def,
            )
            state, _ = transition(
                state,
                TaskStarted(workflow_id=w_id, task_id=next_task_id, worker_id="w1"),
                wf_def,
            )
            state, new_ready = transition(
                state,
                TaskCompleted(workflow_id=w_id, task_id=next_task_id, result="ok"),
                wf_def,
            )

            completed_tasks.add(next_task_id)
            ready.update(new_ready)

        # Transition: WorkflowCompleted
        state, _ = transition(
            state, WorkflowCompleted(workflow_id=w_id, output_data="done"), wf_def
        )
        assert state.status == WorkflowState.COMPLETED
        assert completed_tasks == set(node_names)
