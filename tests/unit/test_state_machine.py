"""Unit tests for the pure functional state machine and replay engine."""

from aegisflow.core.events import (
    TaskCompleted,
    TaskFailed,
    TaskScheduled,
    TaskStarted,
    WorkflowCompleted,
    WorkflowFailed,
    WorkflowInitiated,
    WorkflowRunning,
)
from aegisflow.core.models import TaskDefinition, WorkflowDefinition
from aegisflow.core.states import TaskState, WorkflowState
from aegisflow.engine.replay import (
    get_resumable_tasks,
    get_runnable_tasks_after_recovery,
    replay_workflow,
)
from aegisflow.engine.state_machine import initial_state, transition


def _dummy_fn(*args: object, **kwargs: object) -> int:
    return 42


def _make_sample_workflow() -> WorkflowDefinition:
    """Create a sample diamond DAG: A -> B & C -> D."""
    task_a = TaskDefinition(task_id="A", name="A", fn=_dummy_fn, is_async=False)
    task_b = TaskDefinition(task_id="B", name="B", fn=_dummy_fn, is_async=False, dependencies={"A"})
    task_c = TaskDefinition(task_id="C", name="C", fn=_dummy_fn, is_async=False, dependencies={"A"})
    task_d = TaskDefinition(
        task_id="D", name="D", fn=_dummy_fn, is_async=False, dependencies={"B", "C"}
    )
    return WorkflowDefinition(
        name="diamond_flow",
        tasks={"A": task_a, "B": task_b, "C": task_c, "D": task_d},
    )


def test_initial_state() -> None:
    state = initial_state(
        workflow_id="wf_1",
        workflow_name="test_wf",
        workflow_input={"param": "value"},
    )
    assert state.workflow_id == "wf_1"
    assert state.workflow_name == "test_wf"
    assert state.status == WorkflowState.PENDING
    assert state.workflow_input == {"param": "value"}
    assert state.version == 0
    assert not state.is_terminal


def test_transition_workflow_initiated_identifies_root_tasks() -> None:
    wf_def = _make_sample_workflow()
    state = initial_state("wf_1", wf_def.name)

    init_event = WorkflowInitiated(
        workflow_id="wf_1",
        workflow_name=wf_def.name,
        input_data={"seed": 10},
    )
    new_state, ready = transition(state, init_event, wf_def)

    assert new_state.status == WorkflowState.PENDING
    assert new_state.workflow_input == {"seed": 10}
    assert new_state.version == 1
    assert ready == {"A"}


def test_diamond_dag_dependency_resolution() -> None:
    wf_def = _make_sample_workflow()
    state = initial_state("wf_1", wf_def.name)

    # 1. Initiated
    state, ready = transition(
        state,
        WorkflowInitiated(workflow_id="wf_1", workflow_name=wf_def.name),
        wf_def,
    )
    assert ready == {"A"}

    # 2. Workflow Running
    state, _ = transition(state, WorkflowRunning(workflow_id="wf_1"), wf_def)
    assert state.status == WorkflowState.RUNNING

    # 3. Task A scheduled and started
    state, _ = transition(
        state,
        TaskScheduled(workflow_id="wf_1", task_id="A", task_name="A"),
        wf_def,
    )
    assert state.task_states["A"] == TaskState.SCHEDULED

    state, _ = transition(
        state,
        TaskStarted(workflow_id="wf_1", task_id="A", worker_id="w_1"),
        wf_def,
    )
    assert state.task_states["A"] == TaskState.RUNNING

    # 4. Task A completed -> fan-out triggers B and C
    state, ready = transition(
        state,
        TaskCompleted(workflow_id="wf_1", task_id="A", result=100),
        wf_def,
    )
    assert state.task_states["A"] == TaskState.COMPLETED
    assert state.task_results["A"] == 100
    assert ready == {"B", "C"}

    # 5. Tasks B and C scheduled (both were ready from fan-out)
    state, _ = transition(
        state,
        TaskScheduled(workflow_id="wf_1", task_id="B", task_name="B"),
        wf_def,
    )
    state, _ = transition(
        state,
        TaskScheduled(workflow_id="wf_1", task_id="C", task_name="C"),
        wf_def,
    )

    # Task B completes (D should NOT be ready yet because C is still pending)
    state, ready = transition(
        state,
        TaskCompleted(workflow_id="wf_1", task_id="B", result=200),
        wf_def,
    )
    assert state.task_states["B"] == TaskState.COMPLETED
    assert ready == set()

    # 6. Task C completes -> fan-in triggers D!
    state, ready = transition(
        state,
        TaskCompleted(workflow_id="wf_1", task_id="C", result=300),
        wf_def,
    )
    assert state.task_states["C"] == TaskState.COMPLETED
    assert ready == {"D"}

    # 7. Task D scheduled and completed
    state, _ = transition(
        state,
        TaskScheduled(workflow_id="wf_1", task_id="D", task_name="D"),
        wf_def,
    )
    state, ready = transition(
        state,
        TaskCompleted(workflow_id="wf_1", task_id="D", result=500),
        wf_def,
    )
    assert state.task_states["D"] == TaskState.COMPLETED
    assert ready == set()
    assert state.completed_tasks == {"A", "B", "C", "D"}

    # 8. WorkflowCompleted
    state, _ = transition(
        state,
        WorkflowCompleted(workflow_id="wf_1", output_data=500),
        wf_def,
    )
    assert state.status == WorkflowState.COMPLETED
    assert state.is_successful
    assert state.is_terminal
    assert state.workflow_output == 500


def test_transition_task_failure() -> None:
    wf_def = _make_sample_workflow()
    state = initial_state("wf_1", wf_def.name)

    state, _ = transition(
        state,
        TaskFailed(
            workflow_id="wf_1",
            task_id="A",
            error="DB connection refused",
            traceback="Traceback...",
        ),
        wf_def,
    )

    assert state.task_states["A"] == TaskState.FAILED
    assert state.task_errors["A"] == "DB connection refused"
    assert "A" in state.failed_tasks

    state, _ = transition(
        state,
        WorkflowFailed(workflow_id="wf_1", error="Workflow abort"),
        wf_def,
    )
    assert state.status == WorkflowState.FAILED
    assert state.is_terminal
    assert not state.is_successful


def test_state_immutability() -> None:
    wf_def = _make_sample_workflow()
    state_v0 = initial_state("wf_1", wf_def.name)

    state_v1, _ = transition(
        state_v0,
        WorkflowInitiated(workflow_id="wf_1", workflow_name=wf_def.name),
        wf_def,
    )

    assert state_v0.version == 0
    assert state_v1.version == 1
    assert state_v0 is not state_v1
    assert state_v0.task_states == {}


def test_replay_workflow_reconstruction() -> None:
    wf_def = _make_sample_workflow()
    events = [
        WorkflowInitiated(workflow_id="wf_replay", workflow_name=wf_def.name),
        WorkflowRunning(workflow_id="wf_replay"),
        TaskScheduled(workflow_id="wf_replay", task_id="A", task_name="A"),
        TaskStarted(workflow_id="wf_replay", task_id="A", worker_id="w_1"),
        TaskCompleted(workflow_id="wf_replay", task_id="A", result=99),
    ]

    replayed_state = replay_workflow(wf_def, events)

    assert replayed_state.workflow_id == "wf_replay"
    assert replayed_state.status == WorkflowState.RUNNING
    assert replayed_state.completed_tasks == {"A"}
    assert replayed_state.task_results["A"] == 99

    resumable = get_resumable_tasks(replayed_state, wf_def)
    assert resumable == {"B", "C", "D"}

    runnable = get_runnable_tasks_after_recovery(replayed_state, wf_def)
    assert runnable == {"B", "C"}
