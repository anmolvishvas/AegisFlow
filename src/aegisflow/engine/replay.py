"""Deterministic event journal replay and workflow state reconstruction."""

from collections.abc import Sequence

from aegisflow.core.events import BaseEvent
from aegisflow.core.models import WorkflowDefinition
from aegisflow.engine.state_machine import WorkflowExecutionState, initial_state, transition


def replay_workflow(
    workflow_def: WorkflowDefinition,
    events: Sequence[BaseEvent],
) -> WorkflowExecutionState:
    """Reconstruct workflow execution state by folding historical events in sequence.

    Deterministic and pure: yields the exact same execution state regardless
    of execution environment or timing.

    Args:
        workflow_def: Static DAG definition of the workflow.
        events: Chronological sequence of events recorded in the WAL.

    Returns:
        Reconstructed WorkflowExecutionState.
    """
    if not events:
        return initial_state(
            workflow_id="",
            workflow_name=workflow_def.name,
        )

    workflow_id = events[0].workflow_id
    current_state = initial_state(
        workflow_id=workflow_id,
        workflow_name=workflow_def.name,
    )

    for event in events:
        current_state, _ = transition(current_state, event, workflow_def)

    return current_state


def get_resumable_tasks(
    state: WorkflowExecutionState,
    workflow_def: WorkflowDefinition,
) -> set[str]:
    """Identify tasks that need execution or re-execution upon crash recovery.

    Completed tasks are strictly excluded to preserve idempotency and prevent
    duplicate side effects.

    Args:
        state: Current reconstructed execution state.
        workflow_def: Workflow DAG definition.

    Returns:
        Set of task IDs that are incomplete.
    """
    completed = state.completed_tasks
    all_tasks = set(workflow_def.tasks.keys())
    return all_tasks - completed


def get_runnable_tasks_after_recovery(
    state: WorkflowExecutionState,
    workflow_def: WorkflowDefinition,
) -> set[str]:
    """Return task IDs whose upstream dependencies are satisfied and are not completed.

    Any tasks that were previously marked SCHEDULED or RUNNING before the crash
    are reset to eligible for scheduling once their lease expires.

    Args:
        state: Reconstructed execution state.
        workflow_def: Workflow DAG definition.

    Returns:
        Set of task IDs ready to be scheduled immediately.
    """
    completed = state.completed_tasks
    ready: set[str] = set()

    for task_id, task in workflow_def.tasks.items():
        if task_id in completed:
            continue
        # If all dependencies completed, this task can be run
        if task.dependencies.issubset(completed):
            ready.add(task_id)

    return ready
