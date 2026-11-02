"""Pure functional state machine for AegisFlow workflow execution and DAG transitions."""

from dataclasses import dataclass, field
from typing import Any

from aegisflow.core.events import (
    BaseEvent,
    TaskCompensated,
    TaskCompleted,
    TaskFailed,
    TaskRetried,
    TaskScheduled,
    TaskStarted,
    WorkflowCompensated,
    WorkflowCompensating,
    WorkflowCompleted,
    WorkflowFailed,
    WorkflowInitiated,
    WorkflowRunning,
)
from aegisflow.core.models import WorkflowDefinition
from aegisflow.core.states import TaskState, WorkflowState
from aegisflow.dsl.graph import DAG


@dataclass(frozen=True)
class WorkflowExecutionState:
    """Immutable snapshot of a workflow's execution progress."""

    workflow_id: str
    workflow_name: str
    status: WorkflowState = WorkflowState.PENDING
    task_states: dict[str, TaskState] = field(default_factory=dict)
    task_results: dict[str, Any] = field(default_factory=dict)
    task_errors: dict[str, str] = field(default_factory=dict)
    workflow_input: dict[str, Any] = field(default_factory=dict)
    workflow_output: Any = None
    version: int = 0

    @property
    def is_terminal(self) -> bool:
        """Return True if workflow is in a terminal state."""
        return self.status.is_terminal

    @property
    def is_successful(self) -> bool:
        """Return True if workflow completed successfully."""
        return self.status == WorkflowState.COMPLETED

    @property
    def completed_tasks(self) -> set[str]:
        """Return task IDs that have completed successfully."""
        return {
            task_id for task_id, state in self.task_states.items() if state == TaskState.COMPLETED
        }

    @property
    def running_tasks(self) -> set[str]:
        """Return task IDs that are currently running."""
        return {
            task_id for task_id, state in self.task_states.items() if state == TaskState.RUNNING
        }

    @property
    def failed_tasks(self) -> set[str]:
        """Return task IDs that have failed."""
        return {task_id for task_id, state in self.task_states.items() if state == TaskState.FAILED}


def initial_state(
    workflow_id: str,
    workflow_name: str,
    workflow_input: dict[str, Any] | None = None,
) -> WorkflowExecutionState:
    """Construct an initial pending state for a new workflow."""
    return WorkflowExecutionState(
        workflow_id=workflow_id,
        workflow_name=workflow_name,
        status=WorkflowState.PENDING,
        workflow_input=dict(workflow_input or {}),
        version=0,
    )


def transition(
    state: WorkflowExecutionState,
    event: BaseEvent,
    workflow_def: WorkflowDefinition,
) -> tuple[WorkflowExecutionState, set[str]]:
    """Pure functional state transition.

    Accepts the current execution state, a newly observed event, and the compiled
    workflow definition, returning a new execution state and a set of task IDs
    that are newly eligible to be scheduled.

    Args:
        state: Current immutable execution state.
        event: New domain event to process.
        workflow_def: Static definition of the DAG and tasks.

    Returns:
        Tuple of (new_state, ready_task_ids).
    """
    new_task_states = dict(state.task_states)
    new_task_results = dict(state.task_results)
    new_task_errors = dict(state.task_errors)
    new_status = state.status
    new_output = state.workflow_output
    new_input = dict(state.workflow_input)

    ready_tasks: set[str] = set()
    dag = DAG(name=workflow_def.name, tasks=workflow_def.tasks)

    if isinstance(event, WorkflowInitiated):
        new_status = WorkflowState.PENDING
        new_input = dict(event.input_data)
        # Root tasks with zero dependencies are ready
        completed = {t_id for t_id, s in new_task_states.items() if s == TaskState.COMPLETED}
        active_or_done = {
            t_id
            for t_id, s in new_task_states.items()
            if s
            in (
                TaskState.SCHEDULED,
                TaskState.RUNNING,
                TaskState.COMPLETED,
                TaskState.FAILED,
                TaskState.SKIPPED,
            )
        }
        ready_tasks = dag.get_ready_tasks(
            completed_tasks=completed, running_or_failed_tasks=active_or_done
        )

    elif isinstance(event, WorkflowRunning):
        new_status = WorkflowState.RUNNING
        completed = {t_id for t_id, s in new_task_states.items() if s == TaskState.COMPLETED}
        active_or_done = {
            t_id
            for t_id, s in new_task_states.items()
            if s
            in (
                TaskState.SCHEDULED,
                TaskState.RUNNING,
                TaskState.COMPLETED,
                TaskState.FAILED,
                TaskState.SKIPPED,
            )
        }
        ready_tasks = dag.get_ready_tasks(
            completed_tasks=completed, running_or_failed_tasks=active_or_done
        )

    elif isinstance(event, TaskScheduled):
        new_task_states[event.task_id] = TaskState.SCHEDULED

    elif isinstance(event, TaskStarted):
        new_task_states[event.task_id] = TaskState.RUNNING
        if new_status == WorkflowState.PENDING:
            new_status = WorkflowState.RUNNING

    elif isinstance(event, TaskCompleted):
        new_task_states[event.task_id] = TaskState.COMPLETED
        new_task_results[event.task_id] = event.result

        # Determine downstream tasks whose dependencies are satisfied
        completed = {t_id for t_id, s in new_task_states.items() if s == TaskState.COMPLETED}
        active_or_done = {
            t_id
            for t_id, s in new_task_states.items()
            if s
            in (
                TaskState.SCHEDULED,
                TaskState.RUNNING,
                TaskState.COMPLETED,
                TaskState.FAILED,
                TaskState.SKIPPED,
            )
        }
        ready_tasks = dag.get_ready_tasks(
            completed_tasks=completed, running_or_failed_tasks=active_or_done
        )

    elif isinstance(event, TaskFailed):
        new_task_states[event.task_id] = TaskState.FAILED
        new_task_errors[event.task_id] = event.error

    elif isinstance(event, TaskRetried):
        new_task_states[event.task_id] = TaskState.RETRYING

    elif isinstance(event, WorkflowCompleted):
        new_status = WorkflowState.COMPLETED
        new_output = event.output_data

    elif isinstance(event, WorkflowFailed):
        new_status = WorkflowState.FAILED
        new_task_errors["__workflow__"] = event.error

    elif isinstance(event, WorkflowCompensating):
        new_status = WorkflowState.COMPENSATING

    elif isinstance(event, WorkflowCompensated):
        new_status = WorkflowState.COMPENSATED

    elif isinstance(event, TaskCompensated):
        new_task_states[event.task_id] = TaskState.COMPENSATED

    new_state = WorkflowExecutionState(
        workflow_id=state.workflow_id,
        workflow_name=state.workflow_name,
        status=new_status,
        task_states=new_task_states,
        task_results=new_task_results,
        task_errors=new_task_errors,
        workflow_input=new_input,
        workflow_output=new_output,
        version=state.version + 1,
    )

    return new_state, ready_tasks
