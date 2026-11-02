"""Main event dispatch loop, topological task scheduling, saga rollbacks, and recovery."""

import asyncio
import inspect
import logging
from typing import Any
from uuid import uuid4

from aegisflow.core.events import (
    TaskCompleted,
    TaskFailed,
    TaskRetried,
    TaskScheduled,
    WorkflowCompensated,
    WorkflowCompleted,
    WorkflowDeadLettered,
    WorkflowFailed,
    WorkflowInitiated,
    WorkflowRunning,
)
from aegisflow.core.exceptions import SagaCompensationError, WorkflowNotFoundError
from aegisflow.core.models import TaskDefinition, WorkflowDefinition
from aegisflow.engine.dlq import DeadLetterManager
from aegisflow.engine.replay import get_runnable_tasks_after_recovery, replay_workflow
from aegisflow.engine.retry import RetryScheduler
from aegisflow.engine.saga import SagaCoordinator
from aegisflow.engine.state_machine import WorkflowExecutionState, initial_state, transition
from aegisflow.observability.telemetry import telemetry
from aegisflow.storage.base import EventStoreInterface, LeaseManagerInterface
from aegisflow.workers.base_runner import TaskExecutionResult
from aegisflow.workers.daemon import WorkerSupervisorDaemon

logger = logging.getLogger(__name__)


def resolve_task_inputs(
    task_def: TaskDefinition,
    workflow_input: dict[str, Any],
    task_results: dict[str, Any],
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Determine runtime positional and keyword arguments for a task callable.

    For root tasks without dependencies, arguments are extracted from workflow_input.
    For dependent tasks, outputs from upstream parent tasks are injected either
    by parameter name matching or positionally in sorted dependency order.

    Args:
        task_def: Metadata and callable definition for the task.
        workflow_input: Global workflow input payload.
        task_results: Map of completed task IDs to their returned outputs.

    Returns:
        Tuple of (args, kwargs) to pass to the task callable.
    """
    sig = inspect.signature(task_def.fn)
    param_names = set(sig.parameters.keys())

    # 1. Dependent task: inject upstream results
    if task_def.dependencies:
        matching_kwargs = {
            dep: task_results[dep]
            for dep in task_def.dependencies
            if dep in param_names and dep in task_results
        }
        if matching_kwargs and len(matching_kwargs) == len(task_def.dependencies):
            return (), matching_kwargs

        if len(task_def.dependencies) == 1:
            dep_id = next(iter(task_def.dependencies))
            return (task_results.get(dep_id),), {}

        # Multiple dependencies without matching parameter names: sorted positional args
        sorted_deps = sorted(task_def.dependencies)
        args = tuple(task_results.get(dep) for dep in sorted_deps)
        return args, {}

    # 2. Root task: extract from workflow_input
    if "args" in workflow_input and isinstance(workflow_input["args"], (tuple, list)):
        raw_args = tuple(workflow_input["args"])
    else:
        raw_args = ()

    kwargs = {k: v for k, v in workflow_input.items() if k != "args"}
    return raw_args, kwargs


def compute_workflow_output(
    workflow_def: WorkflowDefinition,
    task_results: dict[str, Any],
) -> Any:
    """Compute the final return value for a completed workflow.

    Returns the output of the single leaf task (out-degree 0 in the DAG),
    or a dictionary mapping leaf task IDs to their respective outputs.

    Args:
        workflow_def: Compiled workflow definition.
        task_results: Map of completed task results.

    Returns:
        Leaf task result or map of leaf results.
    """
    all_dependencies = {dep for task in workflow_def.tasks.values() for dep in task.dependencies}
    leaves = [t_id for t_id in workflow_def.tasks if t_id not in all_dependencies]

    if len(leaves) == 1:
        return task_results.get(leaves[0])
    return {leaf: task_results.get(leaf) for leaf in leaves}


class WorkflowOrchestrator:
    """Orchestrates event-driven DAG execution, retries, saga rollbacks, and recovery."""

    def __init__(
        self,
        event_store: EventStoreInterface,
        lease_manager: LeaseManagerInterface,
        worker_daemon: WorkerSupervisorDaemon | None = None,
        lease_ttl_seconds: float = 15.0,
        retry_scheduler: RetryScheduler | None = None,
        saga_coordinator: SagaCoordinator | None = None,
        dlq_manager: DeadLetterManager | None = None,
        enable_dlq_on_failure: bool = False,
    ) -> None:
        self.event_store = event_store
        self.lease_manager = lease_manager
        self.worker_daemon = worker_daemon or WorkerSupervisorDaemon(
            lease_manager=lease_manager,
            event_store=event_store,
        )
        self.lease_ttl_seconds = lease_ttl_seconds
        self.retry_scheduler = retry_scheduler or RetryScheduler()
        self.saga_coordinator = saga_coordinator or SagaCoordinator()
        self.dlq_manager = dlq_manager or DeadLetterManager(event_store=event_store)
        self.enable_dlq_on_failure = enable_dlq_on_failure

    async def run(
        self,
        workflow_def: WorkflowDefinition,
        *args: Any,
        workflow_id: str | None = None,
        **kwargs: Any,
    ) -> WorkflowExecutionState:
        """Execute a workflow definition from start to finish.

        Emits lifecycle events to the Write-Ahead Log, schedules tasks topographically
        as their dependencies succeed, retries transient errors, and returns the final execution state.

        Args:
            workflow_def: Compiled DAG workflow definition.
            *args: Positional arguments for root tasks.
            workflow_id: Optional unique workflow run ID (generated if omitted).
            **kwargs: Keyword arguments for root tasks or global workflow context.

        Returns:
            Terminal WorkflowExecutionState.
        """
        w_id = workflow_id or str(uuid4())
        input_data: dict[str, Any] = dict(kwargs)
        if args:
            input_data["args"] = args

        # 1. Emit WorkflowInitiated
        initiated_event = WorkflowInitiated(
            workflow_id=w_id,
            workflow_name=workflow_def.name,
            input_data=input_data,
        )
        await self.event_store.append_event(initiated_event)

        state = initial_state(w_id, workflow_def.name, input_data)
        state, ready_tasks = transition(state, initiated_event, workflow_def)

        # 2. Emit WorkflowRunning
        running_event = WorkflowRunning(workflow_id=w_id)
        await self.event_store.append_event(running_event)
        state, _ = transition(state, running_event, workflow_def)

        # 3. Enter topological execution loop
        with telemetry.trace_workflow(w_id, workflow_def.name):
            return await self._execute_loop(state, workflow_def, ready_tasks)

    async def resume(
        self,
        workflow_id: str,
        workflow_def: WorkflowDefinition,
    ) -> WorkflowExecutionState:
        """Resume an incomplete or crashed workflow run from its event journal.

        Skips completed tasks with their cached results, re-schedules pending or
        interrupted tasks, and runs the remaining DAG to completion.

        Args:
            workflow_id: ID of the workflow to resume.
            workflow_def: Compiled DAG definition of the workflow.

        Returns:
            Terminal WorkflowExecutionState.

        Raises:
            WorkflowNotFoundError: If no events exist for the given workflow ID.
        """
        events = await self.event_store.get_events(workflow_id)
        if not events:
            raise WorkflowNotFoundError(f"Workflow run '{workflow_id}' not found in event store")

        state = replay_workflow(workflow_def, events)
        if state.is_terminal:
            logger.info("Workflow '%s' is already in terminal state %s", workflow_id, state.status)
            return state

        runnable_tasks = get_runnable_tasks_after_recovery(state, workflow_def)
        return await self._execute_loop(state, workflow_def, runnable_tasks)

    async def _execute_loop(
        self,
        initial_state: WorkflowExecutionState,
        workflow_def: WorkflowDefinition,
        ready_tasks: set[str],
    ) -> WorkflowExecutionState:
        """Internal event loop coordinating concurrent task dispatch, retries, and compensations."""
        state = initial_state
        w_id = state.workflow_id

        active_tasks: dict[str, asyncio.Task[TaskExecutionResult]] = {}
        task_attempts: dict[str, int] = {}

        async def _dispatch_task(task_id: str) -> None:
            nonlocal state
            task_def = workflow_def.get_task(task_id)
            t_args, t_kwargs = resolve_task_inputs(
                task_def, state.workflow_input, state.task_results
            )

            scheduled_event = TaskScheduled(
                workflow_id=w_id,
                task_id=task_id,
                task_name=task_def.name,
                input_args=t_args,
                input_kwargs=t_kwargs,
            )
            await self.event_store.append_event(scheduled_event)
            state, _ = transition(state, scheduled_event, workflow_def)

            async def _traced_execute() -> TaskExecutionResult:
                with telemetry.trace_task(task_id, w_id):
                    return await self.worker_daemon.execute_task(
                        task_def=task_def,
                        workflow_id=w_id,
                        args=t_args,
                        kwargs=t_kwargs,
                        lease_ttl_seconds=self.lease_ttl_seconds,
                    )

            task_future = asyncio.create_task(_traced_execute())
            active_tasks[task_id] = task_future

        # Schedule initial ready tasks
        for task_id in ready_tasks:
            await _dispatch_task(task_id)

        # Loop until all tasks finish or an unhandled failure aborts
        while active_tasks:
            done_futures, _ = await asyncio.wait(
                active_tasks.values(),
                return_when=asyncio.FIRST_COMPLETED,
            )

            # Map finished futures back to task IDs
            finished_task_ids = [
                t_id for t_id, fut in list(active_tasks.items()) if fut in done_futures
            ]

            for task_id in finished_task_ids:
                fut = active_tasks.pop(task_id)
                exec_result = await fut

                if exec_result.success:
                    completed_event = TaskCompleted(
                        workflow_id=w_id,
                        task_id=task_id,
                        result=exec_result.result,
                    )
                    state, new_ready = transition(state, completed_event, workflow_def)

                    for next_task_id in new_ready:
                        if next_task_id not in active_tasks:
                            await _dispatch_task(next_task_id)
                else:
                    task_def = workflow_def.get_task(task_id)
                    task_attempts[task_id] = task_attempts.get(task_id, 0) + 1
                    attempt = task_attempts[task_id]

                    # 1. Attempt retry if eligible
                    if self.retry_scheduler.should_retry(attempt, task_def.retry_policy):
                        delay, next_retry_at = self.retry_scheduler.calculate_retry(
                            attempt, task_def.retry_policy
                        )
                        retried_event = TaskRetried(
                            workflow_id=w_id,
                            task_id=task_id,
                            attempt=attempt,
                            next_retry_at=next_retry_at,
                        )
                        await self.event_store.append_event(retried_event)
                        state, _ = transition(state, retried_event, workflow_def)

                        async def _dispatch_retry_task(
                            t_id: str,
                            d: float,
                            snapshot_state: WorkflowExecutionState,
                        ) -> TaskExecutionResult:
                            await self.retry_scheduler.wait_for_retry(d)
                            t_def = workflow_def.get_task(t_id)
                            t_args, t_kwargs = resolve_task_inputs(
                                t_def,
                                snapshot_state.workflow_input,
                                snapshot_state.task_results,
                            )
                            return await self.worker_daemon.execute_task(
                                task_def=t_def,
                                workflow_id=w_id,
                                args=t_args,
                                kwargs=t_kwargs,
                                lease_ttl_seconds=self.lease_ttl_seconds,
                            )

                        active_tasks[task_id] = asyncio.create_task(
                            _dispatch_retry_task(task_id, delay, state)
                        )
                    else:
                        # 2. Retries exhausted: permanent failure
                        failed_event = TaskFailed(
                            workflow_id=w_id,
                            task_id=task_id,
                            error=exec_result.error or "Task execution failed",
                            traceback=exec_result.traceback,
                            is_retryable=False,
                        )
                        state, _ = transition(state, failed_event, workflow_def)

                        # Cancel remaining running tasks
                        for running_fut in active_tasks.values():
                            running_fut.cancel()
                        active_tasks.clear()

                        # 3. Check for saga compensation
                        if self.saga_coordinator.has_compensations(
                            workflow_def, state.completed_tasks
                        ):
                            try:
                                await self.saga_coordinator.execute_compensations(
                                    workflow_def=workflow_def,
                                    workflow_id=w_id,
                                    completed_tasks=state.completed_tasks,
                                    task_results=state.task_results,
                                    event_store=self.event_store,
                                    reason=f"Task '{task_id}' failed: {exec_result.error}",
                                )
                                state, _ = transition(
                                    state,
                                    WorkflowCompensated(workflow_id=w_id),
                                    workflow_def,
                                )
                            except SagaCompensationError as comp_exc:
                                dlq_event = WorkflowDeadLettered(
                                    workflow_id=w_id,
                                    reason=f"Saga compensation error: {comp_exc}",
                                    error=str(comp_exc),
                                )
                                state, _ = transition(state, dlq_event, workflow_def)
                                await self.dlq_manager.notify_dead_letter(
                                    w_id,
                                    f"Saga compensation error: {comp_exc}",
                                    str(comp_exc),
                                )
                            return state

                        # 4. No compensation: check DLQ or fail
                        if self.enable_dlq_on_failure:
                            dlq_event = WorkflowDeadLettered(
                                workflow_id=w_id,
                                reason=f"Task '{task_id}' exhausted {attempt} attempts: {exec_result.error}",
                                error=exec_result.error,
                            )
                            await self.event_store.append_event(dlq_event)
                            state, _ = transition(state, dlq_event, workflow_def)
                            await self.dlq_manager.notify_dead_letter(
                                w_id, dlq_event.reason, exec_result.error
                            )
                        else:
                            wf_failed_event = WorkflowFailed(
                                workflow_id=w_id,
                                error=f"Task '{task_id}' failed after {attempt} attempts: {exec_result.error}",
                                traceback=exec_result.traceback,
                            )
                            await self.event_store.append_event(wf_failed_event)
                            state, _ = transition(state, wf_failed_event, workflow_def)

                        return state

        # 4. Check if workflow is complete
        if state.completed_tasks == set(workflow_def.tasks.keys()):
            final_output = compute_workflow_output(workflow_def, state.task_results)
            completed_wf_event = WorkflowCompleted(
                workflow_id=w_id,
                output_data=final_output,
            )
            await self.event_store.append_event(completed_wf_event)
            state, _ = transition(state, completed_wf_event, workflow_def)

        return state
