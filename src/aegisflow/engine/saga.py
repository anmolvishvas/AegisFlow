"""Saga compensation execution engine coordinating reverse topological rollbacks."""

import asyncio
import inspect
import logging
from typing import Any

from aegisflow.core.events import (
    TaskCompensated,
    WorkflowCompensated,
    WorkflowCompensating,
    WorkflowDeadLettered,
)
from aegisflow.core.exceptions import SagaCompensationError
from aegisflow.core.models import WorkflowDefinition
from aegisflow.dsl.graph import DAG
from aegisflow.storage.base import EventStoreInterface

logger = logging.getLogger(__name__)


class SagaCoordinator:
    """Manages transactional compensation rollbacks for failed multi-step workflows."""

    @staticmethod
    def has_compensations(
        workflow_def: WorkflowDefinition,
        completed_tasks: set[str],
    ) -> bool:
        """Check if any completed task in the workflow has a registered compensation function."""
        return any(
            workflow_def.get_task(task_id).compensate_fn is not None
            for task_id in completed_tasks
            if task_id in workflow_def.tasks
        )

    @staticmethod
    def get_compensation_order(
        workflow_def: WorkflowDefinition,
        completed_tasks: set[str],
    ) -> list[str]:
        """Compute the reverse topological order for tasks eligible for compensation.

        Guarantees that downstream operations are rolled back before their upstream
        dependencies (e.g. refunding a payment before releasing reserved inventory).

        Args:
            workflow_def: Static DAG definition of the workflow.
            completed_tasks: Set of task IDs that completed successfully.

        Returns:
            List of task IDs ordered in reverse topological sequence.
        """
        dag = DAG(workflow_def.name, workflow_def.tasks)
        topo_order = dag.topological_sort()
        reversed_order = list(reversed(topo_order))

        return [
            task_id
            for task_id in reversed_order
            if task_id in completed_tasks
            and workflow_def.get_task(task_id).compensate_fn is not None
        ]

    async def execute_compensations(
        self,
        workflow_def: WorkflowDefinition,
        workflow_id: str,
        completed_tasks: set[str],
        task_results: dict[str, Any],
        event_store: EventStoreInterface,
        reason: str,
    ) -> list[str]:
        """Execute all registered compensation handlers in reverse topological order.

        Args:
            workflow_def: Workflow DAG definition.
            workflow_id: Unique ID of the failing workflow run.
            completed_tasks: Tasks that finished prior to the unrecoverable failure.
            task_results: Outputs from previously completed tasks.
            event_store: Storage interface for appending rollback events.
            reason: Root cause failure description.

        Returns:
            List of task IDs that were successfully compensated.

        Raises:
            SagaCompensationError: If any compensation callable fails.
        """
        compensation_order = self.get_compensation_order(workflow_def, completed_tasks)
        if not compensation_order:
            return []

        # 1. Emit WorkflowCompensating event
        await event_store.append_event(WorkflowCompensating(workflow_id=workflow_id, reason=reason))

        compensated_tasks: list[str] = []

        # 2. Execute each compensation in strict reverse topological order
        for task_id in compensation_order:
            task_def = workflow_def.get_task(task_id)
            compensate_fn = task_def.compensate_fn
            if compensate_fn is None:
                continue

            result = task_results.get(task_id)

            try:
                sig = inspect.signature(compensate_fn)
                num_params = len(sig.parameters)

                if num_params > 0:
                    if asyncio.iscoroutinefunction(compensate_fn):
                        comp_res = await compensate_fn(result)
                    else:
                        comp_res = compensate_fn(result)
                else:
                    if asyncio.iscoroutinefunction(compensate_fn):
                        comp_res = await compensate_fn()
                    else:
                        comp_res = compensate_fn()

                await event_store.append_event(
                    TaskCompensated(
                        workflow_id=workflow_id,
                        task_id=task_id,
                        result=comp_res,
                    )
                )
                compensated_tasks.append(task_id)

            except Exception as exc:
                err_msg = f"Saga compensation for task '{task_id}' failed: {exc}"
                logger.error(err_msg, exc_info=True)

                await event_store.append_event(
                    WorkflowDeadLettered(
                        workflow_id=workflow_id,
                        reason=err_msg,
                        error=str(exc),
                    )
                )
                raise SagaCompensationError(err_msg) from exc

        # 3. Emit WorkflowCompensated on full rollback success
        await event_store.append_event(WorkflowCompensated(workflow_id=workflow_id))

        return compensated_tasks
