"""AegisFlow Distributed Saga Rollback Example.

Demonstrates automatic, reverse topological order compensation rollbacks
when a downstream step fails.
"""

import asyncio

from aegisflow import SQLiteStorage, WorkflowOrchestrator, task, workflow


# 1. Define tasks with compensation functions
@task(
    compensate=lambda order_id: print(
        f"[COMPENSATE] Voiding credit card authorization for order: {order_id}"
    )
)
def authorize_payment(order_id: str, amount: float) -> str:
    print(f"[STEP 1] Authorized ${amount:.2f} for order {order_id}")
    return f"AUTH_TX_{order_id}_SUCCESS"


@task(
    compensate=lambda reserve_inventory: print(
        f"[COMPENSATE] Releasing warehouse hold on SKU: {reserve_inventory['sku']} (qty {reserve_inventory['quantity']})"
    )
)
def reserve_inventory(order_id: str) -> dict[str, str | int]:
    print(f"[STEP 2] Reserved SKU ITEM-999 for order {order_id}")
    return {"sku": "ITEM-999", "quantity": 1, "status": "HOLD"}


@task
def dispatch_courier(reserve_inventory: dict[str, str | int]) -> None:
    print("[STEP 3] Attempting to book courier...")
    # Simulate external carrier failure
    raise RuntimeError("Carrier API timeout: No drivers available in region!")


# 2. Chain dependencies: Step 1 -> Step 2 -> Step 3
reserve_inventory.after(authorize_payment)
dispatch_courier.after(reserve_inventory)

# 3. Assemble workflow
order_workflow = workflow(
    "ECommerceOrderSaga",
    tasks=[authorize_payment, reserve_inventory, dispatch_courier],
)


async def main() -> None:
    db_file = "ecommerce_saga.db"
    print(f"[INFO] Initializing AegisFlow SQLite WAL storage: {db_file}")

    storage = SQLiteStorage(database_path=db_file)
    await storage.initialize()

    orchestrator = WorkflowOrchestrator(
        event_store=storage,
        lease_manager=storage,
    )

    workflow_id = "order_1001"
    print(f"\n[RUN] Executing Order Saga: {workflow_id}...")

    # Step 3 will fail, triggering automated saga compensation in reverse order:
    # 1. reserve_inventory is compensated FIRST
    # 2. authorize_payment is compensated SECOND
    final_state = await orchestrator.run(
        order_workflow.to_definition(),
        workflow_id=workflow_id,
        order_id="ORD-2026-X4",
        amount=149.99,
    )

    print("\n" + "=" * 55)
    print(f"Workflow Status: {final_state.status.value}")
    print(f"Compensated Tasks: {final_state.compensated_tasks}")
    print(f"Failed Tasks: {final_state.failed_tasks}")
    print("=" * 55)

    await storage.close()

    print("\nTip: View the compensation events in the event store via CLI:")
    print(f"   aegisflow status {workflow_id} --db {db_file}\n")


if __name__ == "__main__":
    asyncio.run(main())
