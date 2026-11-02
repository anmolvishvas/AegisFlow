"""AegisFlow Quickstart Example: Defining and executing a durable 3-step DAG."""

import asyncio

from aegisflow import SQLiteStorage, WorkflowOrchestrator, task, workflow


# 1. Define tasks using the @task decorator
@task
def extract_metrics(endpoint: str) -> dict[str, list[int]]:
    print(f"[extract_metrics] Fetching raw sensor telemetry from: {endpoint}")
    return {"sensor_readings": [42, 58, 65, 82, 91, 105, 118]}


@task
def compute_statistics(extract_metrics: dict[str, list[int]]) -> dict[str, float]:
    readings = extract_metrics["sensor_readings"]
    avg = sum(readings) / len(readings)
    print(f"[compute_statistics] Processed {len(readings)} samples, average = {avg:.2f}")
    return {"average": avg, "peak": max(readings), "minimum": min(readings)}


@task
def format_alert(compute_statistics: dict[str, float]) -> str:
    peak = compute_statistics["peak"]
    status_label = "CRITICAL" if peak > 100 else "NORMAL"
    report = (
        f"System Status: {status_label} | Peak: {peak} | Avg: {compute_statistics['average']:.2f}"
    )
    print(f"[format_alert] Report: {report}")
    return report


# 2. Declare explicit DAG dependencies
compute_statistics.after(extract_metrics)
format_alert.after(compute_statistics)

# 3. Assemble into a named workflow
telemetry_workflow = workflow(
    "SensorTelemetryPipeline",
    tasks=[extract_metrics, compute_statistics, format_alert],
)


async def main() -> None:
    db_file = "telemetry_pipeline.db"
    print(f"[INFO] Initializing AegisFlow SQLite WAL storage: {db_file}")

    # 4. Initialize storage and orchestrator
    storage = SQLiteStorage(database_path=db_file)
    await storage.initialize()

    orchestrator = WorkflowOrchestrator(
        event_store=storage,
        lease_manager=storage,
    )

    # 5. Run workflow
    workflow_id = "sensor_run_101"
    print(f"\n[RUN] Launching workflow: {workflow_id}...")

    final_state = await orchestrator.run(
        telemetry_workflow.to_definition(),
        workflow_id=workflow_id,
        endpoint="https://sensors.edge.io/telemetry/v1",
    )

    print("\n" + "=" * 50)
    print(f"Workflow Finished! Final Status: {final_state.status.value}")
    print(f"Final Output: {final_state.workflow_output}")
    print("=" * 50)

    await storage.close()

    print("\nTip: Inspect the WAL event history for this workflow via the CLI:")
    print(f"   aegisflow status {workflow_id} --db {db_file}\n")


if __name__ == "__main__":
    asyncio.run(main())
