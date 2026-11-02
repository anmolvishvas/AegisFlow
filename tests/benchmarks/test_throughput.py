"""Performance benchmarking suite for AegisFlow engine, WAL event store, and replay engine."""

import time
from pathlib import Path

import pytest

from aegisflow.core.events import TaskCompleted, TaskScheduled, TaskStarted, WorkflowInitiated
from aegisflow.core.models import TaskDefinition
from aegisflow.dsl.graph import DAGBuilder
from aegisflow.dsl.task import task
from aegisflow.dsl.workflow import workflow
from aegisflow.engine.orchestrator import WorkflowOrchestrator
from aegisflow.engine.replay import replay_workflow
from aegisflow.storage.sqlite import SQLiteStorage
from aegisflow.workers.daemon import WorkerSupervisorDaemon


@pytest.mark.asyncio
async def test_benchmark_sqlite_wal_event_throughput(tmp_path: Path) -> None:
    """Benchmark raw append throughput of the SQLite Write-Ahead Log event store."""
    db_file = tmp_path / "bench_wal.db"
    storage = SQLiteStorage(database_path=db_file)
    await storage.initialize()

    wf_id = "wf_bench_wal"
    event_count = 500

    # Initialize workflow parent row
    await storage.append_event(
        WorkflowInitiated(workflow_id=wf_id, workflow_name="ThroughputBench")
    )

    events = [
        TaskCompleted(
            workflow_id=wf_id,
            task_id=f"task_{i}",
            result={"idx": i, "payload": "data_chunk_sample"},
        )
        for i in range(event_count)
    ]

    start_time = time.perf_counter()
    for ev in events:
        await storage.append_event(ev)
    elapsed = time.perf_counter() - start_time

    throughput = event_count / elapsed
    print(
        f"\n[BENCHMARK] SQLite WAL Throughput: {throughput:.2f} events/sec ({event_count} events in {elapsed:.3f}s)"
    )

    # Assert minimum acceptable threshold (> 200 events/sec across all test platforms)
    assert throughput > 200.0

    await storage.close()


def test_benchmark_dag_compilation_and_sort_latency() -> None:
    """Benchmark Kahn's topological sort and DFS cycle detection on a large 100-node DAG."""
    builder = DAGBuilder(name="large_benchmark_dag")
    num_nodes = 100

    for i in range(num_nodes):
        # Each node depends on the previous 2 nodes where available
        deps = {f"node_{i - 1}"} if i > 0 else set()
        if i > 1:
            deps.add(f"node_{i - 2}")
        builder.add_task(
            TaskDefinition(
                task_id=f"node_{i}",
                name=f"node_{i}",
                fn=lambda: None,
                is_async=False,
                dependencies=deps,
            )
        )

    start_time = time.perf_counter()
    dag = builder.build()
    order = dag.topological_sort()
    elapsed = time.perf_counter() - start_time

    print(f"\n[BENCHMARK] DAG Compilation & Sort (100 nodes): {elapsed * 1000:.2f}ms")
    assert len(order) == num_nodes
    # Should compile and topologically sort well under 25 milliseconds
    assert elapsed < 0.025


def test_benchmark_deterministic_event_replay_speed() -> None:
    """Benchmark pure event replay fold speed over 2,000 historical WAL events."""
    builder = DAGBuilder(name="replay_bench_dag")
    num_tasks = 500

    for i in range(num_tasks):
        deps = {f"t_{i - 1}"} if i > 0 else set()
        builder.add_task(
            TaskDefinition(
                task_id=f"t_{i}",
                name=f"t_{i}",
                fn=lambda: None,
                is_async=False,
                dependencies=deps,
            )
        )

    dag = builder.build()
    wf_def = dag.to_workflow_definition()
    wf_id = "wf_replay_speed"

    # Generate synthetic event stream: Scheduled -> Started -> Completed for each task
    events = [WorkflowInitiated(workflow_id=wf_id, workflow_name=wf_def.name)]
    for i in range(num_tasks):
        t_id = f"t_{i}"
        events.append(TaskScheduled(workflow_id=wf_id, task_id=t_id, task_name=t_id))
        events.append(TaskStarted(workflow_id=wf_id, task_id=t_id, worker_id="w_replay"))
        events.append(TaskCompleted(workflow_id=wf_id, task_id=t_id, result=i))

    total_events = len(events)
    start_time = time.perf_counter()
    reconstructed_state = replay_workflow(wf_def, events)
    elapsed = time.perf_counter() - start_time

    replay_rate = total_events / elapsed
    print(
        f"\n[BENCHMARK] Event Replay Speed: {replay_rate:.2f} events/sec ({total_events} events in {elapsed * 1000:.2f}ms)"
    )

    assert len(reconstructed_state.completed_tasks) == num_tasks
    # In-memory pure fold should exceed 1,000 events/sec across test runners
    assert replay_rate > 1000.0


@pytest.mark.asyncio
async def test_benchmark_orchestration_dispatch_latency(tmp_path: Path) -> None:
    """Benchmark end-to-end task scheduling dispatch loop latency on a linear chain."""
    db_file = tmp_path / "bench_orch.db"
    storage = SQLiteStorage(database_path=db_file)
    await storage.initialize()

    daemon = WorkerSupervisorDaemon(event_store=storage, lease_manager=storage, max_concurrency=16)
    orchestrator = WorkflowOrchestrator(
        event_store=storage,
        lease_manager=storage,
        worker_daemon=daemon,
    )

    chain_len = 10
    tasks_list = []

    for i in range(chain_len):
        idx = i

        @task(name=f"chain_step_{idx}")
        def step(x: int = 0) -> int:
            return x + 1

        if tasks_list:
            step.after(tasks_list[-1])
        tasks_list.append(step)

    wf = workflow("ChainBenchmark", tasks=tasks_list)
    wf_def = wf.to_definition()
    wf_id = "wf_bench_chain_1"

    start_time = time.perf_counter()
    final_state = await orchestrator.run(wf_def, workflow_id=wf_id, x=0)
    elapsed = time.perf_counter() - start_time

    latency_per_step_ms = (elapsed / chain_len) * 1000
    print(
        f"\n[BENCHMARK] Orchestrator Task Hop Latency: {latency_per_step_ms:.2f}ms/task ({chain_len} tasks in {elapsed:.3f}s)"
    )

    assert final_state.workflow_output == chain_len
    # Each task hop (lease, start event, execute, complete event, transition) under 40ms locally
    assert latency_per_step_ms < 40.0

    await daemon.shutdown()
    await storage.close()
