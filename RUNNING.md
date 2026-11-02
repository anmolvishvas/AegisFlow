# How to Run and Use AegisFlow Locally ⚡🛡️

This guide covers everything you need to set up, execute workflows, inspect execution history via the CLI, launch the Web Dashboard, and run tests and benchmarks locally.

---

## 1. Prerequisites

- **Python:** Version `3.11`, `3.12`, or `3.13`
- **Git:** Installed and configured
- **Operating System:** Windows, macOS, or Linux
- **Database:** Zero external servers needed — uses embedded SQLite in Write-Ahead Log (WAL) mode.

---

## 2. Installation & Environment Setup

### Step 1: Navigate to Project Directory
```bash
cd AegisFlow
```

### Step 2: (Optional but Recommended) Create a Virtual Environment
**On Windows (PowerShell):**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

**On macOS / Linux:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Step 3: Install AegisFlow in Editable Mode
Install the package with all optional dependencies for development, benchmarking, CLI, and dashboard:

```bash
pip install -e ".[dev]"
```

### Step 4: Verify Installation
Verify that the `aegisflow` CLI command is accessible:

```bash
aegisflow version
# Expected output: AegisFlow version 0.1.0
```

---

## 3. Running Pre-Built Examples

AegisFlow includes runnable scripts under the `examples/` directory:

### Example 1: 3-Step ETL Pipeline (`examples/01_quickstart.py`)
Executes a 3-step directed acyclic graph:
$$\text{extract\_metrics} \longrightarrow \text{compute\_statistics} \longrightarrow \text{format\_alert}$$

Run the script:
```bash
python examples/01_quickstart.py
```

**What it does:**
- Creates an event store at `telemetry_pipeline.db`.
- Passes upstream outputs to downstream tasks automatically by argument name.
- Appends all lifecycle events to the Write-Ahead Log.

---

### Example 2: Distributed Saga with Rollbacks (`examples/02_saga_rollback.py`)
Demonstrates distributed transaction safety with automatic compensation rollbacks:
$$\text{authorize\_payment} \longrightarrow \text{reserve\_inventory} \longrightarrow \text{dispatch\_courier (FAILS)}$$

Run the script:
```bash
python examples/02_saga_rollback.py
```

**What it does:**
- Authorizes a mock credit card transaction ($149.99).
- Places a warehouse hold on inventory SKU `ITEM-999`.
- Simulates a courier dispatch timeout failure.
- Automatically retries with exponential backoff and randomized jitter.
- Upon retry exhaustion, triggers reverse-topological saga compensation:
  1. `reserve_inventory` is compensated **FIRST** (inventory released).
  2. `authorize_payment` is compensated **SECOND** (payment voided).
- Records all compensations in the WAL with final status `COMPENSATED`.

---

### Example 3: Real-Time Web Dashboard (`examples/03_dashboard.py`)
Launches the embedded FastAPI + HTMX + Tailwind dark-theme dashboard.

Run against the telemetry pipeline database:
```bash
python examples/03_dashboard.py telemetry_pipeline.db
```

Or run against the e-commerce saga database:
```bash
python examples/03_dashboard.py ecommerce_saga.db
```

Then open your browser and navigate to:
👉 **[http://localhost:8000](http://localhost:8000)**

---

## 4. Using the AegisFlow Terminal CLI

AegisFlow provides a rich terminal CLI for live workflow inspection:

### Inspect Workflow Lifecycle Status & Event History
```bash
aegisflow status sensor_run_101 --db telemetry_pipeline.db
```
Renders a structured Rich table detailing:
- Run ID, Workflow Name, Status, Duration, and Output.
- Chronological Write-Ahead Log (WAL) event table with sequence IDs, timestamps, and parameters.

### Live-Monitor a Workflow Execution
```bash
aegisflow watch sensor_run_101 --db telemetry_pipeline.db
```
Polls the event store until the workflow enters a terminal state (`COMPLETED`, `FAILED`, `COMPENSATED`, or `DEAD_LETTER`).

### Inspect Dead-Letter Queue (DLQ)
```bash
aegisflow dlq list --db ecommerce_saga.db
```
Displays all workflows whose retries and saga compensations failed, requiring manual administrator triage.

---

## 5. Writing Your Own Custom Workflow

Create a new file `my_pipeline.py`:

```python
import asyncio
from aegisflow import SQLiteStorage, WorkflowOrchestrator, task, workflow


# 1. Define tasks
@task
def step_a() -> str:
    return "Hello"


@task
def step_b(step_a: str) -> str:
    return f"{step_a}, AegisFlow!"


# 2. Chain dependencies
step_b.after(step_a)

# 3. Assemble workflow
my_wf = workflow("GreetingPipeline", tasks=[step_a, step_b])


async def main():
    # 4. Initialize storage & orchestrator
    storage = SQLiteStorage("greetings.db")
    await storage.initialize()

    orchestrator = WorkflowOrchestrator(
        event_store=storage,
        lease_manager=storage,
    )

    # 5. Run workflow
    state = await orchestrator.run(my_wf.to_definition(), workflow_id="greet_001")
    print(f"Workflow Status: {state.status}")
    print(f"Output: {state.workflow_output}")

    await storage.close()


if __name__ == "__main__":
    asyncio.run(main())
```

Execute your custom script:
```bash
python my_pipeline.py
aegisflow status greet_001 --db greetings.db
```

---

## 6. Running Tests, Benchmarks & Linters

### Run Pytest Test Suite
```bash
python -m pytest tests/
```

### Run Benchmarks (with stdout output)
```bash
python -m pytest tests/benchmarks/ -s
```

### Run Code Linter & Formatter Check
```bash
python -m ruff check .
python -m ruff format --check .
```

### Run Strict Type Checker
```bash
python -m mypy --strict src
```
