# Product Requirements Document (PRD)
## AegisFlow: Resilient Event-Driven DAG Task Engine

---

### 1. Product Overview
**AegisFlow** is a lightweight, durable, event-driven workflow orchestration engine for Python. It allows software engineers to define multi-step task workflows as Directed Acyclic Graphs (DAGs) using native Python code, guaranteeing **durable execution**: if a worker process crashes, power is lost, or network partitions occur, workflows resume exactly where they left off without duplicate side effects or state corruption.

AegisFlow bridges the gap between lightweight, fragile task queues (Celery, RQ) and complex, heavy distributed engines (Temporal, Apache Airflow) by providing enterprise-grade durability with an embedded-first developer experience (zero external infra required for local/small setups, scalable to Postgres/Redis for clusters).

---

### 2. Problem Statement
Modern Python applications frequently require multi-step background processing:
- Payment and subscription flows (charge -> reserve inventory -> issue invoice -> notify)
- AI & LLM agent pipelines (retrieve -> prompt -> tool call -> summarize -> persist)
- ETL and asynchronous data ingestion pipelines

**Current solutions suffer from major architectural flaws:**
1. **Fragility of Task Queues (Celery, RQ):**
   - Chains and chords in Celery are notoriously brittle. If a worker dies mid-chord, the state is lost or left hanging indefinitely.
   - Retries re-run tasks from scratch, risking duplicate non-idempotent operations (e.g., charging a credit card twice).
2. **Operational Overhead of Existing Orchestrators (Temporal, Airflow, Prefect):**
   - **Temporal** requires running Go/Java backend clusters, Cassandra/PostgreSQL, and external daemon services.
   - **Airflow** is batch/cron-oriented with heavy latency (seconds to minutes per task dispatch) and is not suited for real-time application workflows.
3. **Lack of Native Saga/Rollback Support:**
   - When Step 3 of 5 fails permanently, standard task queues fail to execute clean compensating transactions (e.g., refunding a payment or releasing reserved stock).

---

### 3. Goals & Non-Goals

#### Goals
- **Durable Execution by Default:** Event-sourced execution history. Every task state change is recorded in an append-only Write-Ahead Log (WAL).
- **Embedded & Distributed Modes:** Runs out-of-the-box with SQLite (WAL mode) with zero external dependencies; seamlessly scales to PostgreSQL + Redis for distributed clusters.
- **Pythonic & Native DX:** Define workflows using standard Python async/sync functions with intuitive decorators (`@workflow`, `@task`).
- **Low Latency & High Throughput:** Sub-10ms task scheduling overhead for event transitions.
- **Automatic Crash Recovery & Replay:** Re-hydration of in-flight workflows on worker reboot, skipping already completed idempotent tasks.
- **Saga Pattern Support:** First-class compensation actions when workflows fail unrecoverably.
- **Full Observability:** OpenTelemetry distributed tracing and metrics, plus a lightweight real-time dashboard and terminal CLI.

#### Non-Goals
- Not a replacement for distributed batch compute engines like Apache Spark or Ray.
- Not a calendar-based cron scheduling suite (e.g., Airflow's cron scheduling of daily batch jobs); AegisFlow focuses on event-triggered, discrete, durable execution graphs.

---

### 4. Target Users
1. **Senior Backend & Systems Engineers:** Building critical transactional workflows (payments, onboarding, order fulfillment) needing crash resilience.
2. **AI & Agent Infrastructure Engineers:** Running multi-agent tool-calling pipelines and long-running reasoning loops that must survive server restarts.
3. **Platform / DevOps Engineers:** Seeking a minimal-overhead, low-maintenance alternative to running heavy Temporal or Airflow infrastructure.

---

### 5. Core Features (MVP Scope)

| Feature | Description | Acceptance Criteria |
| :--- | :--- | :--- |
| **Workflow & Task DSL** | `@task` and `@workflow` decorators supporting sync and async callables with parameter passing and typing. | Works with native Python syntax; detects circular dependencies at graph build time. |
| **Event-Sourced Storage (WAL)** | SQLite-backed Write-Ahead Log recording every state transition (`WorkflowStarted`, `TaskScheduled`, `TaskCompleted`, `TaskFailed`). | State is recoverable even after sudden `kill -9` of worker processes. |
| **DAG Dependency Resolver** | Kahn's topological sort and dependency tracker to fire tasks as soon as upstream parents complete. | Supports serial chains, branch fan-outs, and parallel fan-ins. |
| **Worker Lease & Heartbeats** | Distributed lease mechanism with heartbeats and fencing tokens. | Detects hung/dead workers within lease timeout and safely reassigns tasks to healthy workers. |
| **Durable Replay Engine** | Deterministic re-execution engine that reads event logs and resumes execution from the exact point of interruption. | Already completed tasks are skipped with cached return values. |
| **Retry & Jitter Policy** | Configurable retries with exponential backoff and randomized full jitter. | Failed tasks retry according to policy before triggering failure events. |
| **Saga Compensation (Rollbacks)** | Ability to attach `@task(compensate=...)` to automatically rollback completed steps if a downstream step fails permanently. | Compensaing actions run in reverse order upon unrecoverable failure. |
| **CLI & Terminal UI** | Command-line tool to inspect workflows, trigger runs, inspect dead-letter queues, and monitor workers. | Rich-based terminal interface with real-time progress bars and status tables. |
