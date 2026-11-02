# AegisFlow ⚡🛡️

> **Resilient Event-Driven DAG Task Engine for Python**  
> *A lightweight, durable execution orchestrator — bridging the gap between fragile task queues (Celery) and heavy enterprise engines (Temporal/Airflow).*

---

## 📖 Project Documentation

All foundational engineering specifications and design documents are available in the [`docs/`](docs/) directory:

- 📋 **[PRD (Product Requirements Document)](docs/PRD.md):** Vision, problem statement, goals, target users, and MVP scope.
- 🏛️ **[ARCHITECTURE.md](docs/ARCHITECTURE.md):** High-level system architecture, core subsystems, technology stack, and directory layout.
- 📜 **[RULES.md](docs/RULES.md):** Inviolable engineering rules, strict type-safety standards, async safety principles, and coding guidelines.
- 🎨 **[DESIGN.md](docs/DESIGN.md):** UI/UX specifications, dark-slate color palettes, status tokens, typography, and Rich CLI layout.
- ✅ **[TASKS.md](docs/TASKS.md):** Comprehensive 7-phase implementation roadmap with acceptance criteria.
- 🧠 **[MEMORY.md](docs/MEMORY.md):** Live project status tracker and Architectural Decision Records (ADRs).

---

## 🎯 Key Capabilities
- **Durable Execution:** Event-sourced execution history. Interrupted or crashed workflows automatically resume from the exact failure point without duplicate executions.
- **Embedded & Distributed Modes:** Zero-dependency embedded SQLite WAL mode out-of-the-box, scaling to PostgreSQL for distributed clusters.
- **Pythonic DSL:** Intuitive `@workflow` and `@task` decorators for native Python sync and async functions.
- **Saga Pattern Support:** Automated compensating rollbacks for multi-step transactional pipelines.
- **Zero-Friction Observability:** OpenTelemetry distributed tracing and a real-time web & terminal dashboard.

---

## 👤 Author
**Anmol Vishvas** ([anmol23vishvas@gmail.com](mailto:anmol23vishvas@gmail.com))
