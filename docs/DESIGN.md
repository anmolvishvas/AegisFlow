# UI/UX & Design Specification (DESIGN.md)
## AegisFlow: Resilient Event-Driven DAG Task Engine

This document defines the design principles, visual hierarchy, color palettes, typography, and component patterns for both the **AegisFlow Web Dashboard** and the **Terminal CLI Interface**.

---

### 1. Design Principles

1. **Information Density with Absolute Legibility:**
   - Operators inspecting distributed workflows need quick access to execution metrics, node states, and error stack traces without excessive scrolling or cognitive overload.
2. **Instant Status Recognition:**
   - State indicators (Success, Running, Failed, Retrying, Suspended) must be distinguishable by both distinctive color and clear iconography/shapes (accessible for color-blind users).
3. **Real-Time Responsiveness:**
   - Zero-refresh interface. Node states in the DAG graph and the event ticker update immediately via Server-Sent Events (SSE) or WebSockets.
4. **Developer-Native Aesthetic:**
   - Dark-mode first design inspired by modern developer infrastructure tools (Linear, Vercel, Supabase, Grafana).

---

### 2. Color System & Semantic Palette

AegisFlow uses a dark-slate surface palette paired with vibrant semantic status accents.

#### 2.1 Surface & Neutral Tokens
| Token | Hex Code | Usage |
| :--- | :--- | :--- |
| `surface-950` | `#090D16` | Main window background |
| `surface-900` | `#0F172A` | Card & sidebar containers |
| `surface-800` | `#1E293B` | Borders, table headers, separators |
| `surface-700` | `#334155` | Hover states, active tabs |
| `text-primary` | `#F8FAFC` | Primary headings and values |
| `text-secondary` | `#94A3B8` | Labels, descriptions, timestamps |
| `text-muted` | `#64748B` | Subtle hints, inactive icons |

#### 2.2 Semantic State Tokens
| State | Accent Hex | Badge Background | Symbol / Icon | Terminal (Rich) |
| :--- | :--- | :--- | :--- | :--- |
| **COMPLETED** | `#10B981` (Emerald) | `rgba(16, 185, 129, 0.12)` | `✓` Check | `[bold green]` |
| **RUNNING** | `#0EA5E9` (Sky) | `rgba(14, 165, 233, 0.12)` | `●` Pulsing Dot | `[bold cyan]` |
| **FAILED** | `#F43F5E` (Rose) | `rgba(244, 63, 94, 0.12)` | `✕` Cross | `[bold red]` |
| **RETRYING** | `#F59E0B` (Amber) | `rgba(245, 158, 11, 0.12)` | `↻` Loop | `[bold yellow]` |
| **PENDING** | `#64748B` (Slate) | `rgba(100, 116, 139, 0.12)` | `⋯` Clock/Ellipsis | `[dim white]` |
| **COMPENSATED** | `#A855F7` (Purple) | `rgba(168, 85, 247, 0.12)` | `↩` Undo Arrow | `[bold magenta]` |

---

### 3. Typography

- **Primary UI Font:** `Inter`, `Geist Sans`, or system sans-serif (`-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto`).
  - *Weights:* 400 (Body), 500 (Labels/Controls), 600 (Card titles, Section headers), 700 (Metrics).
- **Monospace Font:** `JetBrains Mono`, `Fira Code`, or `ui-monospace`.
  - *Usage:* Task IDs, Workflow Run IDs, Fencing Tokens, JSON payloads, and Error Stack Traces.
- **Hierarchy:**
  - `Title (H1):` 24px / 32px line-height, bold (`text-primary`)
  - `Section Header (H2):` 18px / 26px line-height, semi-bold (`text-primary`)
  - `Body / Values:` 14px / 20px line-height, regular (`text-primary`)
  - `Captions & Labels:` 12px / 16px line-height, medium (`text-secondary`)
  - `Code / IDs:` 13px / 18px line-height, monospace

---

### 4. Core UI Components (Web Dashboard)

#### 4.1 Interactive Live DAG Graph
- **Visualization:** Directed node-link diagram rendering tasks as rounded rectangular cards connected by directional Bézier curves.
- **Node Anatomy:**
  - Status pill (Icon + Status text) on the left.
  - Task function name in bold.
  - Execution duration timer (e.g. `245ms`, `1.4s`) or retry count badge (`Retry 2/3`).
  - Pulsing outer glow for currently active `RUNNING` nodes.
- **Interactions:**
  - Clicking any node opens the **Task Detail Drawer** showing input arguments, return values, and full execution events.

#### 4.2 Real-Time Event Stream Ticker
- A continuous, auto-scrolling event log at the bottom or right sidebar displaying every WAL event as it happens:
  `[11:24:02.108] TaskScheduled -> 'reserve_inventory' (Workflow: wf_01hx...)`
- Includes quick-filters by event type: `All`, `Errors Only`, `Retries Only`.

#### 4.3 Task Detail & Failure Drawer
- Slides in from the right edge when a node is selected.
- Displays:
  - Task ID and Fencing Token
  - Worker ID that leased the task
  - Exact input arguments (collapsible JSON tree)
  - Result payload or serialized exception traceback with syntax highlighting
  - Action buttons: `Force Retry`, `Trigger Compensation`, `Inspect Logs`

#### 4.4 Worker Fleet Monitor
- Compact status grid showing all active worker nodes:
  - Worker ID, Hostname, and PID
  - Heartbeat status (Green indicator if updated within <5s; Amber warning if >10s)
  - Active concurrency slots (e.g., `3/8 tasks running`)
  - CPU & RSS Memory utilization meter

---

### 5. Terminal CLI Design (Rich / Typer)

The CLI tool (`aegisflow status`, `aegisflow watch <workflow-id>`) mirrors the dashboard's clarity using the `rich` Python library.

#### 5.1 CLI Layout Example (`aegisflow watch`)
```text
╭────────────────────────────── AegisFlow Workflow Monitor ──────────────────────────────╮
│ Workflow ID:  wf_98df8b1a-3a1b-419f-b989-123456789abc                                  │
│ Workflow:     OrderFulfillmentWorkflow                                                 │
│ Status:       [bold cyan]● RUNNING[/bold cyan]   Duration: 1.42s   Events: 9           │
╰────────────────────────────────────────────────────────────────────────────────────────╯

  Task DAG Execution:
  ├── [bold green]✓ charge_credit_card[/bold green]       (id: tsk_01, 142ms, worker_a)
  ├── [bold green]✓ reserve_inventory[/bold green]        (id: tsk_02,  89ms, worker_b)
  ├── [bold cyan]● generate_shipping_label[/bold cyan]  (id: tsk_03, running [0.8s...], worker_a)
  ├── [dim white]⋯ send_invoice_pdf[/dim white]         (id: tsk_04, pending)
  └── [dim white]⋯ notify_customer[/dim white]          (id: tsk_05, pending)

  Live Event Stream:
  11:24:01.012  [bold]WorkflowInitiated[/bold]    (Workflow: OrderFulfillmentWorkflow)
  11:24:01.015  [green]TaskCompleted[/green]        (Task: charge_credit_card -> txn_99182)
  11:24:01.104  [green]TaskCompleted[/green]        (Task: reserve_inventory -> stock_reserved)
  11:24:01.108  [cyan]TaskStarted[/cyan]          (Task: generate_shipping_label)
```
