"""Command-line interface (CLI) for AegisFlow workflow inspection and DLQ management."""

import asyncio
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from aegisflow._version import __version__
from aegisflow.core.states import WorkflowState
from aegisflow.storage.sqlite import SQLiteStorage

app = typer.Typer(
    name="aegisflow",
    help="AegisFlow: Lightweight, durable event-driven workflow engine CLI.",
    no_args_is_help=True,
)
dlq_app = typer.Typer(
    name="dlq",
    help="Inspect and manage Dead-Letter Queue workflows.",
    no_args_is_help=True,
)
app.add_typer(dlq_app, name="dlq")

console = Console()

STATUS_COLORS: dict[str, str] = {
    "COMPLETED": "[bold green]COMPLETED[/bold green]",
    "RUNNING": "[bold cyan]RUNNING[/bold cyan]",
    "FAILED": "[bold red]FAILED[/bold red]",
    "RETRYING": "[bold yellow]RETRYING[/bold yellow]",
    "PENDING": "[dim white]PENDING[/dim white]",
    "COMPENSATING": "[bold magenta]COMPENSATING[/bold magenta]",
    "COMPENSATED": "[bold purple]COMPENSATED[/bold purple]",
    "DEAD_LETTER": "[bold bright_red]DEAD_LETTER[/bold bright_red]",
}


def _render_status(status_str: str) -> str:
    return STATUS_COLORS.get(status_str, f"[white]{status_str}[/white]")


async def _get_workflow_data(
    db_path: str,
    workflow_id: str,
) -> tuple[dict[str, Any] | None, list[Any]]:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()
    record = await storage.get_workflow_record(workflow_id)
    events = await storage.get_events(workflow_id)
    await storage.close()
    return record, events


async def _get_dlq_list(db_path: str) -> list[dict[str, Any]]:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()
    conn = await storage._get_conn()
    async with conn.execute(
        """
        SELECT workflow_id, name, status, created_at, updated_at, input_data
        FROM workflows
        WHERE status = ?
        ORDER BY updated_at DESC;
        """,
        (WorkflowState.DEAD_LETTER.value,),
    ) as cursor:
        rows = await cursor.fetchall()
        result = [dict(row) for row in rows]
    await storage.close()
    return result


@app.command("version")
def version() -> None:
    """Display current AegisFlow CLI version."""
    console.print(
        f"[bold cyan]AegisFlow[/bold cyan] version [bold green]{__version__}[/bold green]"
    )


@app.command("status")
def status(
    workflow_id: Annotated[str, typer.Argument(help="Unique workflow run ID")],
    db: Annotated[str, typer.Option("--db", "-d", help="Path to SQLite database")] = "aegisflow.db",
) -> None:
    """Inspect current lifecycle status and task execution progress for a workflow."""
    record, events = asyncio.run(_get_workflow_data(db, workflow_id))

    if not record:
        console.print(f"[red]Error:[/red] Workflow [bold]{workflow_id}[/bold] not found in '{db}'")
        raise typer.Exit(code=1)

    status_formatted = _render_status(record["status"])

    meta_table = Table.grid(padding=(0, 2))
    meta_table.add_column("Key", style="bold white")
    meta_table.add_column("Value")
    meta_table.add_row("Workflow ID", record["workflow_id"])
    meta_table.add_row("Name", record["name"])
    meta_table.add_row("Status", status_formatted)
    meta_table.add_row("Created At", str(record["created_at"]))
    meta_table.add_row("Updated At", str(record["updated_at"]))
    if record["output_data"]:
        meta_table.add_row("Output", str(record["output_data"]))

    console.print(Panel(meta_table, title=f"Workflow Run: {record['name']}", border_style="cyan"))

    # Event history table
    event_table = Table(title="Write-Ahead Log Events", header_style="bold cyan")
    event_table.add_column("Seq", style="dim", justify="right")
    event_table.add_column("Timestamp", style="dim")
    event_table.add_column("Event Type", style="bold")
    event_table.add_column("Details")

    for i, event in enumerate(events, 1):
        details = ""
        if hasattr(event, "task_id"):
            details = f"task={event.task_id}"
            if hasattr(event, "result") and event.result is not None:
                details += f", result={event.result}"
            if hasattr(event, "error") and event.error:
                details += f", error={event.error}"
        elif hasattr(event, "error"):
            details = f"error={event.error}"

        event_table.add_row(
            str(i),
            event.timestamp.strftime("%H:%M:%S.%f")[:-3],
            event.event_type,
            details,
        )

    console.print(event_table)


@app.command("watch")
def watch(
    workflow_id: Annotated[str, typer.Argument(help="Unique workflow run ID to monitor")],
    db: Annotated[str, typer.Option("--db", "-d", help="Path to SQLite database")] = "aegisflow.db",
    poll_interval: Annotated[
        float, typer.Option("--interval", "-i", help="Polling interval in seconds")
    ] = 0.5,
) -> None:
    """Live monitor a workflow's execution in real-time until completion."""
    from rich.live import Live

    async def _watch_loop() -> None:
        with Live(console=console, refresh_per_second=4) as live:
            while True:
                record, events = await _get_workflow_data(db, workflow_id)
                if not record:
                    live.update(Panel(f"[red]Workflow '{workflow_id}' not found[/red]"))
                    await asyncio.sleep(poll_interval)
                    continue

                table = Table(
                    title=f"Live Watch: {record['name']} ({_render_status(record['status'])})"
                )
                table.add_column("Seq", justify="right")
                table.add_column("Event")
                table.add_column("Task ID")

                for i, ev in enumerate(events[-10:], max(1, len(events) - 9)):
                    t_id = getattr(ev, "task_id", "-")
                    table.add_row(str(i), ev.event_type, t_id)

                live.update(table)

                st = WorkflowState(record["status"])
                if st.is_terminal:
                    break
                await asyncio.sleep(poll_interval)

    asyncio.run(_watch_loop())


@dlq_app.command("list")
def dlq_list(
    db: Annotated[str, typer.Option("--db", "-d", help="Path to SQLite database")] = "aegisflow.db",
) -> None:
    """List all workflows currently in the Dead-Letter Queue."""
    workflows = asyncio.run(_get_dlq_list(db))

    if not workflows:
        console.print("[green]No dead-lettered workflows found.[/green]")
        return

    table = Table(title="Dead-Letter Queue (DLQ)", header_style="bold red")
    table.add_column("Workflow ID", style="bold")
    table.add_column("Workflow Name")
    table.add_column("Status", style="red")
    table.add_column("Updated At", style="dim")

    for wf in workflows:
        table.add_row(
            wf["workflow_id"],
            wf["name"],
            wf["status"],
            wf["updated_at"],
        )

    console.print(table)
