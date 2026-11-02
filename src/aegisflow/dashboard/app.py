"""Lightweight, real-time web dashboard using FastAPI, HTMX, and Tailwind CSS."""

import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from aegisflow.core.states import WorkflowState
from aegisflow.storage.sqlite import SQLiteStorage


class WorkflowSummary(BaseModel):
    workflow_id: str
    name: str
    status: str
    created_at: str
    updated_at: str
    input_data: dict[str, Any] | None = None
    output_data: Any = None


def create_dashboard_app(database_path: str | Path = "aegisflow.db") -> FastAPI:
    """Create and configure the AegisFlow FastAPI web dashboard application."""
    app = FastAPI(
        title="AegisFlow Dashboard",
        description="Real-time DAG task orchestration monitoring dashboard.",
        version="0.1.0",
    )
    db_path = str(database_path)

    async def _get_storage() -> SQLiteStorage:
        storage = SQLiteStorage(database_path=db_path)
        await storage.initialize()
        return storage

    @app.get("/api/workflows", response_model=list[WorkflowSummary])
    async def list_workflows() -> list[dict[str, Any]]:
        storage = await _get_storage()
        conn = await storage._get_conn()
        async with conn.execute(
            """
            SELECT workflow_id, name, status, created_at, updated_at, input_data, output_data
            FROM workflows
            ORDER BY updated_at DESC
            LIMIT 100;
            """
        ) as cursor:
            rows = await cursor.fetchall()
            result = []
            for r in rows:
                row_dict = dict(r)
                row_dict["input_data"] = (
                    json.loads(row_dict["input_data"]) if row_dict.get("input_data") else {}
                )
                row_dict["output_data"] = (
                    json.loads(row_dict["output_data"]) if row_dict.get("output_data") else None
                )
                result.append(row_dict)
        await storage.close()
        return result

    @app.get("/api/workflows/{workflow_id}")
    async def get_workflow(workflow_id: str) -> dict[str, Any]:
        storage = await _get_storage()
        record = await storage.get_workflow_record(workflow_id)
        if not record:
            await storage.close()
            raise HTTPException(status_code=404, detail="Workflow not found")
        events = await storage.get_events(workflow_id)
        await storage.close()
        return {
            "workflow": record,
            "events": [e.model_dump(mode="json") for e in events],
        }

    @app.get("/api/dlq")
    async def list_dlq() -> list[dict[str, Any]]:
        storage = await _get_storage()
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
            result = [dict(r) for r in rows]
        await storage.close()
        return result

    @app.get("/", response_class=HTMLResponse)
    async def index_html() -> str:
        workflows = await list_workflows()
        rows_html = ""
        for wf in workflows:
            status_color = "text-emerald-400 bg-emerald-950/40 border-emerald-800"
            if wf["status"] == "RUNNING":
                status_color = "text-sky-400 bg-sky-950/40 border-sky-800"
            elif wf["status"] in ("FAILED", "DEAD_LETTER"):
                status_color = "text-rose-400 bg-rose-950/40 border-rose-800"
            elif wf["status"] == "COMPENSATED":
                status_color = "text-purple-400 bg-purple-950/40 border-purple-800"

            rows_html += f"""
            <tr class="border-b border-slate-800 hover:bg-slate-800/40 transition">
                <td class="py-3 px-4 font-mono text-sm text-sky-400">
                    <a href="/workflows/{wf["workflow_id"]}" class="hover:underline">{wf["workflow_id"][:12]}...</a>
                </td>
                <td class="py-3 px-4 font-medium text-slate-200">{wf["name"]}</td>
                <td class="py-3 px-4">
                    <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-semibold border {status_color}">
                        {wf["status"]}
                    </span>
                </td>
                <td class="py-3 px-4 text-xs text-slate-400 font-mono">{wf["created_at"][:19]}</td>
                <td class="py-3 px-4 text-xs text-slate-400 font-mono">{wf["updated_at"][:19]}</td>
            </tr>
            """

        return f"""<!DOCTYPE html>
<html lang="en" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AegisFlow Orchestration Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://unpkg.com/htmx.org@1.9.10"></script>
</head>
<body class="bg-[#090D16] text-slate-100 min-h-screen font-sans">
    <div class="max-w-7xl mx-auto px-4 py-8">
        <header class="flex justify-between items-center mb-8 border-b border-slate-800 pb-4">
            <div>
                <h1 class="text-2xl font-bold tracking-tight text-white flex items-center gap-2">
                    <span class="text-sky-400">⚡</span> AegisFlow Dashboard
                </h1>
                <p class="text-xs text-slate-400 mt-1">Lightweight, Event-Sourced DAG Workflow Engine</p>
            </div>
            <div class="flex gap-4">
                <a href="/" class="px-3 py-1.5 rounded-md text-xs font-medium bg-slate-800 text-sky-400 border border-slate-700">Workflows</a>
                <a href="/api/workflows" target="_blank" class="px-3 py-1.5 rounded-md text-xs font-medium text-slate-400 hover:bg-slate-800 transition">JSON API</a>
            </div>
        </header>

        <section class="bg-[#0F172A] border border-slate-800 rounded-lg p-6 shadow-xl">
            <h2 class="text-lg font-semibold text-white mb-4">Workflow Executions</h2>
            <div class="overflow-x-auto">
                <table class="w-full text-left border-collapse">
                    <thead>
                        <tr class="border-b border-slate-800 text-xs font-semibold uppercase tracking-wider text-slate-400 bg-slate-900/60">
                            <th class="py-3 px-4">Run ID</th>
                            <th class="py-3 px-4">Workflow Name</th>
                            <th class="py-3 px-4">Status</th>
                            <th class="py-3 px-4">Created</th>
                            <th class="py-3 px-4">Last Updated</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-800/60">
                        {rows_html if rows_html else '<tr><td colspan="5" class="py-8 text-center text-slate-500">No workflows recorded yet</td></tr>'}
                    </tbody>
                </table>
            </div>
        </section>
    </div>
</body>
</html>"""

    @app.get("/workflows/{workflow_id}", response_class=HTMLResponse)
    async def workflow_detail_html(workflow_id: str) -> str:
        data = await get_workflow(workflow_id)
        wf = data["workflow"]
        events = data["events"]

        events_rows = ""
        for i, ev in enumerate(events, 1):
            ts = ev.get("timestamp", "")
            ts_str = str(ts)[11:23] if ts else ""
            events_rows += f"""
            <tr class="border-b border-slate-800/40 text-xs">
                <td class="py-2 px-3 font-mono text-slate-400">{i}</td>
                <td class="py-2 px-3 font-mono text-sky-400">{ts_str}</td>
                <td class="py-2 px-3 font-semibold text-slate-200">{ev.get("event_type")}</td>
                <td class="py-2 px-3 font-mono text-slate-300">{ev.get("task_id", "-")}</td>
                <td class="py-2 px-3 font-mono text-slate-400 truncate max-w-xs">{str(ev.get("result") or ev.get("error") or "")}</td>
            </tr>
            """

        return f"""<!DOCTYPE html>
<html lang="en" class="dark">
<head>
    <meta charset="UTF-8">
    <title>AegisFlow - {wf["name"]} ({workflow_id[:8]})</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-[#090D16] text-slate-100 min-h-screen font-sans">
    <div class="max-w-7xl mx-auto px-4 py-8">
        <a href="/" class="text-xs text-sky-400 hover:underline mb-4 inline-block">&larr; Back to all workflows</a>
        <header class="bg-[#0F172A] border border-slate-800 rounded-lg p-6 mb-6">
            <div class="flex justify-between items-start">
                <div>
                    <h1 class="text-xl font-bold text-white">{wf["name"]}</h1>
                    <p class="font-mono text-xs text-slate-400 mt-1">{wf["workflow_id"]}</p>
                </div>
                <span class="px-3 py-1 rounded-full text-xs font-bold bg-slate-800 border border-slate-700 text-sky-400">
                    {wf["status"]}
                </span>
            </div>
        </header>

        <section class="bg-[#0F172A] border border-slate-800 rounded-lg p-6 shadow-xl">
            <h2 class="text-md font-semibold text-white mb-4">Write-Ahead Log Events ({len(events)})</h2>
            <div class="overflow-x-auto">
                <table class="w-full text-left">
                    <thead>
                        <tr class="border-b border-slate-800 text-xs font-semibold uppercase text-slate-400 bg-slate-900/60">
                            <th class="py-2 px-3">Seq</th>
                            <th class="py-2 px-3">Time</th>
                            <th class="py-2 px-3">Event Type</th>
                            <th class="py-2 px-3">Task ID</th>
                            <th class="py-2 px-3">Payload Details</th>
                        </tr>
                    </thead>
                    <tbody>
                        {events_rows}
                    </tbody>
                </table>
            </div>
        </section>
    </div>
</body>
</html>"""

    return app
