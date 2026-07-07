from __future__ import annotations

import html
import json
from typing import Any
from urllib.parse import quote

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from . import core
from .db import db_path

router = APIRouter()


def h(value: Any) -> str:
    return html.escape(str(value if value is not None else ""), quote=True)


def path_id(value: Any) -> str:
    return quote(str(value), safe="")


def pretty_json(value: Any) -> str:
    return h(json.dumps(value, indent=2, ensure_ascii=False, default=str))


def short_id(value: Any, length: int = 14) -> str:
    text = str(value if value is not None else "")
    if len(text) <= length:
        return h(text)
    return h(text[:length] + "…")


def status_pill(status: Any) -> str:
    status_text = str(status if status is not None else "unknown")
    safe_status = "".join(ch if ch.isalnum() or ch in "_-" else "-" for ch in status_text.lower())
    return f"<span class='pill status-{h(safe_status)}'>{h(status_text)}</span>"


def progress_bar(done: int, total: int) -> str:
    pct = 0 if total <= 0 else int(round((done / total) * 100))
    pct = max(0, min(100, pct))
    return f"""
    <div class='progress-wrap' title='{done}/{total} tasks'>
      <div class='progress-fill' style='width:{pct}%'></div>
    </div>
    <div class='tiny'>{done}/{total} · {pct}%</div>
    """


def empty_row(cols: int, message: str) -> str:
    return f"<tr><td colspan='{cols}' class='empty'>{h(message)}</td></tr>"


def layout(title: str, body: str, *, refresh: bool = False) -> str:
    refresh_tag = "<meta http-equiv='refresh' content='5'>" if refresh else ""
    return f"""
    <!doctype html>
    <html lang='en'>
    <head>
      <meta charset='utf-8'>
      <meta name='viewport' content='width=device-width, initial-scale=1'>
      {refresh_tag}
      <title>{h(title)} · TaskGrid</title>
      <style>
        :root {{
          color-scheme: light;
          --bg:#f5f7fb;
          --card:#ffffff;
          --text:#101828;
          --muted:#667085;
          --line:#e4e7ec;
          --soft:#f2f4f7;
          --accent:#344054;
          --good:#dcfae6;
          --good-text:#067647;
          --bad:#fee4e2;
          --bad-text:#b42318;
          --warn:#fef0c7;
          --warn-text:#b54708;
          --info:#e0eaff;
          --info-text:#3538cd;
        }}
        * {{ box-sizing: border-box; }}
        body {{ margin:0; background:var(--bg); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; }}
        a {{ color:#175cd3; text-decoration:none; }}
        a:hover {{ text-decoration:underline; }}
        .shell {{ display:grid; grid-template-columns:220px minmax(0,1fr); min-height:100vh; }}
        .sidebar {{ background:#111827; color:#f9fafb; padding:22px 16px; position:sticky; top:0; height:100vh; }}
        .brand {{ font-size:22px; font-weight:800; margin-bottom:4px; }}
        .tagline {{ color:#d0d5dd; font-size:13px; margin-bottom:24px; line-height:1.35; }}
        .nav a {{ display:block; color:#f9fafb; padding:10px 12px; border-radius:10px; margin:4px 0; }}
        .nav a:hover {{ background:#1f2937; text-decoration:none; }}
        .content {{ padding:26px; max-width:1500px; width:100%; }}
        .topline {{ display:flex; justify-content:space-between; gap:16px; align-items:flex-start; margin-bottom:18px; }}
        h1 {{ margin:0 0 5px; font-size:28px; letter-spacing:-.02em; }}
        h2 {{ margin:0 0 14px; font-size:18px; }}
        .muted {{ color:var(--muted); }}
        .tiny {{ color:var(--muted); font-size:12px; }}
        .grid {{ display:grid; gap:16px; }}
        .stats {{ grid-template-columns:repeat(4,minmax(0,1fr)); }}
        .two {{ grid-template-columns:1.2fr .8fr; align-items:start; }}
        .card {{ background:var(--card); border:1px solid var(--line); border-radius:16px; padding:18px; box-shadow:0 1px 4px rgba(16,24,40,.04); }}
        .stat .label {{ color:var(--muted); font-size:13px; }}
        .stat .value {{ font-size:30px; font-weight:800; margin-top:8px; }}
        table {{ width:100%; border-collapse:collapse; font-size:14px; }}
        th, td {{ text-align:left; border-bottom:1px solid var(--line); padding:10px 8px; vertical-align:top; }}
        th {{ color:#475467; font-size:12px; text-transform:uppercase; letter-spacing:.04em; }}
        tr:last-child td {{ border-bottom:0; }}
        .pill {{ display:inline-block; padding:3px 9px; border-radius:999px; background:var(--soft); color:#344054; font-size:12px; font-weight:700; white-space:nowrap; }}
        .status-succeeded {{ background:var(--good); color:var(--good-text); }}
        .status-failed {{ background:var(--bad); color:var(--bad-text); }}
        .status-running, .status-leasing, .status-cancelling {{ background:var(--warn); color:var(--warn-text); }}
        .status-queued {{ background:var(--info); color:var(--info-text); }}
        .status-paused {{ background:#fef0c7; color:#b54708; }}
        .status-cancelled, .status-stopped {{ background:#eaecf0; color:#344054; }}
        .status-stale {{ background:#fee4e2; color:#b42318; }}
        .status-drained, .status-draining {{ background:#fef0c7; color:#b54708; }}
        .status-disabled {{ background:#eaecf0; color:#344054; }}
        .status-idle {{ background:#ecfdf3; color:#067647; }}
        .progress-wrap {{ height:8px; background:#eaecf0; border-radius:999px; overflow:hidden; min-width:120px; }}
        .progress-fill {{ height:100%; background:#475467; border-radius:999px; }}
        .actions {{ display:flex; flex-wrap:wrap; gap:8px; align-items:center; }}
        .button, button {{ border:0; background:#111827; color:white; border-radius:10px; padding:9px 12px; font-weight:700; cursor:pointer; display:inline-block; }}
        .button.secondary, button.secondary {{ background:#eaecf0; color:#344054; }}
        .button.danger, button.danger {{ background:#b42318; }}
        .button:hover {{ text-decoration:none; filter:brightness(.96); }}
        code, pre {{ font-family:'SFMono-Regular',Consolas,monospace; }}
        code {{ background:#f2f4f7; padding:2px 5px; border-radius:6px; }}
        pre {{ background:#0b1020; color:#f8fafc; padding:14px; border-radius:12px; overflow:auto; font-size:13px; line-height:1.45; }}
        form.stack {{ display:grid; gap:12px; }}
        label {{ display:grid; gap:6px; color:#344054; font-weight:700; font-size:13px; }}
        input, textarea {{ width:100%; border:1px solid #d0d5dd; border-radius:10px; padding:10px 11px; font:inherit; background:white; }}
        textarea {{ min-height:220px; resize:vertical; font-family:'SFMono-Regular',Consolas,monospace; font-size:13px; }}
        .form-row {{ display:grid; grid-template-columns:1fr 140px 160px; gap:12px; }}
        .alert {{ padding:12px 14px; border-radius:12px; margin-bottom:14px; border:1px solid var(--line); }}
        .alert.error {{ background:#fff1f3; color:#b42318; border-color:#fecdd6; }}
        .alert.success {{ background:#ecfdf3; color:#067647; border-color:#abefc6; }}
        .empty {{ color:var(--muted); text-align:center; padding:22px; }}
        .nowrap {{ white-space:nowrap; }}
        .mobile-only {{ display:none; }}
        @media (max-width: 920px) {{
          .shell {{ display:block; }}
          .sidebar {{ position:relative; height:auto; }}
          .nav {{ display:flex; gap:6px; overflow:auto; }}
          .nav a {{ white-space:nowrap; }}
          .content {{ padding:16px; }}
          .stats, .two, .form-row {{ grid-template-columns:1fr; }}
          .topline {{ display:block; }}
          table {{ display:block; overflow:auto; }}
        }}
      </style>
    </head>
    <body>
      <div class='shell'>
        <aside class='sidebar'>
          <div class='brand'>TaskGrid</div>
          <div class='tagline'>Minimal distributed task runner</div>
          <nav class='nav'>
            <a href='/ui'>Dashboard</a>
            <a href='/ui/sessions'>Service Sessions</a>
            <a href='/ui/client-sessions'>Client Resume</a>
            <a href='/ui/jobs'>Jobs</a>
            <a href='/ui/submit'>Submit Job</a>
            <a href='/ui/workers'>Workers</a>
            <a href='/ui/executors'>Executors</a>
            <a href='/ui/queue'>Queue</a>
            <a href='/ui/services'>Services</a>
            <a href='/ui/task-catalog'>Task Catalog</a>
            <a href='/ui/manager/data-flow'>Data Flow</a>
            <a href='/ui/manager/recovery'>Recovery</a>
            <a href='/ui/manager/retention'>Retention</a>
            <a href='/ui/manager/log'>Manager Log</a>
            <a href='/docs'>API Docs</a>
          </nav>
        </aside>
        <main class='content'>{body}</main>
      </div>
    </body>
    </html>
    """


def job_table(jobs: list[dict[str, Any]]) -> str:
    if not jobs:
        rows = empty_row(8, "No jobs yet. Submit one from the web UI or SDK.")
    else:
        rows = "".join(
            f"""
            <tr>
              <td><a href='/ui/jobs/{path_id(j['id'])}'><code>{short_id(j['id'])}</code></a></td>
              <td>{h(j['name'])}<div class='tiny'>{h(j['task_type'])}</div></td>
              <td>{status_pill(j['status'])}{' ' + status_pill('paused') if int(j.get('paused') or 0) else ''}</td>
              <td>{progress_bar(int(j['completed_tasks']), int(j['total_tasks']))}</td>
              <td>{h(j['failed_tasks'])}</td>
              <td>{h(j['priority'])}</td>
              <td class='nowrap'>{h(j['created_at'])}</td>
              <td><a class='button secondary' href='/ui/jobs/{path_id(j['id'])}'>Open</a></td>
            </tr>
            """
            for j in jobs
        )
    return f"""
    <table>
      <thead><tr><th>ID</th><th>Name</th><th>Status</th><th>Progress</th><th>Failed</th><th>Priority</th><th>Created</th><th></th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    """



def session_table(sessions: list[dict[str, Any]]) -> str:
    if not sessions:
        rows = empty_row(12, "No service sessions yet. Submit a job to create one.")
    else:
        rows = "".join(
            f"""
            <tr>
              <td><a href='/ui/sessions/{path_id(sess['id'])}'><code>{short_id(sess['id'])}</code></a></td>
              <td>{h(sess['name'])}<div class='tiny'>{h(sess.get('total_jobs', 0))} job(s)</div></td>
              <td>{status_pill(sess['status'])}{' ' + status_pill('paused') if int(sess.get('paused') or 0) else ''}</td>
              <td>{h(sess.get('priority', 0))}</td>
              <td>{progress_bar(int(sess.get('completed_tasks') or 0), int(sess.get('total_tasks') or 0))}</td>
              <td>{h(sess.get('queued_tasks', 0))}</td>
              <td>{h(sess.get('running_tasks', 0))}</td>
              <td>{h(sess.get('failed_tasks', 0))}</td>
              <td>{h(sess.get('total_tasks', 0))}</td>
              <td class='nowrap'>{h(sess.get('created_at'))}</td>
              <td class='nowrap'>{h(sess.get('started_at') or '—')}<div class='tiny'>end {h(sess.get('finished_at') or '—')}</div></td>
              <td><a class='button secondary' href='/ui/sessions/{path_id(sess['id'])}'>Open</a> <a class='button secondary' href='/ui/sessions/{path_id(sess['id'])}/history'>History</a></td>
            </tr>
            """
            for sess in sessions
        )
    return f"""
    <table>
      <thead><tr><th>ID</th><th>Name</th><th>Status</th><th>Priority</th><th>Progress</th><th>Pending</th><th>Running</th><th>Failed</th><th>Total</th><th>Created</th><th>Start / End</th><th></th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    """


def task_table(tasks: list[dict[str, Any]]) -> str:
    if not tasks:
        rows = empty_row(10, "No tasks found.")
    else:
        rows = "".join(
            f"""
            <tr>
              <td>{h(t.get('input_index') if t.get('input_index') is not None else '—')}</td>
              <td><code>{h(t.get('input_key') or '—')}</code></td>
              <td><a href='/ui/tasks/{path_id(t['id'])}'><code>{short_id(t['id'])}</code></a></td>
              <td>{status_pill(t['status'])}</td>
              <td>{h(t['task_type'])}</td>
              <td>{h(t['attempts'])}/{1 + int(t['max_retries'])}</td>
              <td>{h(t['assigned_worker_id'])}</td>
              <td class='nowrap'>{h(t['updated_at'])}</td>
              <td>{h((t.get('error') or '')[:120])}</td>
              <td><a class='button secondary' href='/ui/tasks/{path_id(t['id'])}'>Open</a></td>
            </tr>
            """
            for t in tasks
        )
    return f"""
    <table>
      <thead><tr><th>Index</th><th>Key</th><th>ID</th><th>Status</th><th>Type</th><th>Attempts</th><th>Worker</th><th>Updated</th><th>Error</th><th></th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    """


def event_table(events: list[dict[str, Any]]) -> str:
    if not events:
        rows = empty_row(6, "No events yet.")
    else:
        rows = "".join(
            f"""
            <tr>
              <td class='nowrap'>{h(e['at'])}</td>
              <td>{h(e['level'])}</td>
              <td><code>{h(e.get('code') or '')}</code></td>
              <td>{h(e['entity_type'])}</td>
              <td><code>{short_id(e['entity_id'])}</code></td>
              <td>{h(e['message'])}</td>
            </tr>
            """
            for e in events
        )
    return f"""
    <table>
      <thead><tr><th>At</th><th>Level</th><th>Code</th><th>Entity</th><th>ID</th><th>Message</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    """




def result_table(items: list[dict[str, Any]], *, include_job: bool = False) -> str:
    if not items:
        cols = 11 if include_job else 10
        rows = empty_row(cols, "No task results yet.")
    else:
        rows = "".join(
            f"""
            <tr>
              {f"<td><a href='/ui/jobs/{path_id(item.get('job_id'))}'><code>{short_id(item.get('job_id'))}</code></a></td>" if include_job else ""}
              <td>{h(item.get('input_index') if item.get('input_index') is not None else '—')}</td>
              <td><code>{h(item.get('input_key') or '—')}</code></td>
              <td><a href='/ui/tasks/{path_id(item.get('task_id'))}'><code>{short_id(item.get('task_id'))}</code></a></td>
              <td>{status_pill(item.get('status'))}</td>
              <td><code>{h(item.get('assigned_worker_id') or '—')}</code></td>
              <td>{h(item.get('attempts'))}/{1 + int(item.get('max_retries') or 0)}</td>
              <td class='nowrap'>{h(item.get('started_at') or '—')}<div class='tiny'>end {h(item.get('finished_at') or '—')}</div></td>
              <td>{h(item.get('payload_bytes') or 0)} B</td>
              <td>{h(item.get('result_bytes') or 0)} B</td>
              <td><pre>{pretty_json(item.get('result') if item.get('error') is None else {'error': item.get('error')})}</pre></td>
            </tr>
            """
            for item in items
        )
    job_head = "<th>Job</th>" if include_job else ""
    return f"""
    <table>
      <thead><tr>{job_head}<th>Index</th><th>Key</th><th>Task</th><th>Status</th><th>Worker</th><th>Attempts</th><th>Start / End</th><th>Payload</th><th>Result</th><th>Value / Error</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    """



def session_task_history_table(history: dict[str, Any]) -> str:
    tasks = history.get("tasks") or []
    if not tasks:
        rows = empty_row(13, "No tasks match this session history filter.")
    else:
        row_parts = []
        for item in tasks:
            worker_id = item.get("worker_id")
            instance_id = item.get("instance_id")
            slot = "—"
            if worker_id and instance_id:
                slot = f"<a href='/ui/executors/{path_id(worker_id)}/instances/{path_id(instance_id)}'><code>{h(worker_id)} / {h(instance_id)}</code></a>"
            elif item.get("assigned_worker_id"):
                slot = f"<code>{h(item.get('assigned_worker_id'))}</code>"
            value = ""
            if item.get("error"):
                value = h(str(item.get("error") or "")[:500])
            elif item.get("result") is not None:
                value = pretty_json(item.get("result"))
            row_parts.append(
                f"""
                <tr>
                  <td><a href='/ui/jobs/{path_id(item.get('job_id'))}'><code>{short_id(item.get('job_id'))}</code></a><div class='tiny'>{h(item.get('job_name') or '')}</div></td>
                  <td>{h(item.get('input_index') if item.get('input_index') is not None else '—')}</td>
                  <td><code>{h(item.get('input_key') or '—')}</code></td>
                  <td><a href='/ui/tasks/{path_id(item.get('task_id'))}'><code>{short_id(item.get('task_id'))}</code></a></td>
                  <td>{status_pill(item.get('status'))}</td>
                  <td>{h(item.get('task_type') or '')}</td>
                  <td>{slot}</td>
                  <td>{h(item.get('attempts'))}/{1 + int(item.get('max_retries') or 0)}</td>
                  <td>{seconds_label(item.get('runtime_seconds'))}</td>
                  <td class='nowrap'>{h(item.get('started_at') or '—')}<div class='tiny'>end {h(item.get('finished_at') or '—')}</div></td>
                  <td>{h(item.get('payload_bytes') or 0)} B</td>
                  <td>{h(item.get('result_bytes') or 0)} B</td>
                  <td><pre>{value}</pre></td>
                </tr>
                """
            )
        rows = "".join(row_parts)
    return f"""
    <table>
      <thead><tr><th>Job</th><th>Index</th><th>Key</th><th>Task</th><th>Status</th><th>Type</th><th>Final Slot</th><th>Attempts</th><th>Runtime</th><th>Start / End</th><th>Payload</th><th>Result</th><th>Value / Error</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    """



def seconds_label(value: Any) -> str:
    if value is None:
        return "—"
    try:
        seconds = int(float(value))
    except (TypeError, ValueError):
        return "—"
    if seconds < 60:
        return f"{seconds}s"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {sec}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m"


def session_current_assignment_table(assignments: dict[str, Any]) -> str:
    running = assignments.get("current_assignments") or []
    if not running:
        rows = empty_row(9, "No running tasks currently assigned for this session.")
    else:
        rows = "".join(
            f"""
            <tr>
              <td><a href='/ui/workers/{path_id(item.get('worker_id'))}'><code>{h(item.get('worker_id') or '—')}</code></a><div class='tiny'><a href='/ui/executors/{path_id(item.get('worker_id'))}/instances/{path_id(item.get('instance_id') or 'node')}'>{h(item.get('instance_id') or 'node')}</a></div></td>
              <td><a href='/ui/tasks/{path_id(item.get('task_id'))}'><code>{short_id(item.get('task_id'))}</code></a></td>
              <td><a href='/ui/jobs/{path_id(item.get('job_id'))}'><code>{short_id(item.get('job_id'))}</code></a><div class='tiny'>{h(item.get('job_name') or '')}</div></td>
              <td>{h(item.get('input_index') if item.get('input_index') is not None else '—')}</td>
              <td><code>{h(item.get('input_key') or '—')}</code></td>
              <td>{h(item.get('task_type') or '')}</td>
              <td>{h(item.get('attempts'))}/{1 + int(item.get('max_retries') or 0)}</td>
              <td>{seconds_label(item.get('runtime_seconds'))}<div class='tiny'>lease {h(item.get('lease_expires_at') or '—')}</div></td>
              <td class='nowrap'>{h(item.get('started_at') or '—')}</td>
            </tr>
            """
            for item in running
        )
    return f"""
    <table>
      <thead><tr><th>Slot</th><th>Task</th><th>Job</th><th>Index</th><th>Key</th><th>Type</th><th>Attempt</th><th>Runtime</th><th>Started</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    """


def session_worker_assignment_table(assignments: dict[str, Any]) -> str:
    workers = assignments.get("workers") or []
    if not workers:
        rows = empty_row(11, "No worker assignments have been recorded for this session yet.")
    else:
        row_parts = []
        for worker in workers:
            active_instances = [inst for inst in worker.get("instances", []) if inst.get("session_tasks") or inst.get("state") in {"running", "drained"}]
            slot_bits = []
            for inst in active_instances[:12]:
                current = inst.get("current_task") or {}
                label = f"{inst.get('instance_id')}:{inst.get('state')}"
                if current.get("task_id"):
                    label += f" {str(current.get('task_id'))[:12]}…"
                slot_bits.append(h(label))
            if len(active_instances) > 12:
                slot_bits.append(h(f"+{len(active_instances) - 12} more"))
            row_parts.append(
                f"""
                <tr>
                  <td><a href='/ui/workers/{path_id(worker.get('worker_id'))}'><code>{h(worker.get('worker_id'))}</code></a><div class='tiny'>{h(worker.get('hostname') or '')}</div></td>
                  <td>{status_pill(worker.get('state'))}</td>
                  <td>{h(worker.get('service_name') or '—')}<div class='tiny'>{h(worker.get('service_version') or '—')}</div></td>
                  <td>{h(worker.get('session_tasks', 0))}</td>
                  <td>{h(worker.get('running_tasks', 0))}</td>
                  <td>{h(worker.get('completed_tasks', 0))}</td>
                  <td>{h(worker.get('failed_tasks', 0))}</td>
                  <td>{h(worker.get('cancelled_tasks', 0))}</td>
                  <td>{h(worker.get('active_instances', 0))}/{h(worker.get('desired_instances', 0))}</td>
                  <td>{', '.join(slot_bits) or '—'}</td>
                  <td class='nowrap'>{h(worker.get('last_heartbeat_at') or '—')}</td>
                </tr>
                """
            )
        rows = "".join(row_parts)
    return f"""
    <table>
      <thead><tr><th>Worker</th><th>State</th><th>Service</th><th>Session Tasks</th><th>Running</th><th>Done</th><th>Failed</th><th>Cancelled</th><th>Slots</th><th>Instances</th><th>Heartbeat</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    """


def session_task_assignment_table(assignments: dict[str, Any]) -> str:
    tasks = assignments.get("tasks") or []
    if not tasks:
        rows = empty_row(12, "No tasks in this session yet.")
    else:
        rows = "".join(
            f"""
            <tr>
              <td><a href='/ui/jobs/{path_id(item.get('job_id'))}'><code>{short_id(item.get('job_id'))}</code></a><div class='tiny'>{h(item.get('job_name') or '')}</div></td>
              <td>{h(item.get('input_index') if item.get('input_index') is not None else '—')}</td>
              <td><code>{h(item.get('input_key') or '—')}</code></td>
              <td><a href='/ui/tasks/{path_id(item.get('task_id'))}'><code>{short_id(item.get('task_id'))}</code></a></td>
              <td>{status_pill(item.get('status'))}</td>
              <td>{h(item.get('task_type') or '')}</td>
              <td><code>{h(item.get('worker_id') or '—')}</code></td>
              <td><code>{h(item.get('instance_id') or '—')}</code></td>
              <td>{h(item.get('attempts'))}/{1 + int(item.get('max_retries') or 0)}</td>
              <td>{seconds_label(item.get('runtime_seconds'))}</td>
              <td class='nowrap'>{h(item.get('started_at') or '—')}<div class='tiny'>updated {h(item.get('updated_at') or '—')}</div></td>
              <td>{h((item.get('error') or '')[:120])}</td>
            </tr>
            """
            for item in tasks
        )
    return f"""
    <table>
      <thead><tr><th>Job</th><th>Index</th><th>Key</th><th>Task</th><th>Status</th><th>Type</th><th>Worker</th><th>Instance</th><th>Attempt</th><th>Runtime</th><th>Start / Updated</th><th>Error</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    """

def executor_slot_table(summary: dict[str, Any]) -> str:
    instances = summary.get("instances") or []
    if not instances:
        rows = empty_row(13, "No worker instances have heartbeated yet.")
    else:
        rows = "".join(
            f"""
            <tr>
              <td><a href='/ui/workers/{path_id(item.get('worker_id'))}'><code>{h(item.get('worker_id'))}</code></a><div class='tiny'>{h(item.get('hostname') or '')}</div></td>
              <td><a href='/ui/executors/{path_id(item.get('worker_id'))}/instances/{path_id(item.get('instance_id'))}'><code>{h(item.get('instance_id'))}</code></a><div class='tiny'>{h(item.get('executor_id'))}</div></td>
              <td>{status_pill(item.get('state'))}<div class='tiny'>worker {h(item.get('worker_state') or '—')}</div></td>
              <td>{h(item.get('service_name') or '—')}<div class='tiny'>{h(item.get('service_version') or '—')}</div></td>
              <td>{h(item.get('taskgrid_version') or '—')}</td>
              <td>{h(item.get('status') or '—')}</td>
              <td>{h(item.get('tasks_completed', 0))}</td>
              <td>{h(item.get('last_poll_at') or '—')}</td>
              <td>{_executor_current_task_cell(item)}</td>
              <td>{_executor_session_cell(item)}</td>
              <td>{_executor_job_cell(item)}</td>
              <td>{h(item.get('drain_reason') or '—')}</td>
              <td><a class='button secondary' href='/ui/workers/{path_id(item.get('worker_id'))}/logs/{path_id(item.get('log_path') or '')}'>Log</a></td>
            </tr>
            """
            for item in instances
        )
    return f"""
    <table>
      <thead><tr><th>Worker</th><th>Instance</th><th>State</th><th>Service</th><th>TaskGrid</th><th>Advertised Status</th><th>Done</th><th>Last Poll</th><th>Current Task</th><th>Session</th><th>Job</th><th>Drain Reason</th><th>Log</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    """


def _executor_current_task_cell(item: dict[str, Any]) -> str:
    task = item.get("current_task") or {}
    advertised = item.get("advertised_current_task_id")
    if task.get("task_id"):
        return f"<a href='/ui/tasks/{path_id(task.get('task_id'))}'><code>{short_id(task.get('task_id'))}</code></a><div class='tiny'>{h(task.get('task_type') or '')} · runtime {seconds_label(task.get('runtime_seconds'))}</div>"
    if advertised:
        return f"<code>{h(advertised)}</code><div class='tiny'>advertised by worker, not running in DB</div>"
    return "—"


def _executor_session_cell(item: dict[str, Any]) -> str:
    task = item.get("current_task") or {}
    session_id = task.get("session_id") or item.get("session_id")
    if not session_id:
        return "—"
    return f"<a href='/ui/sessions/{path_id(session_id)}'><code>{short_id(session_id)}</code></a><div class='tiny'>{h(task.get('session_name') or '')}</div>"


def _executor_job_cell(item: dict[str, Any]) -> str:
    task = item.get("current_task") or {}
    job_id = task.get("job_id") or item.get("job_id")
    if not job_id:
        return "—"
    return f"<a href='/ui/jobs/{path_id(job_id)}'><code>{short_id(job_id)}</code></a><div class='tiny'>{h(task.get('job_name') or '')}</div>"


def executor_recent_task_table(tasks: list[dict[str, Any]]) -> str:
    if not tasks:
        rows = empty_row(11, "No task history has been assigned to this instance yet.")
    else:
        rows = "".join(
            f"""
            <tr>
              <td><a href='/ui/tasks/{path_id(item.get('task_id'))}'><code>{short_id(item.get('task_id'))}</code></a></td>
              <td>{status_pill(item.get('status'))}</td>
              <td>{h(item.get('task_type') or '')}</td>
              <td>{h(item.get('input_index') if item.get('input_index') is not None else '—')}</td>
              <td><code>{h(item.get('input_key') or '—')}</code></td>
              <td><a href='/ui/sessions/{path_id(item.get('session_id'))}'><code>{short_id(item.get('session_id'))}</code></a><div class='tiny'>{h(item.get('session_name') or '')}</div></td>
              <td><a href='/ui/jobs/{path_id(item.get('job_id'))}'><code>{short_id(item.get('job_id'))}</code></a><div class='tiny'>{h(item.get('job_name') or '')}</div></td>
              <td>{h(item.get('attempts'))}/{1 + int(item.get('max_retries') or 0)}</td>
              <td>{seconds_label(item.get('runtime_seconds'))}</td>
              <td class='nowrap'>{h(item.get('started_at') or '—')}<div class='tiny'>updated {h(item.get('updated_at') or '—')}</div></td>
              <td>{h((item.get('error') or '')[:120])}</td>
            </tr>
            """
            for item in tasks
        )
    return f"""
    <table>
      <thead><tr><th>Task</th><th>Status</th><th>Type</th><th>Index</th><th>Key</th><th>Session</th><th>Job</th><th>Attempt</th><th>Runtime</th><th>Start / Updated</th><th>Error</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    """


def manager_log_table(text: str, *, max_rows: int = 500) -> str:
    records: list[dict[str, Any]] = []
    malformed = 0
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            malformed += 1
            continue
        if isinstance(parsed, dict):
            records.append(parsed)
        else:
            malformed += 1

    records = records[-max_rows:]
    if not records:
        detail = "No manager log entries yet."
        if malformed:
            detail += f" Skipped {malformed} malformed/partial line(s)."
        rows = empty_row(7, detail)
    else:
        rows = "".join(
            f"""
            <tr>
              <td class='nowrap'>{h(r.get('at', ''))}</td>
              <td>{h(r.get('level', ''))}</td>
              <td><code>{h(r.get('code', ''))}</code></td>
              <td>{h(r.get('entity_type', ''))}</td>
              <td><code>{short_id(r.get('entity_id', ''))}</code></td>
              <td>{h(r.get('message', ''))}</td>
              <td><pre>{pretty_json(r.get('data', {}))}</pre></td>
            </tr>
            """
            for r in reversed(records)
        )
    note = f"<div class='tiny'>Showing latest {len(records)} parsed entries."
    if malformed:
        note += f" Skipped {malformed} malformed/partial line(s), usually from reading only the tail of a large log."
    note += "</div>"
    return f"""
    {note}
    <table>
      <thead><tr><th>At</th><th>Level</th><th>Code</th><th>Entity</th><th>ID</th><th>Message</th><th>Data</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    """


def queue_reason_table(diagnostics: dict[str, Any]) -> str:
    reasons = diagnostics.get("reason_counts") or {}
    if not reasons:
        rows = empty_row(3, "No queued tasks right now.")
    else:
        rows = "".join(
            f"<tr><td>{status_pill(reason)}</td><td>{h(count)}</td><td>{h(_queue_reason_copy(reason))}</td></tr>"
            for reason, count in sorted(reasons.items(), key=lambda item: (-int(item[1]), str(item[0])))
        )
    return f"""
    <table>
      <thead><tr><th>Reason</th><th>Tasks</th><th>Meaning</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    """


def _queue_reason_copy(reason: str) -> str:
    return {
        "leaseable_now": "Eligible for lease when a capable worker polls.",
        "job_paused": "The job is paused.",
        "session_paused": "The service session is paused.",
        "job_not_leaseable": "The job is cancelling or cancelled.",
        "no_workers_registered": "No workers have heartbeated.",
        "no_worker_supports_task_type": "No worker advertises this task type.",
        "missing_required_tags": "Workers support the task type, but required tags do not match.",
        "all_capable_workers_disabled": "All matching workers are disabled.",
        "all_capable_workers_draining": "All matching workers are draining.",
        "all_capable_workers_stale": "All matching workers are stale/offline.",
        "no_active_capable_worker": "No active worker currently passes all gates.",
        "all_capable_workers_busy": "Matching workers exist but all advertised slots look busy/drained.",
    }.get(str(reason), "Scheduler diagnostic reason.")


def queue_group_table(groups: list[dict[str, Any]], kind: str) -> str:
    if not groups:
        rows = empty_row(6, "No queued backlog groups.")
    else:
        row_parts = []
        for item in groups[:100]:
            if kind == "session":
                label = f"<a href='/ui/sessions/{path_id(item.get('session_id'))}'><code>{short_id(item.get('session_id'))}</code></a><div class='tiny'>{h(item.get('name') or '')}</div>"
                status = item.get("status") or ""
            elif kind == "job":
                label = f"<a href='/ui/jobs/{path_id(item.get('job_id'))}'><code>{short_id(item.get('job_id'))}</code></a><div class='tiny'>{h(item.get('name') or '')}</div>"
                status = item.get("status") or ""
            else:
                label = f"<code>{h(item.get('task_type') or '—')}</code>"
                status = ""
            reasons = ", ".join(f"{h(key)}:{h(value)}" for key, value in sorted((item.get('reasons') or {}).items()))
            row_parts.append(
                f"""
                <tr>
                  <td>{label}</td>
                  <td>{status_pill(status) if status else '—'}</td>
                  <td>{h(item.get('queued_tasks', 0))}</td>
                  <td>{h(item.get('leaseable_now', 0))}</td>
                  <td>{h(item.get('blocked_tasks', 0))}</td>
                  <td>{reasons or '—'}</td>
                </tr>
                """
            )
        rows = "".join(row_parts)
    return f"""
    <table>
      <thead><tr><th>{h(kind.title())}</th><th>Status</th><th>Queued</th><th>Leaseable</th><th>Blocked</th><th>Reasons</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    """


def queue_task_table(diagnostics: dict[str, Any]) -> str:
    tasks = diagnostics.get("tasks") or []
    if not tasks:
        rows = empty_row(10, "No queued tasks right now.")
    else:
        row_parts = []
        for item in tasks[:500]:
            row_parts.append(
                f"""
                <tr>
                  <td><a href='/ui/tasks/{path_id(item.get('task_id'))}'><code>{short_id(item.get('task_id'))}</code></a></td>
                  <td><a href='/ui/sessions/{path_id(item.get('session_id'))}'><code>{short_id(item.get('session_id') or '—')}</code></a><div class='tiny'>{h(item.get('session_name') or '')}</div></td>
                  <td><a href='/ui/jobs/{path_id(item.get('job_id'))}'><code>{short_id(item.get('job_id'))}</code></a><div class='tiny'>{h(item.get('job_name') or '')}</div></td>
                  <td><code>{h(item.get('task_type') or '')}</code></td>
                  <td>{h(item.get('input_index') if item.get('input_index') is not None else '—')}</td>
                  <td><code>{h(item.get('input_key') or '—')}</code></td>
                  <td>{status_pill(item.get('reason'))}</td>
                  <td>{''.join(f"<span class='pill'>{h(tag)}</span> " for tag in item.get('required_tags', [])) or '—'}</td>
                  <td>{h(item.get('attempts'))}/{1 + int(item.get('max_retries') or 0)}</td>
                  <td>{h(item.get('detail') or '')}</td>
                </tr>
                """
            )
        rows = "".join(row_parts)
    return f"""
    <table>
      <thead><tr><th>Task</th><th>Session</th><th>Job</th><th>Type</th><th>Index</th><th>Key</th><th>Reason</th><th>Required Tags</th><th>Attempt</th><th>Detail</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    """


def queue_worker_table(diagnostics: dict[str, Any]) -> str:
    workers = diagnostics.get("workers") or []
    if not workers:
        rows = empty_row(9, "No workers have heartbeated yet.")
    else:
        rows = "".join(
            f"""
            <tr>
              <td><a href='/ui/workers/{path_id(worker.get('worker_id'))}'><code>{h(worker.get('worker_id'))}</code></a><div class='tiny'>{h(worker.get('hostname') or '')}</div></td>
              <td>{status_pill('active' if worker.get('active') else ('disabled' if worker.get('disabled') else ('draining' if worker.get('draining') else 'stale')))}</td>
              <td>{h(worker.get('service_name') or '—')}<div class='tiny'>{h(worker.get('service_version') or '—')}</div></td>
              <td>{', '.join(f'<code>{h(t)}</code>' for t in worker.get('task_types', [])) or '<span class="tiny">open/legacy</span>'}</td>
              <td>{''.join(f"<span class='pill'>{h(tag)}</span> " for tag in worker.get('tags', [])) or '—'}</td>
              <td>{h(worker.get('running_slots', 0))}/{h(worker.get('active_slots', 0))}</td>
              <td>{h(worker.get('drained_slots', 0))}</td>
              <td>{h(worker.get('estimated_free_slots', 0))}</td>
              <td class='nowrap'>{h(worker.get('last_heartbeat_at') or '—')}</td>
            </tr>
            """
            for worker in workers
        )
    return f"""
    <table>
      <thead><tr><th>Worker</th><th>State</th><th>Service</th><th>Task Types</th><th>Tags</th><th>Busy/Slots</th><th>Drained</th><th>Est. Free</th><th>Heartbeat</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    """


def worker_logs_button(worker: dict[str, Any]) -> str:
    metadata = worker.get("metadata", {}) or {}
    if metadata.get("log_url"):
        return f"<a class='button secondary' href='/ui/workers/{path_id(worker['id'])}/logs'>Logs</a>"
    return "<span class='tiny'>not advertised</span>"


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    stats = core.dashboard_stats()
    sessions = core.list_sessions(limit=8)
    jobs = core.list_jobs(limit=12)
    workers = core.list_workers(limit=12)
    events = core.list_events(limit=20)
    worker_rows = "".join(
        f"<tr><td><code>{short_id(w['id'])}</code></td><td>{h(w['hostname'])}</td><td>{status_pill(w['status'])}</td><td>{h(w.get('running_tasks', 0))}/{h(w.get('active_concurrency', w.get('desired_concurrency', 1)))}</td><td>{h(w.get('desired_concurrency', 1))}</td><td>{worker_logs_button(w)}</td><td class='nowrap'>{h(w['last_heartbeat_at'])}</td></tr>"
        for w in workers
    ) or empty_row(7, "No workers have checked in yet.")
    body = f"""
    <div class='topline'>
      <div><h1>Dashboard</h1><div class='muted'>Auto-refreshes every 5 seconds · DB <code>{h(db_path())}</code></div></div>
      <div class='actions'><a class='button' href='/ui/submit'>Submit Job</a><a class='button secondary' href='/ui/manager/recovery'>Recovery</a><a class='button secondary' href='/docs'>API Docs</a></div>
    </div>
    <div class='grid stats'>
      <div class='card stat'><div class='label'>Sessions</div><div class='value'>{h(stats['sessions_total'])}</div><div class='tiny'>queued {h(stats['sessions_queued'])} · running {h(stats['sessions_running'])}</div></div>
      <div class='card stat'><div class='label'>Tasks</div><div class='value'>{h(stats['tasks_total'])}</div><div class='tiny'>queued {h(stats['tasks_queued'])} · running {h(stats['tasks_running'])}</div></div>
      <div class='card stat'><div class='label'>Succeeded</div><div class='value'>{h(stats['tasks_succeeded'])}</div><div class='tiny'>failed {h(stats['tasks_failed'])} · cancelled {h(stats['tasks_cancelled'])}</div></div>
      <div class='card stat'><div class='label'>Workers</div><div class='value'>{h(stats['workers_total'])}</div><div class='tiny'>active {h(stats.get('workers_active', 0))} · stale {h(stats.get('workers_stale', 0))} · expired leases {h(stats.get('tasks_expired_leases', 0))}</div></div>
    </div>
    <div class='grid two' style='margin-top:16px'>
      <section class='card'><h2>Recent Service Sessions</h2>{session_table(sessions)}</section>
      <section class='card'><h2>Workers</h2><table><thead><tr><th>ID</th><th>Host</th><th>Status</th><th>Busy</th><th>Instances</th><th>Logs</th><th>Heartbeat</th></tr></thead><tbody>{worker_rows}</tbody></table></section>
    </div>
    <section class='card' style='margin-top:16px'><h2>Recent Events</h2>{event_table(events)}</section>
    """
    return layout("Dashboard", body, refresh=True)



@router.get("/sessions", response_class=HTMLResponse)
def sessions_page(status: str | None = None) -> str:
    sessions = core.list_sessions(limit=200, status=status)
    links = " ".join(
        f"<a class='button {'secondary' if status != value else ''}' href='/ui/sessions{('?status=' + value) if value else ''}'>{label}</a>"
        for value, label in [(None, "All"), ("queued", "Queued"), ("running", "Running"), ("succeeded", "Succeeded"), ("failed", "Failed"), ("cancelled", "Cancelled")]
    )
    body = f"""
    <div class='topline'>
      <div><h1>Service Sessions</h1><div class='muted'>Each client submission creates a session that groups the created job, task counts, and lifecycle timing.</div></div>
      <div class='actions'><a class='button' href='/ui/submit'>Submit Job</a></div>
    </div>
    <section class='card'><div class='actions' style='margin-bottom:14px'>{links}</div>{session_table(sessions)}</section>
    """
    return layout("Service Sessions", body, refresh=status in {"queued", "running"})


@router.get("/sessions/{session_id}", response_class=HTMLResponse)
def session_detail(session_id: str) -> str:
    session = core.get_session(session_id)
    if not session:
        return layout("Session not found", f"<section class='card'><h1>Session not found</h1><p class='muted'><code>{h(session_id)}</code></p></section>")
    jobs = core.list_session_jobs(session_id, limit=500)
    assignments = core.get_session_assignments(session_id, limit=1000) or {}
    status_counts = assignments.get("status_counts", {})
    events = core.list_events(entity_type="service_session", entity_id=session_id, limit=80)
    body = f"""
    <div class='topline'>
      <div><h1>{h(session['name'])}</h1><div class='muted'><code>{h(session['id'])}</code> · created {h(session.get('created_at'))}</div></div>
      <div class='actions'><a class='button secondary' href='/ui/sessions'>Back to Sessions</a><a class='button secondary' href='/ui/sessions/{path_id(session_id)}/history'>Task History</a><a class='button secondary' href='/ui/sessions/{path_id(session_id)}/results'>Results</a><a class='button secondary' href='/sessions/{path_id(session_id)}/results/export?format=json'>Download JSON</a><a class='button secondary' href='/sessions/{path_id(session_id)}/results/export?format=csv'>Download CSV</a><a class='button secondary' href='/sessions/{path_id(session_id)}/results'>Raw JSON</a><a class='button secondary' href='/sessions/{path_id(session_id)}/assignments'>Assignments JSON</a>{'<form method="post" action="/ui/sessions/' + path_id(session_id) + '/resume"><button type="submit">Resume</button></form>' if int(session.get('paused') or 0) else '<form method="post" action="/ui/sessions/' + path_id(session_id) + '/pause"><button class="secondary" type="submit">Pause</button></form>'}</div>
    </div>
    <div class='grid stats'>
      <div class='card stat'><div class='label'>Status</div><div class='value' style='font-size:20px'>{status_pill(session['status'])}{' ' + status_pill('paused') if int(session.get('paused') or 0) else ''}</div><div class='tiny'>{h(session.get('pause_reason') or '')}</div></div>
      <div class='card stat'><div class='label'>Task Progress</div>{progress_bar(int(session.get('completed_tasks') or 0), int(session.get('total_tasks') or 0))}</div>
      <div class='card stat'><div class='label'>Pending / Running</div><div class='value'>{h(session.get('queued_tasks', 0))}/{h(session.get('running_tasks', 0))}</div></div>
      <div class='card stat'><div class='label'>Failed / Total Tasks</div><div class='value'>{h(session.get('failed_tasks', 0))}/{h(session.get('total_tasks', 0))}</div></div>
    </div>
    <div class='grid stats' style='margin-top:16px'>
      <div class='card stat'><div class='label'>Priority</div><div class='value'>{h(session.get('priority', 0))}</div></div>
      <div class='card stat'><div class='label'>Jobs</div><div class='value'>{h(session.get('total_jobs', 0))}</div></div>
      <div class='card stat'><div class='label'>Client</div><div class='value' style='font-size:14px'><code>{h(session.get('client_id') or '—')}</code></div></div>
      <div class='card stat'><div class='label'>Created</div><div class='value' style='font-size:16px'>{h(session.get('created_at'))}</div></div>
      <div class='card stat'><div class='label'>Started</div><div class='value' style='font-size:16px'>{h(session.get('started_at') or '—')}</div></div>
      <div class='card stat'><div class='label'>Finished</div><div class='value' style='font-size:16px'>{h(session.get('finished_at') or '—')}</div></div>
    </div>
    <section class='card' style='margin-top:16px'>
      <h2>Live Assignment Stats</h2>
      <p class='muted'>Session-scoped execution view. Shows running assignments by worker instance plus all queued/running/final task placement for this session.</p>
      <div class='grid stats'>
        <div class='card stat'><div class='label'>Running Assignments</div><div class='value'>{h(assignments.get('running_assignments', 0))}</div></div>
        <div class='card stat'><div class='label'>Assigned Workers</div><div class='value'>{h(assignments.get('assigned_workers', 0))}</div></div>
        <div class='card stat'><div class='label'>Assigned Instances</div><div class='value'>{h(assignments.get('assigned_instances', 0))}</div></div>
        <div class='card stat'><div class='label'>Status Mix</div><div class='value' style='font-size:16px'>Q {h(status_counts.get('queued', 0))} · R {h(status_counts.get('running', 0))} · OK {h(status_counts.get('succeeded', 0))} · F {h(status_counts.get('failed', 0))}</div></div>
      </div>
      <h2 style='margin-top:18px'>Current Running Assignments</h2>
      {session_current_assignment_table(assignments)}
      <h2 style='margin-top:18px'>Worker / Instance Rollup</h2>
      {session_worker_assignment_table(assignments)}
      <h2 style='margin-top:18px'>Task Assignment Drilldown</h2>
      {session_task_assignment_table(assignments)}
    </section>
    <div class='grid two' style='margin-top:16px'>
      <section class='card'><h2>Created Jobs</h2>{job_table(jobs)}</section>
      <section class='card'>
        <h2>Session Priority</h2>
        <p class='muted'>Changing priority reprioritizes queued tasks in this session. Running tasks are not interrupted.</p>
        <form class='actions' method='post' action='/ui/sessions/{path_id(session_id)}/priority'>
          <input name='priority' type='number' value='{h(session.get('priority', 0))}' style='max-width:140px'>
          <button type='submit'>Apply Priority</button>
        </form>
        <h2 style='margin-top:18px'>Reconnect</h2>
        <p class='muted'>A client can reattach with this client ID and resume token to list/download its sessions after disconnecting.</p>
        <pre>client_id={h(session.get('client_id') or '')}
resume_token={h(session.get('resume_token') or '')}</pre>
        <h2 style='margin-top:18px'>Session Metadata</h2><pre>{pretty_json(session.get('metadata', {}))}</pre><h2 style='margin-top:18px'>Session Events</h2>{event_table(events)}
      </section>
    </div>
    """
    return layout(f"Session {session_id}", body, refresh=session["status"] in {"queued", "running"})




@router.post("/sessions/{session_id}/priority")
async def session_priority_from_ui(session_id: str, request: Request) -> RedirectResponse:
    form = await request.form()
    try:
        priority = int(str(form.get("priority") or "0"))
    except ValueError:
        priority = 0
    core.set_session_priority(session_id, priority, updated_by="ui")
    return RedirectResponse(url=f"/ui/sessions/{path_id(session_id)}", status_code=303)


@router.post("/sessions/{session_id}/pause")
async def session_pause_from_ui(session_id: str, request: Request) -> RedirectResponse:
    form = await request.form()
    core.set_session_paused(session_id, True, reason=str(form.get("reason") or "").strip() or None, updated_by="ui")
    return RedirectResponse(url=f"/ui/sessions/{path_id(session_id)}", status_code=303)


@router.post("/sessions/{session_id}/resume")
def session_resume_from_ui(session_id: str) -> RedirectResponse:
    core.set_session_paused(session_id, False, updated_by="ui")
    return RedirectResponse(url=f"/ui/sessions/{path_id(session_id)}", status_code=303)


@router.post("/sessions/{session_id}/tasks/retry-failed")
def session_retry_failed_tasks_from_ui(session_id: str) -> RedirectResponse:
    core.bulk_update_tasks(action="retry", session_id=session_id, statuses=["failed", "cancelled"], updated_by="ui")
    return RedirectResponse(url=f"/ui/sessions/{path_id(session_id)}/history", status_code=303)


@router.post("/sessions/{session_id}/tasks/cancel-queued")
def session_cancel_queued_tasks_from_ui(session_id: str) -> RedirectResponse:
    core.bulk_update_tasks(action="cancel", session_id=session_id, statuses=["queued"], updated_by="ui")
    return RedirectResponse(url=f"/ui/sessions/{path_id(session_id)}/history?status=cancelled", status_code=303)


@router.post("/sessions/{session_id}/tasks/cancel-running")
def session_cancel_running_tasks_from_ui(session_id: str) -> RedirectResponse:
    core.bulk_update_tasks(action="cancel", session_id=session_id, statuses=["running"], include_running=True, updated_by="ui")
    return RedirectResponse(url=f"/ui/sessions/{path_id(session_id)}/history?status=cancelled", status_code=303)


@router.get("/sessions/{session_id}/history", response_class=HTMLResponse)
def session_task_history_page(session_id: str, status: str | None = None, job_id: str | None = None, order: str = "input", limit: int = 5000) -> str:
    history = core.get_session_task_history(session_id, status=status, job_id=job_id, order=order, limit=limit)
    if not history:
        return layout("Session history not found", f"<section class='card'><h1>Session not found</h1><p class='muted'><code>{h(session_id)}</code></p></section>")
    status_counts = history.get("status_counts") or {}
    status_links = " ".join(
        f"<a class='button {'secondary' if status != value else ''}' href='/ui/sessions/{path_id(session_id)}/history{('?status=' + value) if value else ''}'>{label}</a>"
        for value, label in [(None, "All"), ("queued", "Queued"), ("running", "Running"), ("succeeded", "Succeeded"), ("failed", "Failed"), ("cancelled", "Cancelled")]
    )
    job_options = "".join(
        f"<option value='{h(job.get('job_id'))}' {'selected' if job_id == job.get('job_id') else ''}>{short_id(job.get('job_id'))} · {h(job.get('name') or '')}</option>"
        for job in history.get("jobs", [])
    )
    body = f"""
    <div class='topline'>
      <div><h1>Session Task History</h1><div class='muted'><code>{h(history['session_id'])}</code> · {h(history['name'])} · finished {h(history.get('finished_at') or '—')}</div></div>
      <div class='actions'><a class='button secondary' href='/ui/sessions/{path_id(session_id)}'>Back to Session</a><a class='button secondary' href='/ui/sessions/{path_id(session_id)}/results'>Results</a><a class='button secondary' href='/sessions/{path_id(session_id)}/tasks/history'>Raw JSON</a><a class='button secondary' href='/sessions/{path_id(session_id)}/results/export?format=csv'>Download CSV</a></div>
    </div>
    <div class='grid stats'>
      <div class='card stat'><div class='label'>Status</div><div class='value' style='font-size:20px'>{status_pill(history['status'])}</div></div>
      <div class='card stat'><div class='label'>Tasks in View</div><div class='value'>{h(history.get('task_count', 0))}</div><div class='tiny'>session total {h(history.get('total_tasks', 0))}</div></div>
      <div class='card stat'><div class='label'>Final Workers / Instances</div><div class='value'>{h(history.get('assigned_workers', 0))}/{h(history.get('assigned_instances', 0))}</div></div>
      <div class='card stat'><div class='label'>Avg Runtime</div><div class='value'>{seconds_label(history.get('average_runtime_seconds'))}</div></div>
      <div class='card stat'><div class='label'>Status Mix</div><div class='value' style='font-size:16px'>Q {h(status_counts.get('queued', 0))} · R {h(status_counts.get('running', 0))} · OK {h(status_counts.get('succeeded', 0))} · F {h(status_counts.get('failed', 0))} · C {h(status_counts.get('cancelled', 0))}</div></div>
    </div>
    <section class='card' style='margin-top:16px'>
      <h2>Filters</h2>
      <div class='actions' style='margin-bottom:12px'>{status_links}</div>
      <form class='actions' method='get' action='/ui/sessions/{path_id(session_id)}/history'>
        <label>Job <select name='job_id'><option value=''>All jobs</option>{job_options}</select></label>
        <label>Order <select name='order'>
          <option value='input' {'selected' if order == 'input' else ''}>Input</option>
          <option value='completed' {'selected' if order in {'completed','finished','finished_at'} else ''}>Completed</option>
          <option value='started' {'selected' if order in {'started','started_at'} else ''}>Started</option>
          <option value='status' {'selected' if order == 'status' else ''}>Status</option>
        </select></label>
        <label>Limit <input name='limit' type='number' min='1' max='10000' value='{h(limit)}' style='max-width:120px'></label>
        {f"<input type='hidden' name='status' value='{h(status)}'>" if status else ""}
        <button type='submit'>Apply</button>
      </form>
    </section>

    <section class='card' style='margin-top:16px'>
      <h2>Task Controls</h2>
      <p class='muted'>Bulk actions apply to tasks in this session. Retry only affects failed/cancelled tasks; cancel queued does not touch running work. Force-cancel running marks currently running tasks cancelled and ignores late worker completions.</p>
      <div class='actions'>
        <form method='post' action='/ui/sessions/{path_id(session_id)}/tasks/retry-failed' onsubmit="return confirm('Retry failed/cancelled tasks in this session?');"><button type='submit'>Retry Failed / Cancelled</button></form>
        <form method='post' action='/ui/sessions/{path_id(session_id)}/tasks/cancel-queued' onsubmit="return confirm('Cancel queued tasks in this session? Running tasks are not touched.');"><button class='secondary' type='submit'>Cancel Queued</button></form>
        <form method='post' action='/ui/sessions/{path_id(session_id)}/tasks/cancel-running' onsubmit="return confirm('Force-cancel currently running tasks in this session? Late worker results will be ignored.');"><button class='danger' type='submit'>Force Cancel Running</button></form>
      </div>
    </section>
    <section class='card' style='margin-top:16px'>
      <h2>Each Task History</h2>
      <p class='muted'>Durable session history. This is not limited to current assignments: finished sessions keep every task row, final worker/instance slot, attempts, timing, result/error, and links back to task/job detail.</p>
      {session_task_history_table(history)}
    </section>
    """
    return layout(f"Session task history {session_id}", body, refresh=history["status"] in {"queued", "running"})


@router.get("/sessions/{session_id}/results", response_class=HTMLResponse)
def session_results_page(session_id: str) -> str:
    results = core.get_session_results(session_id)
    if not results:
        return layout("Session results not found", f"<section class='card'><h1>Session not found</h1><p class='muted'><code>{h(session_id)}</code></p></section>")
    body = f"""
    <div class='topline'>
      <div><h1>Session Results</h1><div class='muted'><code>{h(results['session_id'])}</code> · {h(results['name'])}</div></div>
      <div class='actions'><a class='button secondary' href='/ui/sessions/{path_id(session_id)}'>Back to Session</a><a class='button secondary' href='/sessions/{path_id(session_id)}/results'>Raw JSON</a><a class='button secondary' href='/sessions/{path_id(session_id)}/results/export?format=json'>Download JSON</a><a class='button secondary' href='/sessions/{path_id(session_id)}/results/export?format=csv'>Download CSV</a><a class='button secondary' href='/sessions/{path_id(session_id)}/results/export?format=csv&failed_only=true'>Failed CSV</a></div>
    </div>
    <div class='grid stats'>
      <div class='card stat'><div class='label'>Status</div><div class='value' style='font-size:20px'>{status_pill(results['status'])}</div></div>
      <div class='card stat'><div class='label'>Priority</div><div class='value'>{h(results.get('priority', 0))}</div></div>
      <div class='card stat'><div class='label'>Progress</div>{progress_bar(int(results.get('completed_tasks') or 0), int(results.get('total_tasks') or 0))}</div>
      <div class='card stat'><div class='label'>Pending / Running</div><div class='value'>{h(results.get('queued_tasks', 0))}/{h(results.get('running_tasks', 0))}</div></div>
      <div class='card stat'><div class='label'>Failed / Total</div><div class='value'>{h(results.get('failed_tasks', 0))}/{h(results.get('total_tasks', 0))}</div></div>
    </div>
    <section class='card' style='margin-top:16px'>
      <h2>Jobs in Session</h2>
      {job_table([{'id': job['job_id'], 'name': job['name'], 'task_type': job['task_type'], 'status': job['status'], 'completed_tasks': job['completed_tasks'], 'total_tasks': job['total_tasks'], 'failed_tasks': job['failed_tasks'], 'priority': job.get('priority', 0), 'created_at': job.get('created_at')} for job in results.get('jobs', [])])}
    </section>
    <section class='card' style='margin-top:16px'>
      <h2>Task Results</h2>
      <p class='muted'>This is what the client reads from <code>GET /sessions/{h(session_id)}/results</code>. It includes the original payload, final task status, assigned worker, and result/error.</p>
      {result_table(results.get('tasks', []), include_job=True)}
    </section>
    """
    return layout(f"Session results {session_id}", body, refresh=results["status"] in {"queued", "running"})

@router.get("/client-sessions", response_class=HTMLResponse)
def client_sessions_page(client_id: str | None = None, resume_token: str | None = None, status: str | None = None) -> str:
    sessions: list[dict[str, Any]] = []
    looked = bool(client_id and resume_token)
    if looked:
        sessions = core.list_client_sessions(client_id or "", resume_token or "", limit=200, status=status)
    result_html = ""
    if looked:
        result_html = f"""
        <section class='card' style='margin-top:16px'>
          <h2>Resumable Sessions</h2>
          {session_table(sessions)}
        </section>
        """
    body = f"""
    <div class='topline'>
      <div><h1>Client Resume</h1><div class='muted'>List sessions for a reconnecting client using its client ID and resume token.</div></div>
    </div>
    <section class='card'>
      <form class='stack' method='get' action='/ui/client-sessions'>
        <div class='form-row'>
          <label>Client ID<input name='client_id' value='{h(client_id or '')}' required></label>
          <label>Resume token<input name='resume_token' value='{h(resume_token or '')}' required></label>
          <label>Status filter<input name='status' placeholder='optional' value='{h(status or '')}'></label>
        </div>
        <div class='actions'><button type='submit'>Find Sessions</button></div>
      </form>
    </section>
    {result_html}
    """
    return layout("Client Resume", body)


@router.get("/jobs", response_class=HTMLResponse)
def jobs_page(status: str | None = None) -> str:
    jobs = core.list_jobs(limit=200, status=status)
    links = " ".join(
        f"<a class='button {'secondary' if status != value else ''}' href='/ui/jobs{('?status=' + value) if value else ''}'>{label}</a>"
        for value, label in [(None, "All"), ("queued", "Queued"), ("running", "Running"), ("succeeded", "Succeeded"), ("failed", "Failed"), ("cancelled", "Cancelled")]
    )
    body = f"""
    <div class='topline'>
      <div><h1>Jobs</h1><div class='muted'>Filter, inspect, cancel, and review task progress.</div></div>
      <div class='actions'><a class='button' href='/ui/submit'>Submit Job</a></div>
    </div>
    <section class='card'><div class='actions' style='margin-bottom:14px'>{links}</div>{job_table(jobs)}</section>
    """
    return layout("Jobs", body, refresh=status in {"queued", "running"})


@router.get("/jobs/{job_id}", response_class=HTMLResponse)
def job_detail(job_id: str) -> str:
    job = core.get_job(job_id)
    if not job:
        return layout("Job not found", f"<section class='card'><h1>Job not found</h1><p class='muted'><code>{h(job_id)}</code></p></section>")
    tasks = core.list_tasks(job_id=job_id, limit=1000)
    events = core.list_events(entity_type="job", entity_id=job_id, limit=50)
    cancel_button = ""
    if job["status"] not in {"succeeded", "failed", "cancelled", "cancelling"}:
        cancel_button = f"""
        <form method='post' action='/ui/jobs/{path_id(job_id)}/cancel?mode=graceful' onsubmit="return confirm('Request graceful cancellation? Queued tasks cancel now; running tasks finish.');">
          <button class='danger' type='submit'>Graceful Cancel</button>
        </form>
        <form method='post' action='/ui/jobs/{path_id(job_id)}/cancel?mode=force' onsubmit="return confirm('Force cancel queued and running tasks?');">
          <button class='secondary' type='submit'>Force Cancel</button>
        </form>
        """
    retry_button = ""
    if int(job.get("failed_tasks") or 0) > 0 and job["status"] not in {"cancelled", "cancelling"}:
        retry_button = f"""
        <form method='post' action='/ui/jobs/{path_id(job_id)}/retry-failed'>
          <button type='submit'>Retry Failed</button>
        </form>
        """
    body = f"""
    <div class='topline'>
      <div><h1>{h(job['name'])}</h1><div class='muted'><code>{h(job['id'])}</code> · {h(job['task_type'])} · session <a href='/ui/sessions/{path_id(job.get('session_id') or '')}'><code>{short_id(job.get('session_id') or '')}</code></a></div></div>
      <div class='actions'><a class='button secondary' href='/ui/jobs'>Back</a><a class='button secondary' href='/ui/jobs/{path_id(job_id)}/results'>Results</a><a class='button secondary' href='/jobs/{path_id(job_id)}/results'>Raw JSON</a><a class='button secondary' href='/jobs/{path_id(job_id)}/results/export?format=json'>Download JSON</a><a class='button secondary' href='/jobs/{path_id(job_id)}/results/export?format=csv'>Download CSV</a>{'<form method="post" action="/ui/jobs/' + path_id(job_id) + '/resume"><button type="submit">Resume</button></form>' if int(job.get('paused') or 0) and job['status'] not in {'succeeded','failed','cancelled'} else '<form method="post" action="/ui/jobs/' + path_id(job_id) + '/pause"><button class="secondary" type="submit">Pause</button></form>' if job['status'] not in {'succeeded','failed','cancelled','cancelling'} else ''}{retry_button}{cancel_button}</div>
    </div>
    <div class='grid stats'>
      <div class='card stat'><div class='label'>Status</div><div class='value' style='font-size:20px'>{status_pill(job['status'])}{' ' + status_pill('paused') if int(job.get('paused') or 0) else ''}</div><div class='tiny'>{h(job.get('pause_reason') or '')}</div></div>
      <div class='card stat'><div class='label'>Progress</div>{progress_bar(int(job['completed_tasks']), int(job['total_tasks']))}</div>
      <div class='card stat'><div class='label'>Failed</div><div class='value'>{h(job['failed_tasks'])}</div></div>
      <div class='card stat'><div class='label'>Priority</div><div class='value'>{h(job['priority'])}</div></div>
    </div>
    <div class='grid two' style='margin-top:16px'>
      <section class='card'><h2>Tasks</h2>{task_table(tasks)}</section>
      <section class='card'><h2>Job Metadata</h2><pre>{pretty_json(job.get('metadata', {}))}</pre><h2 style='margin-top:18px'>Job Events</h2>{event_table(events)}</section>
    </div>
    """
    return layout(f"Job {job_id}", body, refresh=job["status"] in {"queued", "running", "cancelling"})




@router.get("/jobs/{job_id}/results", response_class=HTMLResponse)
def job_results_page(job_id: str) -> str:
    results = core.get_job_results(job_id)
    if not results:
        return layout("Job results not found", f"<section class='card'><h1>Job not found</h1><p class='muted'><code>{h(job_id)}</code></p></section>")
    body = f"""
    <div class='topline'>
      <div><h1>Job Results</h1><div class='muted'><code>{h(results['job_id'])}</code> · {h(results['name'])} · session <a href='/ui/sessions/{path_id(results.get('session_id') or '')}'><code>{short_id(results.get('session_id') or '')}</code></a></div></div>
      <div class='actions'><a class='button secondary' href='/ui/jobs/{path_id(job_id)}'>Back to Job</a><a class='button secondary' href='/jobs/{path_id(job_id)}/results'>Raw JSON</a><a class='button secondary' href='/jobs/{path_id(job_id)}/results/export?format=json'>Download JSON</a><a class='button secondary' href='/jobs/{path_id(job_id)}/results/export?format=csv'>Download CSV</a><a class='button secondary' href='/jobs/{path_id(job_id)}/results/export?format=csv&failed_only=true'>Failed CSV</a></div>
    </div>
    <div class='grid stats'>
      <div class='card stat'><div class='label'>Status</div><div class='value' style='font-size:20px'>{status_pill(results['status'])}</div></div>
      <div class='card stat'><div class='label'>Progress</div>{progress_bar(int(results.get('completed_tasks') or 0), int(results.get('total_tasks') or 0))}</div>
      <div class='card stat'><div class='label'>Failed</div><div class='value'>{h(results.get('failed_tasks', 0))}</div></div>
      <div class='card stat'><div class='label'>Finished</div><div class='value' style='font-size:16px'>{h(results.get('finished_at') or '—')}</div></div>
    </div>
    <section class='card' style='margin-top:16px'>
      <h2>Task Results</h2>
      <p class='muted'>This is what the client reads from <code>GET /jobs/{h(job_id)}/results</code>. Workers return one result object per task to the broker.</p>
      {result_table(results.get('tasks', []))}
    </section>
    """
    return layout(f"Job results {job_id}", body, refresh=results["status"] in {"queued", "running", "cancelling"})

@router.post("/jobs/{job_id}/cancel")
def cancel_job_from_ui(job_id: str, mode: str = "graceful") -> RedirectResponse:
    core.cancel_job(job_id, mode=mode)
    return RedirectResponse(url=f"/ui/jobs/{path_id(job_id)}", status_code=303)


@router.post("/jobs/{job_id}/pause")
async def pause_job_from_ui(job_id: str, request: Request) -> RedirectResponse:
    form = await request.form()
    core.set_job_paused(job_id, True, reason=str(form.get("reason") or "").strip() or None, updated_by="ui")
    return RedirectResponse(url=f"/ui/jobs/{path_id(job_id)}", status_code=303)


@router.post("/jobs/{job_id}/resume")
def resume_job_from_ui(job_id: str) -> RedirectResponse:
    core.set_job_paused(job_id, False, updated_by="ui")
    return RedirectResponse(url=f"/ui/jobs/{path_id(job_id)}", status_code=303)


@router.post("/jobs/{job_id}/retry-failed")
def retry_failed_from_ui(job_id: str) -> RedirectResponse:
    core.retry_failed_tasks(job_id)
    return RedirectResponse(url=f"/ui/jobs/{path_id(job_id)}", status_code=303)


@router.get("/tasks/{task_id}", response_class=HTMLResponse)
def task_detail(task_id: str) -> str:
    task = core.get_task(task_id)
    if not task:
        return layout("Task not found", f"<section class='card'><h1>Task not found</h1><p class='muted'><code>{h(task_id)}</code></p></section>")
    events = core.list_events(entity_type="task", entity_id=task_id, limit=80)
    retry_button = ""
    if task["status"] in {"failed", "cancelled"}:
        retry_button = f"""
        <form method='post' action='/ui/tasks/{path_id(task_id)}/retry'>
          <button type='submit'>Retry Task</button>
        </form>
        """
    body = f"""
    <div class='topline'>
      <div><h1>Task</h1><div class='muted'><code>{h(task['id'])}</code> · job <a href='/ui/jobs/{path_id(task['job_id'])}'><code>{h(task['job_id'])}</code></a></div></div>
      <div class='actions'><a class='button secondary' href='/ui/jobs/{path_id(task['job_id'])}'>Back to Job</a>{retry_button}</div>
    </div>
    <div class='grid stats'>
      <div class='card stat'><div class='label'>Status</div><div class='value' style='font-size:20px'>{status_pill(task['status'])}</div></div>
      <div class='card stat'><div class='label'>Input</div><div class='value' style='font-size:16px'>#{h(task.get('input_index') if task.get('input_index') is not None else '—')}<div class='tiny'>{h(task.get('input_key') or '—')}</div></div></div>
      <div class='card stat'><div class='label'>Attempts</div><div class='value'>{h(task['attempts'])}/{1 + int(task['max_retries'])}</div></div>
      <div class='card stat'><div class='label'>Worker</div><div class='value' style='font-size:16px'>{h(task['assigned_worker_id'])}</div></div>
      <div class='card stat'><div class='label'>Updated</div><div class='value' style='font-size:16px'>{h(task['updated_at'])}</div></div>
    </div>
    <div class='grid two' style='margin-top:16px'>
      <section class='card'><h2>Payload</h2><pre>{pretty_json(task.get('payload', {}))}</pre><h2 style='margin-top:18px'>Result</h2><pre>{pretty_json(task.get('result'))}</pre></section>
      <section class='card'><h2>Error</h2><pre>{h(task.get('error') or '')}</pre><h2 style='margin-top:18px'>Events</h2>{event_table(events)}</section>
    </div>
    """
    return layout(f"Task {task_id}", body, refresh=task["status"] == "running")


@router.post("/tasks/{task_id}/retry")
def retry_task_from_ui(task_id: str) -> RedirectResponse:
    task = core.retry_task(task_id)
    job_id = task.get("job_id") if task else None
    target = f"/ui/jobs/{path_id(job_id)}" if job_id else "/ui/jobs"
    return RedirectResponse(url=target, status_code=303)


@router.get("/executors", response_class=HTMLResponse)
def executors_page() -> str:
    summary = core.list_executors(limit=2000)
    body = f"""
    <div class='topline'>
      <div><h1>Executors</h1><div class='muted'>Manager-wide live worker instance dashboard. Shows every known execution slot, not just tasks from one session.</div></div>
      <div class='actions'><a class='button secondary' href='/executors'>Raw JSON</a><a class='button secondary' href='/ui/workers'>Workers</a><a class='button secondary' href='/ui/services'>Services</a></div>
    </div>
    <div class='grid stats'>
      <div class='card stat'><div class='label'>Instances</div><div class='value'>{h(summary.get('instance_count', 0))}</div><div class='tiny'>workers {h(summary.get('worker_count', 0))}</div></div>
      <div class='card stat'><div class='label'>Running</div><div class='value'>{h(summary.get('running_instances', 0))}</div><div class='tiny'>running tasks {h(summary.get('running_tasks', 0))}</div></div>
      <div class='card stat'><div class='label'>Idle</div><div class='value'>{h(summary.get('idle_instances', 0))}</div><div class='tiny'>available slots</div></div>
      <div class='card stat'><div class='label'>Drained / Disabled / Stale</div><div class='value'>{h(summary.get('drained_instances', 0))}/{h(summary.get('disabled_instances', 0))}/{h(summary.get('stale_instances', 0))}</div><div class='tiny'>operator states</div></div>
    </div>
    <section class='card' style='margin-top:16px'>
      <h2>Live Instance Slots</h2>
      <p class='muted'>This page is global across the manager. Use session assignment drilldown for a session-scoped task view; use this page to see each current executor instance and the task/session/job currently occupying it.</p>
      {executor_slot_table(summary)}
    </section>
    """
    return layout("Executors", body, refresh=True)


@router.get("/executors/{worker_id}/instances/{instance_id}", response_class=HTMLResponse)
def executor_detail_page(worker_id: str, instance_id: str, recent_limit: int = 100) -> str:
    detail = core.get_executor(worker_id, instance_id, recent_limit=recent_limit)
    if not detail:
        return layout("Executor not found", f"<section class='card'><h1>Executor not found</h1><p class='muted'><code>{h(worker_id)}</code> / <code>{h(instance_id)}</code></p></section>")
    instance = detail.get("instance") or {}
    worker = detail.get("worker") or {}
    current = detail.get("current_task") or {}
    current_html = "<p class='muted'>No task is currently running on this instance.</p>"
    if current:
        current_html = f"""
        <table><thead><tr><th>Task</th><th>Session</th><th>Job</th><th>Type</th><th>Index</th><th>Attempt</th><th>Runtime</th><th>Lease</th></tr></thead>
          <tbody><tr>
            <td><a href='/ui/tasks/{path_id(current.get('task_id'))}'><code>{short_id(current.get('task_id'))}</code></a></td>
            <td><a href='/ui/sessions/{path_id(current.get('session_id'))}'><code>{short_id(current.get('session_id'))}</code></a><div class='tiny'>{h(current.get('session_name') or '')}</div></td>
            <td><a href='/ui/jobs/{path_id(current.get('job_id'))}'><code>{short_id(current.get('job_id'))}</code></a><div class='tiny'>{h(current.get('job_name') or '')}</div></td>
            <td>{h(current.get('task_type') or '')}</td>
            <td>{h(current.get('input_index') if current.get('input_index') is not None else '—')}<div class='tiny'>{h(current.get('input_key') or '—')}</div></td>
            <td>{h(current.get('attempts'))}/{1 + int(current.get('max_retries') or 0)}</td>
            <td>{seconds_label(current.get('runtime_seconds'))}</td>
            <td class='nowrap'>{h(current.get('lease_expires_at') or '—')}</td>
          </tr></tbody>
        </table>
        """
    log_path = instance.get('log_path') or f"instances/{instance_id}/worker.log"
    body = f"""
    <div class='topline'>
      <div><h1>Executor Instance</h1><div class='muted'><code>{h(worker_id)}</code> / <code>{h(instance_id)}</code></div></div>
      <div class='actions'><a class='button secondary' href='/ui/executors'>Back to Executors</a><a class='button secondary' href='/ui/workers/{path_id(worker_id)}'>Worker</a><a class='button secondary' href='/ui/workers/{path_id(worker_id)}/logs/{path_id(log_path)}'>Instance Log</a><a class='button secondary' href='/executors/{path_id(worker_id)}/instances/{path_id(instance_id)}'>Raw JSON</a></div>
    </div>
    <div class='grid stats'>
      <div class='card stat'><div class='label'>State</div><div class='value' style='font-size:20px'>{status_pill(instance.get('state'))}</div><div class='tiny'>worker {h(instance.get('worker_state') or worker.get('state') or '—')}</div></div>
      <div class='card stat'><div class='label'>Service</div><div class='value' style='font-size:16px'>{h(worker.get('service_name') or instance.get('service_name') or '—')}</div><div class='tiny'>{h(worker.get('service_version') or instance.get('service_version') or '—')}</div></div>
      <div class='card stat'><div class='label'>Completed By Instance</div><div class='value'>{h(instance.get('tasks_completed', 0))}</div><div class='tiny'>advertised heartbeat counter</div></div>
      <div class='card stat'><div class='label'>Last Poll</div><div class='value' style='font-size:16px'>{h(instance.get('last_poll_at') or '—')}</div><div class='tiny'>heartbeat {h(worker.get('last_heartbeat_at') or '—')}</div></div>
    </div>
    <section class='card' style='margin-top:16px'>
      <h2>Current Assignment</h2>
      {current_html}
    </section>
    <section class='card' style='margin-top:16px'>
      <h2>Instance Details</h2>
      <table><tbody>
        <tr><th>Executor ID</th><td><code>{h(instance.get('executor_id'))}</code></td></tr>
        <tr><th>Worker Host</th><td>{h(worker.get('hostname') or instance.get('hostname') or '')}</td></tr>
        <tr><th>Advertised Status</th><td>{h(instance.get('status') or '—')}</td></tr>
        <tr><th>Draining</th><td>{h(instance.get('instance_draining'))} {h(instance.get('drain_reason') or '')}</td></tr>
        <tr><th>Last Error</th><td>{h(instance.get('last_error') or '—')}</td></tr>
        <tr><th>Log Path</th><td><code>{h(log_path)}</code></td></tr>
      </tbody></table>
    </section>
    <section class='card' style='margin-top:16px'>
      <h2>Recent Tasks On This Instance</h2>
      {executor_recent_task_table(detail.get('recent_tasks') or [])}
    </section>
    """
    return layout(f"Executor {worker_id}/{instance_id}", body, refresh=True)


@router.get("/queue", response_class=HTMLResponse)
def queue_page(limit: int = 1000) -> str:
    diagnostics = core.get_queue_diagnostics(limit=limit)
    body = f"""
    <div class='topline'>
      <div><h1>Queue Diagnostics</h1><div class='muted'>Explains queued tasks and why they are or are not leasing right now. Checked {h(diagnostics.get('checked_at'))}.</div></div>
      <div class='actions'><a class='button secondary' href='/queue/diagnostics'>Raw JSON</a><a class='button secondary' href='/ui/task-catalog'>Task Catalog</a><a class='button secondary' href='/ui/executors'>Executors</a></div>
    </div>
    <div class='grid stats'>
      <div class='card stat'><div class='label'>Queued Tasks</div><div class='value'>{h(diagnostics.get('queued_tasks', 0))}</div></div>
      <div class='card stat'><div class='label'>Leaseable Now</div><div class='value'>{h(diagnostics.get('leaseable_now', 0))}</div><div class='tiny'>eligible when workers poll</div></div>
      <div class='card stat'><div class='label'>Blocked</div><div class='value'>{h(diagnostics.get('blocked_tasks', 0))}</div></div>
      <div class='card stat'><div class='label'>Estimated Free Slots</div><div class='value'>{h(diagnostics.get('estimated_free_slots', 0))}</div><div class='tiny'>active workers {h(diagnostics.get('workers_active', 0))}/{h(diagnostics.get('workers_total', 0))}</div></div>
    </div>
    <div class='grid two' style='margin-top:16px'>
      <section class='card'><h2>Why Queued?</h2>{queue_reason_table(diagnostics)}</section>
      <section class='card'><h2>By Task Type</h2>{queue_group_table(diagnostics.get('task_types') or [], 'task type')}</section>
    </div>
    <div class='grid two' style='margin-top:16px'>
      <section class='card'><h2>By Session</h2>{queue_group_table(diagnostics.get('sessions') or [], 'session')}</section>
      <section class='card'><h2>By Job</h2>{queue_group_table(diagnostics.get('jobs') or [], 'job')}</section>
    </div>
    <section class='card' style='margin-top:16px'><h2>Queued Task Detail</h2>{queue_task_table(diagnostics)}</section>
    <section class='card' style='margin-top:16px'><h2>Worker Capacity Snapshot</h2>{queue_worker_table(diagnostics)}</section>
    """
    return layout("Queue Diagnostics", body, refresh=True)


@router.get("/task-catalog", response_class=HTMLResponse)
def task_catalog_page(task_type: str | None = None, required_tags: str | None = None) -> str:
    tags = [tag.strip() for tag in (required_tags or "").split(",") if tag.strip()]
    catalog = core.get_task_catalog(task_type=task_type, required_tags=tags)
    type_rows = ""
    for item in catalog.get("task_types", []):
        workers = item.get("workers", [])
        worker_links = " ".join(
            f"<a href='/ui/workers'><code>{h(w.get('id'))}</code></a>" for w in workers[:6]
        )
        if len(workers) > 6:
            worker_links += f" <span class='tiny'>+{len(workers) - 6} more</span>"
        type_rows += f"""
        <tr>
          <td><code>{h(item.get('task_type'))}</code></td>
          <td>{h(item.get('active_workers', 0))}/{h(item.get('total_workers', 0))}</td>
          <td>{''.join(f"<span class='pill'>{h(tag)}</span> " for tag in item.get('tags', [])) or '<span class="tiny">none</span>'}</td>
          <td>{worker_links or '<span class="tiny">none</span>'}</td>
        </tr>
        """
    if not type_rows:
        type_rows = empty_row(4, "No workers have advertised task types yet.")

    capability_html = ""
    if task_type:
        warnings = catalog.get("warnings", [])
        capable = catalog.get("capable_workers", [])
        warning_html = "".join(f"<div class='alert error'>{h(w)}</div>" for w in warnings)
        capable_rows = "".join(
            f"""
            <tr>
              <td><code>{h(w.get('id'))}</code></td>
              <td>{status_pill(w.get('status'))}</td>
              <td>{''.join(f"<span class='pill'>{h(tag)}</span> " for tag in w.get('tags', []))}</td>
              <td>{h(w.get('service_name'))} {h(w.get('service_version'))}</td>
              <td>{h(w.get('last_heartbeat_at'))}</td>
            </tr>
            """
            for w in capable
        ) or empty_row(5, "No active capable worker matched that task type/tag filter.")
        capability_html = f"""
        <section class='card' style='margin-top:16px'>
          <h2>Capability Check</h2>
          {warning_html or '<div class="alert success">At least one active worker can run this task.</div>'}
          <table><thead><tr><th>Worker</th><th>Status</th><th>Tags</th><th>Service</th><th>Heartbeat</th></tr></thead><tbody>{capable_rows}</tbody></table>
        </section>
        """

    body = f"""
    <div class='topline'>
      <div><h1>Task Catalog</h1><div class='muted'>Task types and capabilities advertised by live worker nodes.</div></div>
      <div class='actions'><a class='button secondary' href='/ui/workers'>Workers</a><a class='button secondary' href='/task-catalog'>Raw API</a></div>
    </div>
    <section class='card'>
      <form class='form-row' method='get' action='/ui/task-catalog'>
        <label>Check task type<input name='task_type' placeholder='square' value='{h(task_type or '')}'></label>
        <label>Required tags<input name='required_tags' placeholder='gpu,risk' value='{h(required_tags or '')}'></label>
        <label>&nbsp;<button type='submit'>Check</button></label>
      </form>
    </section>
    {capability_html}
    <section class='card' style='margin-top:16px'>
      <h2>Advertised Task Types</h2>
      <table><thead><tr><th>Task Type</th><th>Active/Total Workers</th><th>Tags Seen</th><th>Workers</th></tr></thead><tbody>{type_rows}</tbody></table>
    </section>
    """
    return layout("Task Catalog", body, refresh=True)


@router.get("/services", response_class=HTMLResponse)
def services_page(service_name: str | None = None, service_version: str | None = None) -> str:
    registry = core.get_services(service_name=service_name, service_version=service_version)
    services = registry.get("services", [])
    if not services:
        rows = empty_row(9, "No worker service metadata has been advertised yet.")
    else:
        row_parts = []
        for item in services:
            task_types = "".join(f"<span class='pill'>{h(task_type)}</span> " for task_type in item.get("task_types", [])) or '<span class="tiny">none</span>'
            tags = "".join(f"<span class='pill'>{h(tag)}</span> " for tag in item.get("tags", [])) or '<span class="tiny">none</span>'
            workers = item.get("workers", [])
            worker_links = " ".join(f"<a href='/ui/workers'><code>{h(worker.get('id'))}</code></a>" for worker in workers[:6])
            if len(workers) > 6:
                worker_links += f" <span class='tiny'>+{len(workers) - 6} more</span>"
            taskgrid_versions = ", ".join(str(v) for v in item.get("taskgrid_versions", [])) or "—"
            row_parts.append(f"""
            <tr>
              <td><strong>{h(item.get('service_name'))}</strong><div class='tiny'>version {h(item.get('service_version'))}</div></td>
              <td>{h(item.get('workers_active', 0))}/{h(item.get('workers_total', 0))}<div class='tiny'>stale {h(item.get('workers_stale', 0))} · disabled {h(item.get('workers_disabled', 0))} · draining {h(item.get('workers_draining', 0))}</div></td>
              <td>{h(item.get('running_tasks', 0))}/{h(item.get('active_instances', 0))}<div class='tiny'>desired {h(item.get('desired_instances', 0))}</div></td>
              <td>{task_types}</td>
              <td>{tags}</td>
              <td>{h(taskgrid_versions)}</td>
              <td>{worker_links or '<span class="tiny">none</span>'}</td>
              <td><a class='button secondary' href='/services?service_name={path_id(item.get('service_name'))}&service_version={path_id(item.get('service_version'))}'>Raw</a></td>
            </tr>
            """)
        rows = "".join(row_parts)

    body = f"""
    <div class='topline'>
      <div><h1>Services</h1><div class='muted'>Runtime service/application versions advertised by worker heartbeats. Useful for spotting mixed Docker image versions and stale workers.</div></div>
      <div class='actions'><a class='button secondary' href='/ui/workers'>Workers</a><a class='button secondary' href='/ui/task-catalog'>Task Catalog</a><a class='button secondary' href='/services'>Raw API</a></div>
    </div>
    <section class='card'>
      <form class='form-row' method='get' action='/ui/services'>
        <label>Service name<input name='service_name' placeholder='risk-engine' value='{h(service_name or '')}'></label>
        <label>Version<input name='service_version' placeholder='1.2.3' value='{h(service_version or '')}'></label>
        <label>&nbsp;<button type='submit'>Filter</button></label>
      </form>
    </section>
    <section class='card' style='margin-top:16px'>
      <h2>Advertised Services</h2>
      <table><thead><tr><th>Service</th><th>Workers Active/Total</th><th>Busy/Instances</th><th>Task Types</th><th>Tags</th><th>TaskGrid Versions</th><th>Workers</th><th></th></tr></thead><tbody>{rows}</tbody></table>
    </section>
    """
    return layout("Services", body, refresh=True)


@router.get("/workers", response_class=HTMLResponse)
def workers_page() -> str:
    workers = core.list_workers(limit=250)
    if not workers:
        rows = empty_row(14, "No workers have checked in yet. Start one with: python -m taskgrid.worker --module examples.custom_tasks --instances 4")
    else:
        row_parts = []
        for w in workers:
            disabled = bool(w.get("disabled"))
            draining = bool(w.get("draining"))
            effective_status = "disabled" if disabled else ("draining" if draining else (w["status"] if w.get("active") else "stale"))
            state_action = "enable" if disabled else "disable"
            state_label = "Enable" if disabled else "Disable"
            state_class = "secondary" if disabled else "danger"
            drain_action = "undrain" if draining else "drain"
            drain_label = "Clear Drain" if draining else "Drain"
            drain_class = "secondary" if draining else "warning"
            disabled_reason_text = f"<div class='tiny'>disabled reason: {h(w.get('disabled_reason'))}</div>" if disabled and w.get("disabled_reason") else ""
            drain_reason_text = f"<div class='tiny'>drain reason: {h(w.get('drain_reason'))}</div>" if draining and w.get("drain_reason") else ""
            drained_instances = w.get('drained_instances') or []
            instance_drain_text = f"<div class='tiny'>drained instances: {h(', '.join(drained_instances))}</div>" if drained_instances else ""
            reason_text = disabled_reason_text + drain_reason_text + instance_drain_text
            row_parts.append(f"""
            <tr>
              <td><input form='bulk-worker-state' type='checkbox' name='worker_ids' value='{h(w['id'])}'></td>
              <td><a href='/ui/workers/{path_id(w['id'])}'><code>{h(w['id'])}</code></a></td>
              <td>{h(w['hostname'])}</td>
              <td>{status_pill(effective_status)}<div class='tiny'>active {h(w.get('active'))}</div>{reason_text}</td>
              <td>{''.join(f"<span class='pill'>{h(t)}</span> " for t in (w.get('task_types') or (w.get('metadata') or {}).get('task_types') or [])) or '<span class="tiny">none advertised</span>'}</td>
              <td>{h(w.get('running_tasks', 0))}/{h(w.get('active_concurrency', w.get('desired_concurrency', 1)))}</td>
              <td>{h(w.get('desired_concurrency', 1))}</td>
              <td>{h(w.get('pending_concurrency') or '')}</td>
              <td>{h(w['current_task_id'])}</td>
              <td>{h(w['version'])}</td>
              <td>{worker_logs_button(w)}</td>
              <td class='nowrap'>{h(w['last_heartbeat_at'])}</td>
              <td>
                <form class='inline-form' method='post' action='/ui/workers/{path_id(w['id'])}/config'>
                  <input name='desired_concurrency' type='number' min='1' max='{core.MAX_WORKER_CONCURRENCY}' value='{h(w.get('desired_concurrency', 1))}' style='width:84px'>
                  <button type='submit'>Apply</button>
                </form>
                <details style='margin-top:8px'><summary class='tiny'>metadata</summary><pre style='margin:8px 0 0; max-height:120px'>{pretty_json(w.get('metadata', {}))}</pre></details>
              </td>
              <td>
                <form class='inline-form' method='post' action='/ui/workers/{path_id(w['id'])}/{state_action}' onsubmit="return confirm('{state_label} worker {h(w['id'])}?');">
                  <input name='reason' placeholder='reason' style='width:120px'>
                  <button class='{state_class}' type='submit'>{state_label}</button>
                </form>
                <form class='inline-form' method='post' action='/ui/workers/{path_id(w['id'])}/{drain_action}' onsubmit="return confirm('{drain_label} worker {h(w['id'])}?');" style='margin-top:6px'>
                  <input name='reason' placeholder='reason' style='width:120px'>
                  <button class='{drain_class}' type='submit'>{drain_label}</button>
                </form>
                <details style='margin-top:6px'><summary class='tiny'>instance drain</summary>
                  <form class='inline-form' method='post' action='/ui/workers/{path_id(w['id'])}/instances/drain' style='margin-top:6px'>
                    <input name='instance_id' placeholder='instance-001' style='width:120px'>
                    <input name='reason' placeholder='reason' style='width:120px'>
                    <button class='warning' type='submit'>Drain Instance</button>
                  </form>
                  <form class='inline-form' method='post' action='/ui/workers/{path_id(w['id'])}/instances/undrain' style='margin-top:6px'>
                    <input name='instance_id' placeholder='instance-001' style='width:120px'>
                    <button class='secondary' type='submit'>Clear Instance</button>
                  </form>
                </details>
              </td>
            </tr>
            """)
        rows = "".join(row_parts)
    stale_count = sum(1 for worker in workers if not worker.get('active'))
    disabled_count = sum(1 for worker in workers if worker.get('disabled'))
    purge_disabled = "disabled" if stale_count == 0 else ""
    body = f"""
    <div class='topline'>
      <div><h1>Workers</h1><div class='muted'>Worker nodes poll the broker, run single-task instances, heartbeat while running, and resize or disable nodes from this page.</div></div>
      <div class='actions'>
        <form class='inline-form' method='post' action='/ui/workers/purge-offline' onsubmit="return confirm('Purge stale/offline worker records from the manager registry? Running task assignments are skipped.');">
          <button class='secondary' type='submit' {purge_disabled}>Purge Offline Workers ({h(stale_count)})</button>
        </form>
      </div>
    </div>
    <section class='card'>
      <p class='muted'>Changing desired instances does not kill running tasks. Drain blocks new leases while running work finishes and instances remain configured; disable also tells heartbeating nodes to scale local instance loops down to zero until re-enabled.</p>
      <p class='muted'>Offline workers persist in the manager registry for visibility. Purging removes stale worker/config rows only; manager events, task history, results, and remote worker logs are kept.</p>
      <form id='bulk-worker-state' class='inline-form' method='post' action='/ui/workers/bulk-state' onsubmit="return confirm('Apply this state change to selected workers?');" style='margin:0 0 12px'>
        <input name='reason' placeholder='optional reason for selected workers' style='min-width:260px'>
        <button class='danger' type='submit' name='action' value='disable'>Disable Selected</button>
        <button class='secondary' type='submit' name='action' value='enable'>Enable Selected</button>
        <button class='warning' type='submit' name='action' value='drain'>Drain Selected</button>
        <button class='secondary' type='submit' name='action' value='undrain'>Clear Drain</button>
        <span class='tiny'>Disabled workers: {h(disabled_count)}</span>
      </form>
      <table><thead><tr><th></th><th>ID</th><th>Host</th><th>Status</th><th>Task Types</th><th>Busy</th><th>Instances</th><th>Pending</th><th>Task</th><th>Version</th><th>Logs</th><th>Heartbeat</th><th>Config</th><th>State</th></tr></thead><tbody>{rows}</tbody></table>
    </section>
    """
    return layout("Workers", body, refresh=True)


@router.get("/workers/{worker_id}", response_class=HTMLResponse)
def worker_detail_page(worker_id: str) -> str:
    worker = core.get_worker(worker_id)
    if not worker:
        return layout("Worker not found", f"<section class='card'><h1>Worker not found</h1><p class='muted'><code>{h(worker_id)}</code></p></section>")
    summary = core.list_executors(limit=2000)
    worker_summary = next((item for item in summary.get("workers", []) if item.get("worker_id") == worker_id), None)
    instance_rows = ""
    if worker_summary:
        instance_rows = "".join(
            f"""
            <tr>
              <td><a href='/ui/executors/{path_id(worker_id)}/instances/{path_id(inst.get('instance_id'))}'><code>{h(inst.get('instance_id'))}</code></a></td>
              <td>{status_pill(inst.get('state'))}</td>
              <td>{_executor_current_task_cell(inst)}</td>
              <td>{_executor_session_cell(inst)}</td>
              <td>{_executor_job_cell(inst)}</td>
              <td>{h(inst.get('tasks_completed', 0))}</td>
              <td>{h(inst.get('last_poll_at') or '—')}</td>
              <td><a class='button secondary' href='/ui/workers/{path_id(worker_id)}/logs/{path_id(inst.get('log_path') or '')}'>Log</a></td>
            </tr>
            """
            for inst in worker_summary.get("instances", [])
        )
    if not instance_rows:
        instance_rows = empty_row(8, "No instances have been advertised for this worker yet.")
    body = f"""
    <div class='topline'>
      <div><h1>Worker</h1><div class='muted'><code>{h(worker_id)}</code> · {h(worker.get('hostname') or '')}</div></div>
      <div class='actions'><a class='button secondary' href='/ui/workers'>Back to Workers</a><a class='button secondary' href='/ui/executors'>Executors</a>{worker_logs_button(worker)}</div>
    </div>
    <div class='grid stats'>
      <div class='card stat'><div class='label'>Status</div><div class='value' style='font-size:20px'>{status_pill('disabled' if worker.get('disabled') else ('draining' if worker.get('draining') else (worker.get('status') if worker.get('active') else 'stale')))}</div></div>
      <div class='card stat'><div class='label'>Instances</div><div class='value'>{h(worker.get('active_concurrency', 0))}/{h(worker.get('desired_concurrency', 0))}</div><div class='tiny'>running {h(worker.get('running_tasks', 0))}</div></div>
      <div class='card stat'><div class='label'>Service</div><div class='value' style='font-size:16px'>{h(worker.get('service_name') or '—')}</div><div class='tiny'>{h(worker.get('service_version') or '—')}</div></div>
      <div class='card stat'><div class='label'>Heartbeat</div><div class='value' style='font-size:16px'>{h(worker.get('last_heartbeat_at') or '—')}</div></div>
    </div>
    <section class='card' style='margin-top:16px'>
      <h2>Instances</h2>
      <table><thead><tr><th>Instance</th><th>State</th><th>Current Task</th><th>Session</th><th>Job</th><th>Done</th><th>Last Poll</th><th>Log</th></tr></thead><tbody>{instance_rows}</tbody></table>
    </section>
    <section class='card' style='margin-top:16px'>
      <h2>Worker Metadata</h2>
      <pre>{pretty_json(worker.get('metadata') or {})}</pre>
    </section>
    """
    return layout(f"Worker {worker_id}", body, refresh=True)


@router.get("/workers/{worker_id}/logs", response_class=HTMLResponse)
def worker_logs_page(worker_id: str) -> str:
    worker = core.get_worker(worker_id)
    if not worker:
        return layout("Worker not found", f"<section class='card'><h1>Worker not found</h1><p class='muted'><code>{h(worker_id)}</code></p></section>")
    try:
        logs = core.list_worker_logs(worker_id)
        files = logs.get("files", [])
        if not files:
            rows = empty_row(4, "No log files found yet.")
        else:
            rows = "".join(
                f"""
                <tr>
                  <td><a href='/ui/workers/{path_id(worker_id)}/logs/{path_id(item['name'])}'><code>{h(item['name'])}</code></a></td>
                  <td>{h(item.get('size_bytes'))}</td>
                  <td class='nowrap'>{h(item.get('modified_at'))}</td>
                  <td><a class='button secondary' href='/ui/workers/{path_id(worker_id)}/logs/{path_id(item['name'])}'>Open</a></td>
                </tr>
                """
                for item in files
            )
        error_html = ""
    except core.WorkerLogError as exc:
        rows = empty_row(4, "Logs are not available from this worker.")
        error_html = f"<div class='alert error'>{h(exc)}</div>"
        logs = {"log_dir": (worker.get("metadata") or {}).get("log_dir", "")}

    body = f"""
    <div class='topline'>
      <div><h1>Worker Logs</h1><div class='muted'><code>{h(worker_id)}</code> · {h(worker.get('hostname'))}</div></div>
      <div class='actions'><a class='button secondary' href='/ui/workers'>Back to Workers</a><a class='button secondary' href='/workers/{path_id(worker_id)}/logs'>Raw JSON</a></div>
    </div>
    <section class='card'>
      {error_html}
      <p class='muted'>Node log folder: <code>{h(logs.get('log_dir') or (worker.get('metadata') or {}).get('log_dir') or '')}</code></p>
      <table><thead><tr><th>Log</th><th>Size</th><th>Modified</th><th></th></tr></thead><tbody>{rows}</tbody></table>
    </section>
    """
    return layout(f"Worker logs {worker_id}", body, refresh=True)


@router.get("/workers/{worker_id}/logs/{filename:path}", response_class=HTMLResponse)
def worker_log_file_page(worker_id: str, filename: str, tail: int = core.MAX_WORKER_LOG_TAIL_BYTES) -> str:
    worker = core.get_worker(worker_id)
    if not worker:
        return layout("Worker not found", f"<section class='card'><h1>Worker not found</h1><p class='muted'><code>{h(worker_id)}</code></p></section>")
    try:
        text = core.read_worker_log(worker_id, filename, tail_bytes=tail)
        error_html = ""
    except core.WorkerLogError as exc:
        text = ""
        error_html = f"<div class='alert error'>{h(exc)}</div>"
    body = f"""
    <div class='topline'>
      <div><h1>{h(filename)}</h1><div class='muted'>Worker <code>{h(worker_id)}</code></div></div>
      <div class='actions'><a class='button secondary' href='/ui/workers/{path_id(worker_id)}/logs'>Back to Logs</a><a class='button secondary' href='/workers/{path_id(worker_id)}/logs/{path_id(filename)}'>Raw Text</a></div>
    </div>
    <section class='card'>
      {error_html}
      <pre>{h(text)}</pre>
    </section>
    """
    return layout(f"{filename} · {worker_id}", body, refresh=True)


@router.post("/workers/{worker_id}/config")
async def worker_config_from_ui(worker_id: str, request: Request) -> RedirectResponse:
    form = await request.form()
    try:
        desired = int(str(form.get("desired_concurrency") or "1"))
    except ValueError:
        desired = 1
    core.set_worker_config(worker_id, desired, updated_by="ui")
    return RedirectResponse(url="/ui/workers", status_code=303)


@router.post("/workers/{worker_id}/disable")
async def worker_disable_from_ui(worker_id: str, request: Request) -> RedirectResponse:
    form = await request.form()
    core.set_worker_enabled(worker_id, enabled=False, reason=str(form.get("reason") or ""), updated_by="ui")
    return RedirectResponse(url="/ui/workers", status_code=303)


@router.post("/workers/{worker_id}/enable")
async def worker_enable_from_ui(worker_id: str, request: Request) -> RedirectResponse:
    form = await request.form()
    core.set_worker_enabled(worker_id, enabled=True, reason=str(form.get("reason") or ""), updated_by="ui")
    return RedirectResponse(url="/ui/workers", status_code=303)


@router.post("/workers/{worker_id}/drain")
async def worker_drain_from_ui(worker_id: str, request: Request) -> RedirectResponse:
    form = await request.form()
    core.set_worker_draining(worker_id, draining=True, reason=str(form.get("reason") or ""), updated_by="ui")
    return RedirectResponse(url="/ui/workers", status_code=303)


@router.post("/workers/{worker_id}/undrain")
async def worker_undrain_from_ui(worker_id: str, request: Request) -> RedirectResponse:
    form = await request.form()
    core.set_worker_draining(worker_id, draining=False, reason=str(form.get("reason") or ""), updated_by="ui")
    return RedirectResponse(url="/ui/workers", status_code=303)


@router.post("/workers/{worker_id}/instances/drain")
async def worker_instance_drain_from_ui(worker_id: str, request: Request) -> RedirectResponse:
    form = await request.form()
    instance_id = str(form.get("instance_id") or "").strip()
    if instance_id:
        core.set_worker_instance_draining(worker_id, instance_id, draining=True, reason=str(form.get("reason") or ""), updated_by="ui")
    return RedirectResponse(url="/ui/workers", status_code=303)


@router.post("/workers/{worker_id}/instances/undrain")
async def worker_instance_undrain_from_ui(worker_id: str, request: Request) -> RedirectResponse:
    form = await request.form()
    instance_id = str(form.get("instance_id") or "").strip()
    if instance_id:
        core.set_worker_instance_draining(worker_id, instance_id, draining=False, updated_by="ui")
    return RedirectResponse(url="/ui/workers", status_code=303)


@router.post("/workers/bulk-state")
async def worker_bulk_state_from_ui(request: Request) -> RedirectResponse:
    form = await request.form()
    worker_ids = [str(value) for value in form.getlist("worker_ids") if str(value).strip()]
    action = str(form.get("action") or "disable").lower()
    reason = str(form.get("reason") or "")
    if worker_ids:
        if action in {"drain", "undrain"}:
            for worker_id in worker_ids:
                core.set_worker_draining(worker_id, draining=(action == "drain"), reason=reason, updated_by="ui")
        else:
            core.set_workers_enabled(worker_ids, enabled=(action == "enable"), reason=reason, updated_by="ui")
    return RedirectResponse(url="/ui/workers", status_code=303)



@router.post("/workers/purge-offline")
def purge_offline_workers_from_ui() -> RedirectResponse:
    core.purge_offline_workers(updated_by="ui")
    return RedirectResponse(url="/ui/workers", status_code=303)



@router.get("/manager/data-flow", response_class=HTMLResponse)
def manager_data_flow_page() -> str:
    events = core.list_events(limit=80, code=None)
    task_events = [event for event in events if str(event.get("code", "")).startswith("Task")][:40]
    body = f"""
    <div class='topline'>
      <div><h1>Data Flow</h1><div class='muted'>Manager-side view of how clients, broker, worker instances, and results interact.</div></div>
      <div class='actions'><a class='button secondary' href='/ui/manager/log'>Manager Log</a><a class='button secondary' href='/docs'>API Docs</a></div>
    </div>
    <section class='card'>
      <h2>Runtime path</h2>
      <table>
        <thead><tr><th>Step</th><th>Who calls</th><th>Endpoint</th><th>What moves</th><th>Manager event</th></tr></thead>
        <tbody>
          <tr><td>1</td><td>Client</td><td><code>POST /jobs</code></td><td>Job name, task type, JSON task payload array, priority/retries/metadata.</td><td><code>JobSubmitted</code></td></tr>
          <tr><td>2</td><td>Broker</td><td>SQLite state</td><td>Creates one service session, one job, and one queued task row per payload.</td><td><code>ServiceSessionCreated</code></td></tr>
          <tr><td>3</td><td>Worker instance</td><td><code>POST /tasks/lease</code></td><td>Leases exactly one queued task and receives its <code>payload</code> JSON.</td><td><code>TaskAccepted</code> <span class='tiny'>(debug)</span></td></tr>
          <tr><td>4</td><td>Engine</td><td>Local Python registry</td><td>Runs <code>run_task(task_type, payload)</code> inside a one-task process slot.</td><td>worker instance log</td></tr>
          <tr><td>5</td><td>Worker instance</td><td><code>POST /tasks/&lt;id&gt;/complete</code></td><td>Returns JSON-serializable result to the broker.</td><td><code>TaskCompleted</code> <span class='tiny'>(debug)</span></td></tr>
          <tr><td>6</td><td>Client/UI</td><td><code>GET /jobs/&lt;id&gt;/results</code> or <code>GET /sessions/&lt;id&gt;/results</code></td><td>Reads all task results/errors with payload/result sizes and timing.</td><td>read-only</td></tr>
        </tbody>
      </table>
    </section>
    <section class='card' style='margin-top:16px'>
      <h2>Recent Task Flow Events</h2>
      <p class='muted'>Normal mode keeps task state on task rows and suppresses high-volume successful accept/complete events. Set <code>TASKGRID_EVENT_MODE=debug</code> or <code>TASKGRID_VERBOSE_TASK_EVENTS=1</code> to record every successful task lifecycle event here.</p>
      {event_table(task_events)}
    </section>
    <section class='card' style='margin-top:16px'>
      <h2>Important model</h2>
      <p class='muted'>The broker does not send code to engines. It sends task data. Engines already have task code loaded through <code>--module</code>. The task payload and final result are JSON so the client can submit/read them through HTTP without direct access to worker machines.</p>
    </section>
    """
    return layout("Data Flow", body, refresh=True)

@router.get("/manager/recovery", response_class=HTMLResponse)
def manager_recovery_page() -> str:
    status = core.recovery_status()
    stale_workers = status.get("stale_workers", [])
    expired_tasks = status.get("expired_tasks", [])
    worker_rows = "".join(
        f"""
        <tr>
          <td><code>{h(worker.get('id'))}</code></td>
          <td>{h(worker.get('hostname'))}</td>
          <td>{status_pill(worker.get('status') if worker.get('active') else 'stale')}</td>
          <td>{h(worker.get('last_heartbeat_at'))}</td>
          <td>{''.join(f"<span class='pill'>{h(tag)}</span> " for tag in worker.get('tags', [])) or '<span class="tiny">none</span>'}</td>
        </tr>
        """
        for worker in stale_workers
    ) or empty_row(5, "No stale workers detected.")
    task_rows = "".join(
        f"""
        <tr>
          <td><a href='/ui/tasks/{path_id(task.get('id'))}'><code>{short_id(task.get('id'))}</code></a></td>
          <td><a href='/ui/jobs/{path_id(task.get('job_id'))}'><code>{short_id(task.get('job_id'))}</code></a></td>
          <td><code>{h(task.get('assigned_worker_id'))}</code></td>
          <td>{h(task.get('attempts'))}/{1 + int(task.get('max_retries') or 0)}</td>
          <td class='nowrap'>{h(task.get('lease_expires_at'))}</td>
        </tr>
        """
        for task in expired_tasks
    ) or empty_row(5, "No expired running task leases detected.")
    body = f"""
    <div class='topline'>
      <div><h1>Recovery</h1><div class='muted'>Manager-side health checks for stale workers, expired task leases, and ignored late results.</div></div>
      <div class='actions'><a class='button secondary' href='/maintenance/status'>Raw Status</a><a class='button secondary' href='/ui/manager/log'>Manager Log</a></div>
    </div>
    <div class='grid stats'>
      <div class='card stat'><div class='label'>Workers</div><div class='value'>{h(status.get('workers_total', 0))}</div><div class='tiny'>active {h(status.get('workers_active', 0))} · stale {h(status.get('workers_stale', 0))}</div></div>
      <div class='card stat'><div class='label'>Running Tasks</div><div class='value'>{h(status.get('running_tasks', 0))}</div><div class='tiny'>currently leased</div></div>
      <div class='card stat'><div class='label'>Expired Leases</div><div class='value'>{h(status.get('expired_running_tasks', 0))}</div><div class='tiny'>eligible for recovery</div></div>
      <div class='card stat'><div class='label'>Historical Recoveries</div><div class='value'>{h(status.get('lease_requeue_events', 0))}</div><div class='tiny'>failed by lease {h(status.get('lease_failed_events', 0))}</div></div>
    </div>
    <section class='card' style='margin-top:16px'>
      <h2>Reconcile Manager State</h2>
      <p class='muted'>Recomputes job/session counters and statuses from task rows, and recovers expired leases in the same safe pass. This is also run on manager startup.</p>
      <form method='post' action='/ui/manager/recovery/reconcile'><button type='submit'>Reconcile Manager State</button></form>
    </section>
    <section class='card' style='margin-top:16px'>
      <h2>Recover Expired Leases Only</h2>
      <p class='muted'>This requeues expired running tasks that still have retry attempts left, or fails them when retries are exhausted. Running tasks with valid leases are not touched.</p>
      <form method='post' action='/ui/manager/recovery/run'><button class='secondary' type='submit'>Recover Expired Leases</button></form>
    </section>
    <section class='card' style='margin-top:16px'>
      <h2>Purge Offline Workers</h2>
      <p class='muted'>Removes stale/offline worker rows and their config rows from the manager registry. Workers with running task assignments are skipped; run lease recovery first if needed.</p>
      <form method='post' action='/ui/manager/recovery/purge-offline-workers' onsubmit="return confirm('Purge offline worker records from the manager registry?');"><button class='secondary' type='submit'>Purge Offline Workers</button></form>
    </section>
    <div class='grid two' style='margin-top:16px'>
      <section class='card'><h2>Expired Running Tasks</h2><table><thead><tr><th>Task</th><th>Job</th><th>Worker Instance</th><th>Attempts</th><th>Lease Expires</th></tr></thead><tbody>{task_rows}</tbody></table></section>
      <section class='card'><h2>Stale Workers</h2><table><thead><tr><th>ID</th><th>Host</th><th>Status</th><th>Last Heartbeat</th><th>Tags</th></tr></thead><tbody>{worker_rows}</tbody></table></section>
    </div>
    <section class='card' style='margin-top:16px'>
      <h2>Recovery Events</h2>
      {event_table(core.list_events(code='MaintenanceReconcileRun', limit=20) + core.list_events(code='MaintenanceRecoveryRun', limit=20) + core.list_events(code='TaskLeaseExpiredRequeued', limit=20) + core.list_events(code='TaskLeaseExpiredFailed', limit=20) + core.list_events(code='TaskResultIgnored', limit=20) + core.list_events(code='WorkerPurgeOfflineRun', limit=20) + core.list_events(code='WorkerPurgedOffline', limit=20))}
    </section>
    """
    return layout("Recovery", body, refresh=True)


@router.post("/manager/recovery/reconcile")
def reconcile_from_ui() -> RedirectResponse:
    core.reconcile_manager_state(recover_expired=True, updated_by="ui")
    return RedirectResponse(url="/ui/manager/recovery", status_code=303)


@router.post("/manager/recovery/run")
def run_recovery_from_ui() -> RedirectResponse:
    core.recover_expired_leases(updated_by="ui")
    return RedirectResponse(url="/ui/manager/recovery", status_code=303)


@router.post("/manager/recovery/purge-offline-workers")
def purge_offline_workers_from_recovery_ui() -> RedirectResponse:
    core.purge_offline_workers(updated_by="ui")
    return RedirectResponse(url="/ui/manager/recovery", status_code=303)


@router.get("/manager/retention", response_class=HTMLResponse)
def manager_retention_page(completed_days: int = 30, failed_days: int = 90, event_days: int = 30, purge_workers_active_seconds: int | None = None) -> str:
    preview = core.retention_preview(
        completed_days=completed_days,
        failed_days=failed_days,
        event_days=event_days,
        purge_workers_active_seconds=purge_workers_active_seconds,
    )
    body = f"""
    <div class='topline'>
      <div><h1>Retention</h1><div class='muted'>Preview and apply cleanup for old terminal sessions, old manager events, and stale worker rows.</div></div>
      <div class='actions'><a class='button secondary' href='/ui/manager/recovery'>Recovery</a><a class='button secondary' href='/ui/manager/log'>Manager Log</a></div>
    </div>
    <section class='card'>
      <h2>Cleanup Preview</h2>
      <form class='form-row' method='get' action='/ui/manager/retention'>
        <label>Completed/cancelled session days<input type='number' min='0' max='3650' name='completed_days' value='{h(completed_days)}'></label>
        <label>Failed session days<input type='number' min='0' max='3650' name='failed_days' value='{h(failed_days)}'></label>
        <label>Event days<input type='number' min='0' max='3650' name='event_days' value='{h(event_days)}'></label>
        <label>Stale worker seconds<input type='number' min='1' max='86400' name='purge_workers_active_seconds' value='{h(purge_workers_active_seconds or "")}' placeholder='optional'></label>
        <label>&nbsp;<button type='submit'>Preview</button></label>
      </form>
      <div class='grid stats' style='margin-top:16px'>
        <div class='card stat'><div class='label'>Completed/cancelled sessions</div><div class='value'>{h(preview.get('completed_or_cancelled_sessions'))}</div></div>
        <div class='card stat'><div class='label'>Failed sessions</div><div class='value'>{h(preview.get('failed_sessions'))}</div></div>
        <div class='card stat'><div class='label'>Old events</div><div class='value'>{h(preview.get('old_events'))}</div></div>
        <div class='card stat'><div class='label'>Stale workers</div><div class='value'>{h(preview.get('stale_workers'))}</div></div>
      </div>
      <pre>{pretty_json(preview)}</pre>
      <form method='post' action='/ui/manager/retention/apply' class='actions'>
        <input type='hidden' name='completed_days' value='{h(completed_days)}'>
        <input type='hidden' name='failed_days' value='{h(failed_days)}'>
        <input type='hidden' name='event_days' value='{h(event_days)}'>
        <input type='hidden' name='purge_workers_active_seconds' value='{h(purge_workers_active_seconds or "") }'>
        <button class='danger' type='submit'>Apply Cleanup</button>
        <span class='tiny'>Deletes only terminal sessions/jobs/tasks older than the selected windows. Running/queued work is never removed.</span>
      </form>
    </section>
    """
    return layout("Retention", body)


@router.post("/manager/retention/apply")
async def apply_retention_from_ui(request: Request) -> RedirectResponse:
    form = await request.form()

    def int_or_none(name: str) -> int | None:
        raw = str(form.get(name) or "").strip()
        if not raw:
            return None
        try:
            return int(raw)
        except ValueError:
            return None

    completed_days = int_or_none("completed_days") or 30
    failed_days = int_or_none("failed_days") or 90
    event_days = int_or_none("event_days") or 30
    purge_workers_active_seconds = int_or_none("purge_workers_active_seconds")
    core.apply_retention_cleanup(
        completed_days=completed_days,
        failed_days=failed_days,
        event_days=event_days,
        purge_workers_active_seconds=purge_workers_active_seconds,
        updated_by="ui",
    )
    query = f"completed_days={completed_days}&failed_days={failed_days}&event_days={event_days}"
    if purge_workers_active_seconds is not None:
        query += f"&purge_workers_active_seconds={purge_workers_active_seconds}"
    return RedirectResponse(url=f"/ui/manager/retention?{query}", status_code=303)


@router.get("/manager/log", response_class=HTMLResponse)
def manager_log_page(tail: int = core.MAX_MANAGER_LOG_TAIL_BYTES, view: str = "table") -> str:
    tail = max(1, min(core.MAX_MANAGER_LOG_TAIL_BYTES, int(tail)))
    text = core.read_manager_log(tail_bytes=tail)
    table_active = "secondary" if view == "raw" else ""
    raw_active = "" if view == "raw" else "secondary"
    log_body = f"<pre>{h(text)}</pre>" if view == "raw" else manager_log_table(text)
    body = f"""
    <div class='topline'>
      <div><h1>Manager Log</h1><div class='muted'>Broker-side JSONL audit log · <code>{h(core.manager_log_path())}</code></div></div>
      <div class='actions'>
        <a class='button {table_active}' href='/ui/manager/log?tail={h(tail)}'>Table View</a>
        <a class='button {raw_active}' href='/ui/manager/log?view=raw&tail={h(tail)}'>Raw View</a>
        <a class='button secondary' href='/manager/log?tail={h(tail)}'>Raw API</a>
        <a class='button secondary' href='/ui'>Dashboard</a>
      </div>
    </div>
    <section class='card'>
      <p class='muted'>Shows manager-side operational events. Normal mode records submissions, warnings, failures, recovery, and admin actions; high-volume successful <code>TaskAccepted</code>/<code>TaskCompleted</code> events are debug-only via <code>TASKGRID_EVENT_MODE=debug</code> or <code>TASKGRID_VERBOSE_TASK_EVENTS=1</code>.</p>
      {log_body}
    </section>
    """
    return layout("Manager Log", body, refresh=True)


@router.get("/submit", response_class=HTMLResponse)
def submit_page(error: str | None = None) -> str:
    sample = json.dumps([{"x": 2}, {"x": 5}, {"x": 10}], indent=2)
    error_html = f"<div class='alert error'>{h(error)}</div>" if error else ""
    body = f"""
    <div class='topline'>
      <div><h1>Submit Job</h1><div class='muted'>Create a job made of one or more JSON task payloads.</div></div>
    </div>
    <section class='card'>
      {error_html}
      <form class='stack' method='post' action='/ui/submit'>
        <label>Job name<input name='name' value='square demo' required maxlength='200'></label>
        <div class='form-row'>
          <label>Task type<input name='task_type' value='square' required maxlength='120'></label>
          <label>Job priority override<input name='priority' type='number' placeholder='inherit'></label>
          <label>Max retries<input name='max_retries' type='number' min='0' max='20' value='2'></label>
        </div>
        <div class='form-row'>
          <label>Session name<input name='session_name' value='square demo session' maxlength='200'></label>
          <label>Session priority<input name='session_priority' type='number' value='0'></label>
          <label>Existing session ID <input name='session_id' placeholder='optional: attach this job to an existing session'></label>
        </div>
        <div class='form-row'>
          <label>Client ID<input name='client_id' placeholder='optional, generated if blank'></label>
          <label>Resume token<input name='resume_token' placeholder='optional, required to attach securely'></label>
          <label>Required worker tags <input name='required_tags' placeholder='gpu, risk'></label>
        </div>
        <label><span><input name='require_capable_worker' type='checkbox' value='1' style='width:auto; margin-right:8px'>Reject if no active capable worker is online</span></label>
        <label>Tasks JSON array<textarea name='tasks_json' required>{h(sample)}</textarea></label>
        <label>Metadata JSON object<textarea name='metadata_json' style='min-height:90px'>{{}}</textarea></label>
        <div class='actions'><button type='submit'>Submit Job</button><a class='button secondary' href='/ui'>Cancel</a></div>
      </form>
    </section>
    <section class='card' style='margin-top:16px'>
      <h2>Tip</h2>
      <p class='muted'>The default task type <code>square</code> works with <code>examples.custom_tasks</code>. To satisfy required worker tags, start a tagged worker:</p>
      <pre>python -m taskgrid.worker --module examples.custom_tasks --tag gpu</pre>
    </section>
    """
    return layout("Submit Job", body)


@router.post("/submit")
async def submit_from_ui(request: Request):
    form = await request.form()
    try:
        name = str(form.get("name") or "").strip()
        task_type = str(form.get("task_type") or "").strip()
        priority_raw = str(form.get("priority") or "").strip()
        priority = int(priority_raw) if priority_raw else None
        session_priority_raw = str(form.get("session_priority") or "").strip()
        session_priority = int(session_priority_raw) if session_priority_raw else None
        max_retries = int(str(form.get("max_retries") or "2"))
        tasks = json.loads(str(form.get("tasks_json") or "[]"))
        metadata = json.loads(str(form.get("metadata_json") or "{}"))
        session_id = str(form.get("session_id") or "").strip() or None
        session_name = str(form.get("session_name") or "").strip() or None
        client_id = str(form.get("client_id") or "").strip() or None
        resume_token = str(form.get("resume_token") or "").strip() or None
        required_tags = [tag.strip().lower() for tag in str(form.get("required_tags") or "").split(",") if tag.strip()]
        require_capable_worker = str(form.get("require_capable_worker") or "").lower() in {"1", "true", "on", "yes"}
        if required_tags:
            metadata["required_tags"] = sorted(set(required_tags))
        if not name:
            raise ValueError("Job name is required.")
        if not task_type:
            raise ValueError("Task type is required.")
        if not isinstance(tasks, list) or not tasks:
            raise ValueError("Tasks JSON must be a non-empty array.")
        if not all(isinstance(item, dict) for item in tasks):
            raise ValueError("Each task payload must be a JSON object.")
        if not isinstance(metadata, dict):
            raise ValueError("Metadata JSON must be an object.")
        if max_retries < 0 or max_retries > 20:
            raise ValueError("Max retries must be between 0 and 20.")
    except Exception as exc:
        return submit_page(error=str(exc))

    try:
        job = core.create_job(
            name=name,
            task_type=task_type,
            payloads=tasks,
            priority=priority,
            max_retries=max_retries,
            metadata=metadata,
            session_id=session_id,
            session_name=session_name,
            session_priority=session_priority,
            require_capable_worker=require_capable_worker,
            client_id=client_id,
            resume_token=resume_token,
        )
    except core.CapabilityError as exc:
        return submit_page(error=f"{exc}: {json.dumps(exc.details.get('warnings', []), ensure_ascii=False)}")
    return RedirectResponse(url=f"/ui/sessions/{path_id(job['session_id'])}", status_code=303)
