from __future__ import annotations

import atexit
import json
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template_string, request

from enm_mdt_scheduler.models import EnmSession, MdtTransferSettings, ScheduledJob
from enm_mdt_scheduler.scheduler_service import SchedulerService
from enm_mdt_scheduler.sessions import fallback_sessions, load_manager_sessions, merge_sessions


APP_DIR = Path(__file__).resolve().parent
CONFIG_PATH = APP_DIR / "config.json"
SCHEDULES_PATH = APP_DIR / "schedules.json"
DEFAULT_PORT = 8095

TYPE_LABELS = {
    "mdt_transfer": "MDT Transfer",
    "python_script": "Script (.py)",
}

app = Flask(__name__)
state_lock = threading.RLock()


def load_config() -> dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {}
    try:
        raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def save_config(payload: dict[str, Any]) -> None:
    CONFIG_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_sessions() -> list[EnmSession]:
    config = load_config()
    imported = load_manager_sessions() or fallback_sessions()
    saved = [
        EnmSession.from_dict(item)
        for item in config.get("sessions", [])
        if isinstance(item, dict)
    ]
    return merge_sessions(imported, saved)


sessions: list[EnmSession] = load_sessions()
scheduler = SchedulerService(SCHEDULES_PATH, sessions)
scheduler.start_loop()
atexit.register(scheduler.shutdown)


def sessions_by_id() -> dict[str, EnmSession]:
    return {session.id: session for session in sessions}


def save_sessions() -> None:
    config = load_config()
    config["sessions"] = [session.to_dict(include_password=False) for session in sessions]
    save_config(config)


def schedule_status(schedule: ScheduledJob) -> tuple[str, str]:
    if scheduler.is_schedule_running(schedule):
        return "running", "running"
    if schedule.last_error:
        return "error", "error"
    if schedule.enabled:
        return "active", "active"
    return "stopped", "stopped"


def every_label(schedule: ScheduledJob) -> str:
    if schedule.test_interval_seconds > 0:
        return f"{schedule.test_interval_seconds} sec"
    return f"{schedule.interval_minutes} min"


def window_label(schedule: ScheduledJob) -> str:
    if schedule.start_time and schedule.end_time:
        return f"{schedule.start_time} -> {schedule.end_time}"
    return "always"


def last_next_label(schedule: ScheduledJob) -> str:
    parts = []
    if schedule.last_run:
        parts.append(f"Last: {schedule.last_run}")
    if schedule.next_run and schedule.enabled:
        parts.append(f"Next: {schedule.next_run}")
    if schedule.last_error:
        parts.append(f"Status: {schedule.last_error}")
    return " | ".join(parts) if parts else "-"


def schedule_to_api(schedule: ScheduledJob) -> dict[str, Any]:
    status, tag = schedule_status(schedule)
    session = sessions_by_id().get(schedule.session_id or "")
    job = scheduler.get_job(schedule.last_job_id)
    return {
        "id": schedule.id,
        "name": schedule.name,
        "job_type": schedule.job_type,
        "type_label": TYPE_LABELS.get(schedule.job_type, schedule.job_type),
        "session_id": schedule.session_id,
        "session_name": session.name if session else "-- None --",
        "status": status,
        "status_tag": tag,
        "every": every_label(schedule),
        "window": window_label(schedule),
        "last_next": last_next_label(schedule),
        "interval_minutes": schedule.interval_minutes,
        "test_interval_seconds": schedule.test_interval_seconds,
        "script_path": schedule.script_path,
        "start_time": schedule.start_time,
        "end_time": schedule.end_time,
        "enabled": schedule.enabled,
        "last_run": schedule.last_run,
        "next_run": schedule.next_run,
        "last_error": schedule.last_error,
        "last_job_id": schedule.last_job_id,
        "job_status": job.get("status") if job else "idle",
        "mdt": schedule.mdt.to_dict(),
    }


def session_to_api(session: EnmSession) -> dict[str, Any]:
    return {
        "id": session.id,
        "name": session.name,
        "host": session.host,
        "port": session.port,
        "username": session.username,
        "timeout": session.timeout,
        "has_password": bool(session.password),
    }


def schedule_from_payload(payload: dict[str, Any]) -> ScheduledJob:
    name = str(payload.get("name") or "").strip()
    if not name:
        raise ValueError("Enter a schedule name.")
    job_type = str(payload.get("job_type") or "mdt_transfer")
    if job_type not in TYPE_LABELS:
        raise ValueError(f"Unsupported job type: {job_type}")

    session_id = payload.get("session_id") or None
    start_time = payload.get("start_time") or None
    end_time = payload.get("end_time") or None
    if start_time and end_time:
        if parse_datetime(end_time) <= parse_datetime(start_time):
            raise ValueError("The end time must be after the start time.")

    script_path = ""
    mdt = MdtTransferSettings()
    if job_type == "python_script":
        script_path = str(payload.get("script_path") or "").strip()
        if not script_path:
            raise ValueError("Select a Python script.")
        if not Path(script_path).is_file():
            raise ValueError("The selected script file does not exist.")
    else:
        if not session_id:
            raise ValueError("Select an ENM session for MDT Transfer.")
        raw_mdt = payload.get("mdt") or {}
        remote_bases = raw_mdt.get("remote_bases") or []
        if isinstance(remote_bases, str):
            remote_bases = [
                item.strip().rstrip("/")
                for item in remote_bases.replace("\n", ";").split(";")
                if item.strip()
            ]
        mdt = MdtTransferSettings(
            local_base=str(raw_mdt.get("local_base") or "").strip(),
            remote_bases=[str(item).rstrip("/") for item in remote_bases if str(item).strip()],
            initial_lookback_minutes=int(raw_mdt.get("initial_lookback_minutes") or 90),
            grace_minutes=int(raw_mdt.get("grace_minutes") or 30),
            max_parallel_downloads=int(raw_mdt.get("max_parallel_downloads") or 2),
            dry_run=bool(raw_mdt.get("dry_run", True)),
        )
        if not mdt.local_base:
            raise ValueError("Select a local base folder.")
        if not mdt.remote_bases:
            raise ValueError("Add at least one remote CELLTRACE base.")

    return ScheduledJob(
        name=name,
        job_type=job_type,
        session_id=str(session_id) if session_id else None,
        script_path=script_path,
        interval_minutes=max(1, int(payload.get("interval_minutes") or 60)),
        test_interval_seconds=max(0, int(payload.get("test_interval_seconds") or 0)),
        start_time=start_time,
        end_time=end_time,
        enabled=bool(payload.get("enabled", False)),
        mdt=mdt,
    )


def parse_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def error_response(exc: Exception, status: int = 400):
    return jsonify({"ok": False, "error": str(exc)}), status


@app.get("/")
def index():
    return render_template_string(INDEX_HTML, default_local=str(APP_DIR / "MDT_Downloads"))


@app.get("/api/state")
def api_state():
    with state_lock:
        return jsonify(
            {
                "ok": True,
                "sessions": [session_to_api(session) for session in sessions],
                "schedules": [schedule_to_api(schedule) for schedule in scheduler.list_schedules()],
            }
        )


@app.post("/api/session")
def api_session_update():
    payload = request.get_json(force=True)
    session_id = str(payload.get("id") or "")
    with state_lock:
        session = sessions_by_id().get(session_id)
        if not session:
            return error_response(ValueError("Session not found."), 404)
        try:
            session.host = str(payload.get("host") or "").strip()
            session.port = int(payload.get("port") or 22)
            session.username = str(payload.get("username") or "").strip()
            if "password" in payload:
                session.password = str(payload.get("password") or "")
            session.timeout = int(payload.get("timeout") or 30)
            scheduler.set_sessions(sessions)
            save_sessions()
        except Exception as exc:  # noqa: BLE001
            return error_response(exc)
    return jsonify({"ok": True})


@app.post("/api/import-manager-sessions")
def api_import_manager_sessions():
    global sessions
    imported = load_manager_sessions()
    if not imported:
        return error_response(ValueError("No ENM Manager sessions found."), 404)
    with state_lock:
        current = sessions_by_id()
        for session in imported:
            old = current.get(session.id)
            if old:
                session.password = old.password
        sessions = merge_sessions(imported, [])
        scheduler.set_sessions(sessions)
        save_sessions()
    return jsonify({"ok": True, "count": len(imported)})


@app.post("/api/schedules")
def api_add_schedule():
    payload = request.get_json(force=True)
    try:
        schedule = schedule_from_payload(payload)
        schedule_id = scheduler.add_schedule(schedule)
    except Exception as exc:  # noqa: BLE001
        return error_response(exc)
    return jsonify({"ok": True, "id": schedule_id})


@app.put("/api/schedules/<schedule_id>")
def api_update_schedule(schedule_id: str):
    payload = request.get_json(force=True)
    try:
        scheduler.update_schedule(schedule_id, schedule_from_payload(payload))
    except Exception as exc:  # noqa: BLE001
        return error_response(exc)
    return jsonify({"ok": True})


@app.delete("/api/schedules/<schedule_id>")
def api_delete_schedule(schedule_id: str):
    scheduler.remove_schedule(schedule_id)
    return jsonify({"ok": True})


@app.post("/api/schedules/<schedule_id>/start")
def api_start_schedule(schedule_id: str):
    scheduler.start_schedule(schedule_id)
    return jsonify({"ok": True})


@app.post("/api/schedules/<schedule_id>/stop")
def api_stop_schedule(schedule_id: str):
    scheduler.stop_schedule(schedule_id)
    return jsonify({"ok": True})


@app.post("/api/schedules/<schedule_id>/run-now")
def api_run_now(schedule_id: str):
    try:
        job_id = scheduler.run_now(schedule_id)
    except Exception as exc:  # noqa: BLE001
        return error_response(exc)
    return jsonify({"ok": True, "job_id": job_id})


@app.post("/api/schedules/<schedule_id>/stop-run")
def api_stop_run(schedule_id: str):
    schedule = next(
        (item for item in scheduler.list_schedules() if item.id == schedule_id),
        None,
    )
    if not schedule:
        return error_response(ValueError("Schedule not found."), 404)
    ok, message = scheduler.cancel_job(schedule.last_job_id)
    return jsonify({"ok": ok, "message": message})


@app.get("/api/schedules/<schedule_id>/progress")
def api_progress(schedule_id: str):
    schedule = next(
        (item for item in scheduler.list_schedules() if item.id == schedule_id),
        None,
    )
    if not schedule:
        return error_response(ValueError("Schedule not found."), 404)
    job = scheduler.get_job(schedule.last_job_id) or {"status": "idle", "lines": []}
    return jsonify(
        {
            "ok": True,
            "schedule": schedule_to_api(schedule),
            "job": job,
        }
    )


INDEX_HTML = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>ENM MDT Scheduler</title>
  <style>
    :root {
      --bg: #0f1b33;
      --panel: #0f1b33;
      --table: #101a2e;
      --table-alt: #15213a;
      --header: #1a2947;
      --line: #60708d;
      --grid: #2d3a55;
      --text: #e7edf8;
      --title: #d7e2f2;
      --muted: #a8b3c7;
      --input: #f4f7fb;
      --input-disabled: #dbe2ec;
      --input-text: #081426;
      --selected: #243b63;
      --hover: #e5edf9;
      --accent: #2f6fed;
      --green: #68d391;
      --red: #ff7b7b;
      --orange: #f6ad55;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: Segoe UI, Arial, sans-serif;
      font-size: 14px;
    }
    main { padding: 14px; max-width: 1240px; margin: 0 auto; }
    h1 { margin: 0 0 10px; font-size: 26px; }
    fieldset {
      border: 1px solid var(--line);
      border-radius: 4px;
      padding: 14px 12px 12px;
      margin: 0 0 12px;
      background: var(--panel);
      font-weight: 700;
    }
    legend { padding: 0 4px; font-weight: 700; color: var(--title); }
    label { font-weight: 700; }
    input, select, button {
      font: inherit;
      border-radius: 3px;
      border: 1px solid #a9b4c6;
      padding: 5px 8px;
    }
    input, select {
      background: var(--input);
      color: var(--input-text);
      font-weight: 400;
    }
    input:disabled, select:disabled {
      background: var(--input-disabled);
      color: #4b5568;
    }
    input::selection {
      background: var(--accent);
      color: #ffffff;
    }
    button {
      cursor: pointer;
      background: var(--input);
      color: var(--input-text);
      font-weight: 600;
      min-width: 86px;
      border-radius: 4px;
      padding: 6px 14px;
    }
    button:hover { background: var(--hover); }
    button:disabled {
      background: var(--input-disabled);
      color: #718096;
      cursor: default;
    }
    .row { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
    .grow { flex: 1 1 220px; }
    .short { width: 78px; }
    .medium { width: 160px; }
    .long { width: min(100%, 680px); }
    table { width: 100%; border-collapse: collapse; table-layout: fixed; }
    th, td { border: 1px solid var(--grid); padding: 10px 8px; text-align: left; }
    th {
      background: var(--header);
      color: var(--title);
      border-color: var(--grid);
      font-weight: 700;
    }
    tr { background: var(--table); }
    tr:nth-child(even) { background: var(--table-alt); }
    tr.selected { background: var(--selected); }
    td { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .status-running { color: var(--orange); font-weight: 800; }
    .status-active { color: var(--green); font-weight: 800; }
    .status-stopped, .status-error { color: var(--red); font-weight: 800; }
    .actions { margin-top: 8px; }
    .progress { display: none; }
    pre {
      height: 180px;
      overflow: auto;
      margin: 8px 0 0;
      padding: 10px;
      background: #071120;
      color: var(--title);
      border: 1px solid var(--line);
      font-family: Consolas, monospace;
      font-size: 13px;
    }
    .grid {
      display: grid;
      grid-template-columns: auto minmax(240px, 1fr) auto auto auto auto;
      gap: 8px;
      align-items: center;
    }
    .full { grid-column: 1 / -1; }
    .hidden { display: none !important; }
    .note { color: var(--muted); margin-left: 8px; }
    .error { color: var(--red); font-weight: 700; }
    @media (max-width: 900px) {
      .grid { grid-template-columns: 1fr; }
      .long, .medium, .short { width: 100%; }
      td, th { font-size: 12px; }
    }
  </style>
</head>
<body>
<main>
  <h1>Script Scheduler</h1>

  <fieldset>
    <legend>ENM Sessions</legend>
    <div class="row">
      <label>Session</label>
      <select id="sessionSelect" class="medium"></select>
      <label>Host</label>
      <input id="sessionHost" class="medium">
      <label>Port</label>
      <input id="sessionPort" class="short" type="number">
      <label>User</label>
      <input id="sessionUser" class="medium">
      <label>Password</label>
      <input id="sessionPassword" class="medium" type="password" autocomplete="current-password">
      <label>Timeout</label>
      <input id="sessionTimeout" class="short" type="number">
      <button onclick="saveSession()">Apply</button>
      <button onclick="importSessions()">Import Manager Sessions</button>
      <span id="sessionMsg" class="note"></span>
    </div>
  </fieldset>

  <fieldset>
    <legend>Scheduled Jobs</legend>
    <table>
      <thead>
        <tr>
          <th style="width:105px">Status</th>
          <th style="width:180px">Name</th>
          <th style="width:130px">Type</th>
          <th style="width:145px">Session</th>
          <th style="width:80px">Every</th>
          <th style="width:260px">Window</th>
          <th>Last / Next</th>
        </tr>
      </thead>
      <tbody id="scheduleRows"></tbody>
    </table>
    <div class="row actions">
      <button onclick="showProgress()">Progress</button>
      <button onclick="scheduleAction('start')">Start</button>
      <button onclick="scheduleAction('stop')">Stop</button>
      <button onclick="scheduleAction('run-now')">Run Now</button>
      <button onclick="scheduleAction('stop-run')">Stop Run</button>
      <button onclick="editSelected()">Edit</button>
      <button onclick="deleteSelected()">Delete</button>
      <span id="jobMsg" class="note"></span>
    </div>
  </fieldset>

  <fieldset id="progressPanel" class="progress">
    <legend id="progressTitle">Progress</legend>
    <button onclick="hideProgress()">Close</button>
    <pre id="progressText"></pre>
  </fieldset>

  <fieldset>
    <legend id="formTitle">Add New Schedule</legend>
    <div class="grid">
      <label>Name</label>
      <input id="name" class="grow" placeholder="MDT BARB hourly">
      <label>Type</label>
      <select id="jobType" class="medium" onchange="typeChanged()">
        <option value="mdt_transfer">MDT Transfer</option>
        <option value="python_script">Script (.py)</option>
      </select>
      <label>Session</label>
      <select id="jobSession" class="medium"></select>

      <div id="scriptFields" class="full hidden row">
        <label>Script</label>
        <input id="scriptPath" class="long" placeholder="C:\path\script.py">
      </div>

      <div id="mdtFields" class="full">
        <div class="row">
          <label>Local base</label>
          <input id="localBase" class="long" value="{{ default_local }}">
        </div>
        <div class="row" style="margin-top:8px">
          <label>Remote bases</label>
          <input id="remoteBases" class="long" value="/ericsson/pmic1/CELLTRACE;/ericsson/pmic2/CELLTRACE">
        </div>
        <div class="row" style="margin-top:8px">
          <label>First lookback min</label>
          <input id="lookback" class="short" type="number" value="90">
          <label>Grace min</label>
          <input id="grace" class="short" type="number" value="30">
          <label>Parallel</label>
          <input id="parallel" class="short" type="number" value="2">
          <label><input id="dryRun" type="checkbox" checked> Dry run (scan only)</label>
        </div>
      </div>

      <div class="full row" style="margin-top:4px">
        <label>Run every</label>
        <input id="interval" class="short" type="number" value="60">
        <span>min</span>
        <label>Test sec</label>
        <input id="testSeconds" class="short" type="number" value="0">
        <label><input id="timeWindow" type="checkbox" onchange="windowChanged()"> Time window</label>
        <span>From</span>
        <input id="startDate" class="medium" value="">
        <input id="startHour" class="short" type="number" min="0" max="23">
        <span>:</span>
        <input id="startMinute" class="short" type="number" min="0" max="59">
        <span>To</span>
        <input id="endDate" class="medium" value="">
        <input id="endHour" class="short" type="number" min="0" max="23">
        <span>:</span>
        <input id="endMinute" class="short" type="number" min="0" max="59">
        <label><input id="enabled" type="checkbox"> Start immediately</label>
      </div>

      <div class="full row">
        <button id="submitBtn" onclick="submitSchedule()">Add Schedule</button>
        <button id="cancelEditBtn" class="hidden" onclick="cancelEdit()">Cancel edit</button>
        <span id="formMsg" class="note"></span>
      </div>
    </div>
  </fieldset>
</main>

<script>
let state = {sessions: [], schedules: []};
let selectedScheduleId = null;
let editingScheduleId = null;
let progressScheduleId = null;
let renderedSessionId = null;

function $(id) { return document.getElementById(id); }

function setMsg(id, text, isError=false) {
  const el = $(id);
  el.textContent = text || '';
  el.className = isError ? 'error' : 'note';
}

function pad2(value) {
  return String(value).padStart(2, '0');
}

function initDates() {
  const now = new Date();
  const later = new Date(now.getTime() + 60 * 60 * 1000);
  $('startDate').value = now.toISOString().slice(0, 10);
  $('endDate').value = later.toISOString().slice(0, 10);
  $('startHour').value = pad2(now.getHours());
  $('startMinute').value = pad2(now.getMinutes());
  $('endHour').value = pad2(later.getHours());
  $('endMinute').value = pad2(later.getMinutes());
  windowChanged();
}

async function api(path, options={}) {
  const response = await fetch(path, {
    headers: {'Content-Type': 'application/json'},
    ...options
  });
  const data = await response.json();
  if (!data.ok) throw new Error(data.error || data.message || 'Request failed');
  return data;
}

async function refreshState() {
  try {
    state = await api('/api/state');
    renderSessions();
    renderSchedules();
    if (progressScheduleId) refreshProgress();
  } catch (err) {
    setMsg('jobMsg', err.message, true);
  }
}

function renderSessions() {
  const currentSession = $('sessionSelect').value;
  const currentJobSession = $('jobSession').value;
  for (const select of [$('sessionSelect'), $('jobSession')]) {
    select.innerHTML = '';
    for (const session of state.sessions) {
      const option = document.createElement('option');
      option.value = session.id;
      option.textContent = session.name;
      select.appendChild(option);
    }
  }
  if (currentSession) $('sessionSelect').value = currentSession;
  if (currentJobSession) $('jobSession').value = currentJobSession;
  if (!$('sessionSelect').value && state.sessions[0]) $('sessionSelect').value = state.sessions[0].id;
  if (!$('jobSession').value && state.sessions[0]) $('jobSession').value = state.sessions[0].id;
  if ($('sessionSelect').value !== renderedSessionId) {
    fillSessionForm();
  }
}

function fillSessionForm() {
  const session = state.sessions.find(item => item.id === $('sessionSelect').value);
  if (!session) return;
  $('sessionHost').value = session.host || '';
  $('sessionPort').value = session.port || 22;
  $('sessionUser').value = session.username || '';
  $('sessionTimeout').value = session.timeout || 30;
  $('sessionPassword').value = '';
  $('sessionPassword').placeholder = session.has_password ? 'password set' : '';
  renderedSessionId = session.id;
}

function renderSchedules() {
  const body = $('scheduleRows');
  body.innerHTML = '';
  for (const schedule of state.schedules) {
    const tr = document.createElement('tr');
    tr.dataset.id = schedule.id;
    if (schedule.id === selectedScheduleId) tr.classList.add('selected');
    tr.onclick = () => {
      selectedScheduleId = schedule.id;
      progressScheduleId = progressScheduleId || schedule.id;
      renderSchedules();
    };
    tr.innerHTML = `
      <td class="status-${schedule.status_tag}">${escapeHtml(schedule.status)}</td>
      <td title="${escapeAttr(schedule.name)}">${escapeHtml(schedule.name)}</td>
      <td>${escapeHtml(schedule.type_label)}</td>
      <td>${escapeHtml(schedule.session_name)}</td>
      <td>${escapeHtml(schedule.every)}</td>
      <td title="${escapeAttr(schedule.window)}">${escapeHtml(schedule.window)}</td>
      <td title="${escapeAttr(schedule.last_next)}">${escapeHtml(schedule.last_next)}</td>
    `;
    body.appendChild(tr);
  }
  if (selectedScheduleId && !state.schedules.some(item => item.id === selectedScheduleId)) {
    selectedScheduleId = null;
  }
}

async function saveSession() {
  try {
    await api('/api/session', {
      method: 'POST',
      body: JSON.stringify({
        id: $('sessionSelect').value,
        host: $('sessionHost').value,
        port: Number($('sessionPort').value || 22),
        username: $('sessionUser').value,
        password: $('sessionPassword').value,
        timeout: Number($('sessionTimeout').value || 30)
      })
    });
    setMsg('sessionMsg', 'Session updated. Password stays only in memory.');
    await refreshState();
  } catch (err) {
    setMsg('sessionMsg', err.message, true);
  }
}

async function importSessions() {
  try {
    const result = await api('/api/import-manager-sessions', {method: 'POST'});
    setMsg('sessionMsg', `Imported ${result.count} session(s).`);
    await refreshState();
  } catch (err) {
    setMsg('sessionMsg', err.message, true);
  }
}

async function scheduleAction(action) {
  if (!selectedScheduleId) return setMsg('jobMsg', 'Select a schedule first.', true);
  try {
    const result = await api(`/api/schedules/${selectedScheduleId}/${action}`, {method: 'POST'});
    setMsg('jobMsg', result.message || 'OK');
    if (action === 'run-now') {
      progressScheduleId = selectedScheduleId;
      showProgress();
    }
    await refreshState();
  } catch (err) {
    setMsg('jobMsg', err.message, true);
  }
}

async function deleteSelected() {
  if (!selectedScheduleId) return setMsg('jobMsg', 'Select a schedule first.', true);
  const schedule = state.schedules.find(item => item.id === selectedScheduleId);
  if (!confirm(`Remove schedule '${schedule?.name || selectedScheduleId}'?`)) return;
  try {
    await api(`/api/schedules/${selectedScheduleId}`, {method: 'DELETE'});
    selectedScheduleId = null;
    await refreshState();
  } catch (err) {
    setMsg('jobMsg', err.message, true);
  }
}

function editSelected() {
  if (!selectedScheduleId) return setMsg('jobMsg', 'Select a schedule first.', true);
  const schedule = state.schedules.find(item => item.id === selectedScheduleId);
  if (!schedule) return;
  editingScheduleId = schedule.id;
  $('formTitle').textContent = 'Edit Schedule';
  $('submitBtn').textContent = 'Update Schedule';
  $('cancelEditBtn').classList.remove('hidden');
  $('name').value = schedule.name || '';
  $('jobType').value = schedule.job_type || 'mdt_transfer';
  $('jobSession').value = schedule.session_id || '';
  $('scriptPath').value = schedule.script_path || '';
  $('interval').value = schedule.interval_minutes || 60;
  $('testSeconds').value = schedule.test_interval_seconds || 0;
  $('enabled').checked = !!schedule.enabled;
  $('localBase').value = schedule.mdt?.local_base || '';
  $('remoteBases').value = (schedule.mdt?.remote_bases || []).join(';');
  $('lookback').value = schedule.mdt?.initial_lookback_minutes || 90;
  $('grace').value = schedule.mdt?.grace_minutes || 30;
  $('parallel').value = schedule.mdt?.max_parallel_downloads || 2;
  $('dryRun').checked = !!schedule.mdt?.dry_run;
  $('timeWindow').checked = !!(schedule.start_time && schedule.end_time);
  if (schedule.start_time) setDateParts('start', schedule.start_time);
  if (schedule.end_time) setDateParts('end', schedule.end_time);
  typeChanged();
  windowChanged();
}

function cancelEdit() {
  editingScheduleId = null;
  $('formTitle').textContent = 'Add New Schedule';
  $('submitBtn').textContent = 'Add Schedule';
  $('cancelEditBtn').classList.add('hidden');
  $('name').value = '';
}

function setDateParts(prefix, value) {
  const [date, time] = value.replace('T', ' ').split(' ');
  const [hour, minute] = (time || '00:00').split(':');
  $(`${prefix}Date`).value = date;
  $(`${prefix}Hour`).value = hour;
  $(`${prefix}Minute`).value = minute;
}

function collectPayload() {
  const timeWindow = $('timeWindow').checked;
  return {
    name: $('name').value,
    job_type: $('jobType').value,
    session_id: $('jobSession').value || null,
    script_path: $('scriptPath').value,
    interval_minutes: Number($('interval').value || 60),
    test_interval_seconds: Number($('testSeconds').value || 0),
    enabled: $('enabled').checked,
    start_time: timeWindow ? dateValue('start') : null,
    end_time: timeWindow ? dateValue('end') : null,
    mdt: {
      local_base: $('localBase').value,
      remote_bases: $('remoteBases').value,
      initial_lookback_minutes: Number($('lookback').value || 90),
      grace_minutes: Number($('grace').value || 30),
      max_parallel_downloads: Number($('parallel').value || 2),
      dry_run: $('dryRun').checked
    }
  };
}

function dateValue(prefix) {
  return `${$(`${prefix}Date`).value} ${pad2($(`${prefix}Hour`).value)}:${pad2($(`${prefix}Minute`).value)}:00`;
}

async function submitSchedule() {
  try {
    const method = editingScheduleId ? 'PUT' : 'POST';
    const path = editingScheduleId ? `/api/schedules/${editingScheduleId}` : '/api/schedules';
    const result = await api(path, {method, body: JSON.stringify(collectPayload())});
    selectedScheduleId = editingScheduleId || result.id;
    cancelEdit();
    setMsg('formMsg', 'Schedule saved.');
    await refreshState();
  } catch (err) {
    setMsg('formMsg', err.message, true);
  }
}

function showProgress() {
  if (!selectedScheduleId) return setMsg('jobMsg', 'Select a schedule first.', true);
  progressScheduleId = selectedScheduleId;
  $('progressPanel').style.display = 'block';
  refreshProgress();
}

function hideProgress() {
  progressScheduleId = null;
  $('progressPanel').style.display = 'none';
}

async function refreshProgress() {
  if (!progressScheduleId) return;
  try {
    const data = await api(`/api/schedules/${progressScheduleId}/progress`);
    $('progressTitle').textContent = `Progress - ${data.schedule.name} [${data.job.status}]`;
    $('progressText').textContent = (data.job.lines || []).join('\n');
    $('progressText').scrollTop = $('progressText').scrollHeight;
  } catch (err) {
    $('progressText').textContent = err.message;
  }
}

function typeChanged() {
  const isMdt = $('jobType').value === 'mdt_transfer';
  $('mdtFields').classList.toggle('hidden', !isMdt);
  $('scriptFields').classList.toggle('hidden', isMdt);
}

function windowChanged() {
  const enabled = $('timeWindow').checked;
  for (const id of ['startDate','startHour','startMinute','endDate','endHour','endMinute']) {
    $(id).disabled = !enabled;
  }
}

function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, ch => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[ch]));
}

function escapeAttr(value) { return escapeHtml(value).replace(/"/g, '&quot;'); }

$('sessionSelect').addEventListener('change', () => {
  renderedSessionId = null;
  fillSessionForm();
});
initDates();
typeChanged();
refreshState();
setInterval(refreshState, 2000);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    port = int(os.environ.get("PORT") or DEFAULT_PORT)
    print(f"ENM MDT Scheduler running at http://127.0.0.1:{port}")
    app.run(host="127.0.0.1", port=port, debug=False, use_reloader=False)
