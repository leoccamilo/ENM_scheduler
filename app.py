from __future__ import annotations

import atexit
import html
import json
import os
import threading
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from enm_mdt_scheduler.models import EnmSession, MdtTransferSettings, ScheduledJob
from enm_mdt_scheduler.scheduler_service import SchedulerService
from enm_mdt_scheduler.security import assert_command_safe
from enm_mdt_scheduler.secure_store import SecureStore
from enm_mdt_scheduler.sessions import fallback_sessions, merge_sessions


APP_DIR = Path(__file__).resolve().parent
CONFIG_PATH = APP_DIR / "config.json"
SCHEDULES_PATH = APP_DIR / "schedules.json"
CREDENTIALS_PATH = APP_DIR / ".enm_credentials.json"
COLLECTOR_SOURCE = APP_DIR / "enm_mdt_scheduler" / "collector.py"
LOGO_PATH = APP_DIR / "static" / "amdocs_logo.png"
MAX_PREVIEW_BYTES = 500_000
DEFAULT_PORT = 8095
secure_store = SecureStore(CREDENTIALS_PATH)

MDT_PREVIEW_SUMMARY = (
    "MDT Transfer (built-in logic). What this job does on each cycle:\n"
    "  1. Connects to the selected ENM session over SFTP.\n"
    "  2. Lists the CELLTRACE bases and auto-discovers sites\n"
    "     (MeContext=<site> / ManagedElement=<site> folders).\n"
    "     - 'Collect all': processes every discovered site.\n"
    "     - 'From site list': processes only the sites registered in this job\n"
    "       (case-insensitive site-name comparison).\n"
    "  3. Selects only *.bin.gz and *.gpb.gz files. The first successful cycle\n"
    "     collects every file still available; later cycles scan from the\n"
    "     previous checkpoint minus 'Grace min'.\n"
    "  4. Skips files already downloaded according to _state/downloaded_files.json.\n"
    "  5. Downloads new files to <Local base>/DDMMYYYY/<site>/.\n"
    "     In 'Dry run', it only lists what it would download and does not\n"
    "     transfer files or save state.\n\n"
    "Actual source code (enm_mdt_scheduler/collector.py):\n"
    + "=" * 78 + "\n"
)

TYPE_LABELS = {
    "mdt_transfer": "MDT Transfer",
    "python_script": "Script (.py)",
}

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
    saved = [
        EnmSession.from_dict(item)
        for item in config.get("sessions", [])
        if isinstance(item, dict)
    ]
    # Sessions are fully self-contained in config.json. On a brand-new install
    # (no saved sessions yet) seed the default ENM connection names once so the
    # user has something to edit; afterwards their saved list is authoritative.
    if saved:
        loaded = sorted(saved, key=lambda item: item.name.lower())
    else:
        loaded = merge_sessions(fallback_sessions(), [])

    saved_passwords = secure_store.load_passwords()
    for session in loaded:
        session.password = saved_passwords.get(session.id, session.password)
    return loaded


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
    secure_store.save_passwords(
        {session.id: session.password for session in sessions if session.password}
    )


def schedule_status(schedule: ScheduledJob) -> tuple[str, str]:
    if scheduler.is_schedule_running(schedule):
        return "running", "running"
    if schedule.last_error and schedule.last_error != "cancelled":
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


def _parse_site_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        parts = value.replace("\n", ";").replace(",", ";").split(";")
    else:
        parts = list(value)
    seen: dict[str, None] = {}
    for item in parts:
        name = str(item).strip()
        if name:
            seen.setdefault(name, None)
    return list(seen.keys())


def _reject_unsafe_commands(payload: dict[str, Any]) -> None:
    """Block any remote-command field that could harm the ENM management plane.

    Enforced defensively for current and future command-bearing job types
    (CLI, amosbatch, egrep). MDT Transfer carries no command fields, so this is
    a no-op for it.
    """
    command_keys = ("command", "cli_command", "amosbatch_command", "egrep_commands")
    scopes = [payload]
    nested = payload.get("amosbatch") or payload.get("cli")
    if isinstance(nested, dict):
        scopes.append(nested)
    for scope in scopes:
        for key in command_keys:
            value = scope.get(key)
            if isinstance(value, str) and value.strip():
                assert_command_safe(value)


def schedule_from_payload(payload: dict[str, Any]) -> ScheduledJob:
    name = str(payload.get("name") or "").strip()
    if not name:
        raise ValueError("Enter a schedule name.")
    job_type = str(payload.get("job_type") or "mdt_transfer")
    if job_type not in TYPE_LABELS:
        raise ValueError(f"Unsupported job type: {job_type}")

    _reject_unsafe_commands(payload)

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
        site_mode = str(raw_mdt.get("site_mode") or "all").strip().lower()
        sites = _parse_site_list(raw_mdt.get("sites")) if site_mode == "list" else []
        mdt = MdtTransferSettings(
            local_base=str(raw_mdt.get("local_base") or "").strip(),
            remote_bases=[str(item).rstrip("/") for item in remote_bases if str(item).strip()],
            sites=sites,
            initial_lookback_minutes=int(raw_mdt.get("initial_lookback_minutes") or 90),
            grace_minutes=int(raw_mdt.get("grace_minutes") or 30),
            max_parallel_downloads=int(raw_mdt.get("max_parallel_downloads") or 2),
            retry_attempts=max(1, int(raw_mdt.get("retry_attempts") or 5)),
            retry_delay_seconds=max(0, int(raw_mdt.get("retry_delay_seconds") or 20)),
            dry_run=bool(raw_mdt.get("dry_run", True)),
        )
        if not mdt.local_base:
            raise ValueError("Select a local base folder.")
        if not mdt.remote_bases:
            raise ValueError("Add at least one remote CELLTRACE base.")
        if site_mode == "list" and not mdt.sites:
            raise ValueError("Add at least one site, or choose 'Collect all'.")

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


def api_state() -> dict[str, Any]:
    with state_lock:
        return {
            "ok": True,
            "sessions": [session_to_api(session) for session in sessions],
            "schedules": [schedule_to_api(schedule) for schedule in scheduler.list_schedules()],
        }


def api_session_update(payload: dict[str, Any]) -> dict[str, Any]:
    session_id = str(payload.get("id") or "")
    with state_lock:
        session = sessions_by_id().get(session_id)
        if not session:
            raise KeyError("Session not found.")
        new_name = str(payload.get("name") or "").strip()
        if new_name and new_name != session.name:
            if any(
                other.id != session_id and other.name.lower() == new_name.lower()
                for other in sessions
            ):
                raise ValueError(f"A session named '{new_name}' already exists.")
            session.name = new_name
        session.host = str(payload.get("host") or "").strip()
        session.port = int(payload.get("port") or 22)
        session.username = str(payload.get("username") or "").strip()
        if payload.get("clear_password"):
            session.password = ""
        else:
            password = str(payload.get("password") or "")
            if password:
                session.password = password
        session.timeout = int(payload.get("timeout") or 30)
        sessions.sort(key=lambda item: item.name.lower())
        scheduler.set_sessions(sessions)
        save_sessions()
    return {"ok": True, "id": session.id}


def api_add_session(payload: dict[str, Any]) -> dict[str, Any]:
    global sessions
    name = str(payload.get("name") or "").strip()
    if not name:
        raise ValueError("Enter a session name.")
    with state_lock:
        if any(item.id == name or item.name.lower() == name.lower() for item in sessions):
            raise ValueError(f"A session named '{name}' already exists.")
        session = EnmSession(
            id=name,
            name=name,
            host=str(payload.get("host") or "").strip(),
            port=int(payload.get("port") or 5023),
            username=str(payload.get("username") or "").strip(),
            timeout=int(payload.get("timeout") or 30),
            password=str(payload.get("password") or ""),
        )
        sessions.append(session)
        sessions.sort(key=lambda item: item.name.lower())
        scheduler.set_sessions(sessions)
        save_sessions()
    return {"ok": True, "id": session.id}


def api_delete_session(session_id: str) -> dict[str, Any]:
    global sessions
    with state_lock:
        remaining = [item for item in sessions if item.id != session_id]
        if len(remaining) == len(sessions):
            raise KeyError("Session not found.")
        sessions = remaining
        scheduler.set_sessions(sessions)
        save_sessions()
    return {"ok": True}


def api_add_schedule(payload: dict[str, Any]) -> dict[str, Any]:
    schedule = schedule_from_payload(payload)
    schedule_id = scheduler.add_schedule(schedule)
    return {"ok": True, "id": schedule_id}


def api_update_schedule(schedule_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    scheduler.update_schedule(schedule_id, schedule_from_payload(payload))
    return {"ok": True}


def api_delete_schedule(schedule_id: str) -> dict[str, Any]:
    scheduler.remove_schedule(schedule_id)
    return {"ok": True}


def api_start_schedule(schedule_id: str) -> dict[str, Any]:
    scheduler.start_schedule(schedule_id)
    return {"ok": True}


def api_stop_schedule(schedule_id: str) -> dict[str, Any]:
    scheduler.stop_schedule(schedule_id)
    return {
        "ok": True,
        "message": "Schedule stopped. Active run unchanged; use Stop run to cancel it.",
    }


def api_run_now(schedule_id: str) -> dict[str, Any]:
    job_id = scheduler.run_now(schedule_id)
    return {"ok": True, "job_id": job_id}


def api_stop_run(schedule_id: str) -> dict[str, Any]:
    schedule = next(
        (item for item in scheduler.list_schedules() if item.id == schedule_id),
        None,
    )
    if not schedule:
        raise KeyError("Schedule not found.")
    ok, message = scheduler.cancel_job(schedule.last_job_id)
    return {"ok": ok, "message": message}


def api_progress(schedule_id: str) -> dict[str, Any]:
    schedule = next(
        (item for item in scheduler.list_schedules() if item.id == schedule_id),
        None,
    )
    if not schedule:
        raise KeyError("Schedule not found.")
    job = scheduler.get_job(schedule.last_job_id) or {"status": "idle", "lines": []}
    return {
        "ok": True,
        "schedule": schedule_to_api(schedule),
        "job": job,
    }


def api_preview(query: dict[str, list[str]]) -> dict[str, Any]:
    job_type = (query.get("job_type") or ["mdt_transfer"])[0]
    if job_type == "python_script":
        raw_path = (query.get("path") or [""])[0].strip()
        if not raw_path:
            raise ValueError("Select a Python script first.")
        path = Path(raw_path)
        if not path.is_file():
            raise ValueError("The selected script file does not exist.")
        size = path.stat().st_size
        content = path.read_bytes()[:MAX_PREVIEW_BYTES].decode("utf-8", errors="replace")
        return {
            "ok": True,
            "title": path.name,
            "path": str(path),
            "content": content,
            "truncated": size > MAX_PREVIEW_BYTES,
        }
    site_mode = (query.get("site_mode") or ["all"])[0].strip().lower()
    sites = _parse_site_list((query.get("sites") or [""])[0]) if site_mode == "list" else []
    if sites:
        scope = (
            f"Job scope: ONLY the {len(sites)} site(s) listed below.\n"
            "All other folders discovered in the ENM will be ignored.\n"
            + "".join(f"  - {name}\n" for name in sites)
            + "\n"
        )
    else:
        scope = (
            "Job scope: ALL discovered sites ('Collect all' mode).\n"
            "No site list is defined.\n\n"
        )
    source = COLLECTOR_SOURCE.read_text(encoding="utf-8")
    return {
        "ok": True,
        "title": "MDT Transfer - enm_mdt_scheduler/collector.py",
        "path": str(COLLECTOR_SOURCE),
        "content": scope + MDT_PREVIEW_SUMMARY + source,
        "truncated": False,
    }


def api_pick_folder(query: dict[str, list[str]]) -> dict[str, Any]:
    initial = (query.get("initial") or [""])[0].strip()
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception as exc:  # pragma: no cover - headless host
        raise RuntimeError(
            "Native folder picker is not available on this host. Type the path manually."
        ) from exc
    try:
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        try:
            chosen = filedialog.askdirectory(
                initialdir=initial or str(Path.home()),
                title="Select the MDT download folder",
                mustexist=False,
            )
        finally:
            root.destroy()
    except Exception as exc:  # pragma: no cover - no interactive desktop
        raise RuntimeError(
            "Could not open the folder picker on this host. Type the path manually."
        ) from exc
    return {"ok": True, "path": chosen or ""}


def render_index() -> str:
    default_local = html.escape(str(APP_DIR / "MDT_Downloads"), quote=True)
    return INDEX_HTML.replace("{{ default_local }}", default_local)


class SchedulerHttpHandler(BaseHTTPRequestHandler):
    server_version = "ENMScheduler/0.1"

    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path == "/":
            self._send_html(render_index())
            return
        if path == "/api/state":
            self._send_json(api_state())
            return
        if path == "/api/preview":
            query = parse_qs(urlparse(self.path).query)
            self._handle_json(lambda: api_preview(query))
            return
        if path == "/api/pick-folder":
            query = parse_qs(urlparse(self.path).query)
            self._handle_json(lambda: api_pick_folder(query))
            return
        if path.startswith("/api/schedules/") and path.endswith("/progress"):
            schedule_id = path.split("/")[3]
            self._handle_json(lambda: api_progress(schedule_id))
            return
        if path == "/static/amdocs_logo.png":
            if LOGO_PATH.is_file():
                self._send_bytes(LOGO_PATH.read_bytes(), "image/png")
            else:
                self.send_response(HTTPStatus.NOT_FOUND)
                self.end_headers()
            return
        if path == "/favicon.ico":
            self.send_response(HTTPStatus.NO_CONTENT)
            self.end_headers()
            return
        self._send_json({"ok": False, "error": "Not found"}, HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        payload = self._read_json()
        if path == "/api/session":
            self._handle_json(lambda: api_session_update(payload))
            return
        if path == "/api/sessions":
            self._handle_json(lambda: api_add_session(payload))
            return
        if path == "/api/schedules":
            self._handle_json(lambda: api_add_schedule(payload))
            return
        if path.startswith("/api/schedules/"):
            parts = path.strip("/").split("/")
            if len(parts) == 4:
                schedule_id = parts[2]
                action = parts[3]
                actions = {
                    "start": lambda: api_start_schedule(schedule_id),
                    "stop": lambda: api_stop_schedule(schedule_id),
                    "run-now": lambda: api_run_now(schedule_id),
                    "stop-run": lambda: api_stop_run(schedule_id),
                }
                if action in actions:
                    self._handle_json(actions[action])
                    return
        self._send_json({"ok": False, "error": "Not found"}, HTTPStatus.NOT_FOUND)

    def do_PUT(self) -> None:
        path = urlparse(self.path).path
        payload = self._read_json()
        if path.startswith("/api/schedules/"):
            parts = path.strip("/").split("/")
            if len(parts) == 3:
                self._handle_json(lambda: api_update_schedule(parts[2], payload))
                return
        self._send_json({"ok": False, "error": "Not found"}, HTTPStatus.NOT_FOUND)

    def do_DELETE(self) -> None:
        path = urlparse(self.path).path
        if path.startswith("/api/schedules/"):
            parts = path.strip("/").split("/")
            if len(parts) == 3:
                self._handle_json(lambda: api_delete_schedule(parts[2]))
                return
        if path.startswith("/api/sessions/"):
            parts = path.strip("/").split("/")
            if len(parts) == 3:
                self._handle_json(lambda: api_delete_session(unquote(parts[2])))
                return
        self._send_json({"ok": False, "error": "Not found"}, HTTPStatus.NOT_FOUND)

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or "0")
        if length <= 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8")
        data = json.loads(raw) if raw.strip() else {}
        return data if isinstance(data, dict) else {}

    def _handle_json(self, callback) -> None:
        try:
            self._send_json(callback())
        except KeyError as exc:
            self._send_json({"ok": False, "error": str(exc).strip("'")}, HTTPStatus.NOT_FOUND)
        except Exception as exc:  # noqa: BLE001
            self._send_json({"ok": False, "error": str(exc)}, HTTPStatus.BAD_REQUEST)

    def _send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, content: str) -> None:
        body = content.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_bytes(self, body: bytes, content_type: str) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "max-age=86400")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


INDEX_HTML = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>ENM MDT Scheduler</title>
  <style>
    :root {
      --bg: #0b1428;
      --modal: #1a2035;
      --modal-head: #16213e;
      --card: #1a2540;
      --card-2: #0f1830;
      --input: #0e1726;
      --line: #2d3a55;
      --text: #e2e8f0;
      --muted: #718096;
      --label: #a0aec0;
      --link: #7eb8f7;
      --input-text: #c0cce0;
      --primary: #2563eb;
      --primary-hover: #1d4ed8;
      --ok: #48bb78;
      --warn: #f6ad55;
      --err: #fc8181;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: Segoe UI, Arial, sans-serif;
      font-size: 12px;
    }
    main { padding: 14px 18px; max-width: 1340px; margin: 0 auto; }
    .enm-modal-dark {
      background: var(--modal);
      color: var(--text);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: 0 18px 55px rgba(0,0,0,.35);
      overflow: hidden;
      resize: both;
      min-width: 720px;
      min-height: 360px;
      max-width: 97vw;
      max-height: 92vh;
      width: 1140px;
      display: flex;
      flex-direction: column;
    }
    .enm-modal-header {
      background: var(--modal-head);
      border-bottom: 1px solid var(--line);
      padding: 10px 16px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-shrink: 0;
    }
    .brand { display: flex; align-items: center; gap: 12px; }
    .brand-logo {
      height: 22px;
      width: auto;
      display: block;
      border-right: 1px solid var(--line);
      padding-right: 12px;
    }
    .enm-modal-title {
      font-size: 14px;
      font-weight: 600;
      color: var(--text);
    }
    .modal-x {
      color: #a0aec0;
      font-size: 28px;
      line-height: 1;
      font-weight: 200;
    }
    .modal-body { padding: 12px 16px 14px; overflow-y: auto; min-height: 0; flex: 1 1 auto; }
    .tab-bar {
      display: flex;
      gap: 4px;
      border-bottom: 1px solid var(--line);
      margin-bottom: 14px;
    }
    .tab-btn {
      background: transparent;
      border: none;
      border-bottom: 2px solid transparent;
      color: var(--muted, #8fa3bf);
      padding: 8px 16px;
      font-size: 13px;
      font-weight: 600;
      cursor: pointer;
      border-radius: 0;
    }
    .tab-btn:hover { color: var(--link); }
    .tab-btn.active {
      color: var(--link);
      border-bottom-color: var(--link);
    }
    .enm-section {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 12px;
      margin-bottom: 14px;
    }
    .enm-section-title {
      font-size: 12px;
      font-weight: 600;
      color: var(--link);
      margin-bottom: 10px;
    }
    label {
      color: var(--label);
      font-size: 12px;
      white-space: nowrap;
    }
    .help-label {
      cursor: help;
      text-decoration: underline dotted;
      text-underline-offset: 3px;
    }
    input, select, button {
      font: inherit;
      border-radius: 3px;
      border: 1px solid var(--line);
    }
    input, select {
      background: var(--input);
      color: var(--input-text);
      padding: 4px 8px;
      outline: none;
    }
    input:disabled, select:disabled {
      opacity: .55;
    }
    input::selection {
      background: var(--primary);
      color: #ffffff;
    }
    button {
      cursor: pointer;
      background: #e5e7eb;
      color: #374151;
      border: 1px solid #9ca3af;
      padding: 3px 9px;
      font-size: 11px;
      border-radius: 2px;
      white-space: nowrap;
    }
    button:hover { background: #d1d5db; }
    button:disabled {
      opacity: .55;
      cursor: default;
    }
    .enm-btn-primary {
      background: var(--primary) !important;
      border-color: var(--primary) !important;
      color: #ffffff !important;
    }
    .enm-btn-primary:hover { background: var(--primary-hover) !important; }
    .enm-btn-danger { background: #2a1420; border-color: #7f1d1d; color: #fecaca; }
    .row { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
    .grow { flex: 1 1 220px; }
    .short { width: 78px; }
    .medium { width: 160px; }
    .long { width: min(100%, 680px); }
    .schedule-list {
      margin-bottom: 14px;
      max-height: 210px;
      overflow-y: auto;
      border: 1px solid var(--line);
      border-radius: 4px;
      background: #16213a;
    }
    .enm-sched-row {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-bottom: 1px solid #1e2a40;
      font-size: 12px;
      cursor: default;
    }
    .enm-sched-row:last-child { border-bottom: none; }
    .enm-sched-row.selected { background: #243b63; }
    .enm-sched-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: #4a5568;
      flex-shrink: 0;
    }
    .enm-sched-dot.active { background: var(--ok); box-shadow: 0 0 6px var(--ok); }
    .enm-sched-dot.running { background: var(--warn); box-shadow: 0 0 6px var(--warn); }
    .enm-sched-dot.error { background: var(--err); box-shadow: 0 0 6px var(--err); }
    .enm-sched-name { color: var(--text); font-weight: 600; white-space: nowrap; }
    .enm-sched-meta { color: var(--muted); font-size: 11px; flex: 1; min-width: 260px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .enm-sched-type { color: var(--link); }
    .enm-sched-actions { display: flex; gap: 4px; flex-shrink: 0; }
    .progress { display: none; }
    pre {
      height: 220px;
      overflow: auto;
      margin: 8px 0 0;
      padding: 10px;
      background: #060e1c;
      color: #c0cce0;
      border: 1px solid var(--line);
      font-family: Consolas, monospace;
      font-size: 11px;
      line-height: 1.45;
      white-space: pre-wrap;
    }
    .grid {
      display: grid;
      grid-template-columns: auto minmax(240px, 1fr) auto auto auto auto;
      gap: 8px;
      align-items: center;
    }
    .full { grid-column: 1 / -1; }
    .hidden { display: none !important; }
    .note { color: var(--muted); margin-left: 8px; font-size: 11px; }
    .error { color: var(--err); font-weight: 700; }
    @media (max-width: 900px) {
      .grid { grid-template-columns: 1fr; }
      .long, .medium, .short { width: 100%; }
      .enm-modal-dark { min-width: 0; width: 100%; }
      .enm-sched-row { align-items: flex-start; flex-direction: column; }
      .enm-sched-actions { flex-wrap: wrap; }
    }
  </style>
</head>
<body>
<main>
  <section class="enm-modal-dark">
    <div class="enm-modal-header">
      <div class="brand">
        <img class="brand-logo" src="/static/amdocs_logo.png" alt="Amdocs">
        <span class="enm-modal-title">ENM Script Scheduler</span>
      </div>
      <span class="modal-x">x</span>
    </div>
    <div class="modal-body">
      <div class="tab-bar">
        <button id="tabCreateBtn" type="button" class="tab-btn active" onclick="switchTab('create')">Create scheduler</button>
        <button id="tabSessionsBtn" type="button" class="tab-btn" onclick="switchTab('sessions')">Manage sessions</button>
      </div>

      <div id="tabSessions" class="tab-pane hidden">
      <div class="enm-section">
        <div class="enm-section-title">ENM Sessions</div>
        <div class="row">
          <label>Session</label>
          <select id="sessionSelect" class="medium"></select>
          <label>Name</label>
          <input id="sessionName" class="medium" placeholder="ENMBARB">
          <label>Host</label>
          <input id="sessionHost" class="medium" placeholder="10.x.x.x">
          <label>Port</label>
          <input id="sessionPort" class="short" type="number">
          <label>User</label>
          <input id="sessionUser" class="medium">
          <label>Password</label>
          <input id="sessionPassword" class="medium" type="password" autocomplete="current-password">
          <label>Timeout</label>
          <input id="sessionTimeout" class="short" type="number">
          <button class="enm-btn-primary" onclick="saveSession()">Save changes</button>
          <button onclick="addSession()">Add new</button>
          <button class="enm-btn-danger" onclick="removeSession()">Remove</button>
          <span id="sessionMsg" class="note"></span>
        </div>
      </div>
      </div>

      <div id="tabCreate" class="tab-pane">
      <div id="scheduleRows" class="schedule-list"></div>
      <div id="jobMsg" class="note" style="margin-bottom:10px;"></div>

      <div id="progressPanel" class="enm-section progress">
        <div class="row" style="justify-content:space-between;">
          <div id="progressTitle" class="enm-section-title" style="margin-bottom:0;">Progress</div>
          <button onclick="hideProgress()">Close</button>
        </div>
        <pre id="progressText"></pre>
      </div>

      <div class="enm-section">
        <div id="formTitle" class="enm-section-title">Add New Schedule</div>
        <div class="grid">
          <label>Name</label>
          <input id="name" class="grow" placeholder="MDT BARB hourly">
          <label class="help-label" title="MDT Transfer uses the built-in ENM collector; Script runs a selected local Python file.">Type</label>
          <select id="jobType" class="medium" onchange="typeChanged()">
            <option value="mdt_transfer">MDT Transfer</option>
            <option value="python_script">Script (.py)</option>
          </select>
          <label class="help-label" title="ENM connection used by this schedule. Connections are configured in Manage sessions.">Session</label>
          <select id="jobSession" class="medium"></select>

          <div id="scriptFields" class="full hidden row">
            <label>Script</label>
            <input id="scriptPath" class="long" placeholder="C:\path\script.py">
          </div>

          <div id="mdtFields" class="full">
            <div class="row">
              <label class="help-label" title="Local folder where MDT files are saved, organized as DDMMYYYY/site.">Download folder</label>
              <input id="localBase" class="long" value="{{ default_local }}" title="Destination folder for downloaded MDT files.">
              <button type="button" onclick="pickFolder()">Browse...</button>
              <span class="note">files saved as: folder/DDMMYYYY/&lt;site&gt;/</span>
            </div>
            <div class="row" style="margin-top:8px">
              <label class="help-label" title="CELLTRACE directories searched in the ENM. Separate multiple paths with semicolons.">Remote bases</label>
              <input id="remoteBases" class="long" value="/ericsson/pmic1/CELLTRACE;/ericsson/pmic2/CELLTRACE" title="Remote ENM directories searched for MDT files.">
            </div>
            <div class="row" style="margin-top:8px">
              <label class="help-label" title="Choose whether to collect every discovered ENM site or only the sites entered in Site list.">Sites</label>
              <select id="siteMode" class="medium" onchange="siteModeChanged()">
                <option value="all">Collect all (auto-discover)</option>
                <option value="list" selected>From site list</option>
              </select>
              <button type="button" id="loadSitesBtn" class="hidden" onclick="document.getElementById('sitesFile').click()">Load .txt</button>
              <input type="file" id="sitesFile" accept=".txt,.csv" class="hidden" onchange="onSitesFile(event)">
              <span id="sitesCount" class="note"></span>
            </div>
            <div id="sitesRow" class="row hidden" style="margin-top:6px">
              <label class="help-label" title="Sites to collect. Names are matched without distinguishing uppercase and lowercase.">Site list</label>
              <input id="sites" class="long" placeholder="MAAP2_MG, MASC2_MG, ... (comma, semicolon or one site per line)" oninput="updateSitesCount()">
            </div>

            <div class="row" style="margin-top:8px">
              <input id="lookback" type="hidden" value="90">
              <label class="help-label" title="On the first successful collection, the job downloads every MDT file still available in the ENM for the selected sites.">Initial scan</label>
              <span class="note" title="The first collection is not limited by file age.">All available</span>
              <label class="help-label" title="Safety overlap: each later scan starts this many minutes before the previous checkpoint. Already downloaded files are not duplicated.">Grace min</label>
              <input id="grace" class="short" type="number" value="30" title="Minutes revisited before the previous scan checkpoint.">
              <label class="help-label" title="Maximum number of MDT files downloaded simultaneously. Higher values use more ENM, network and disk resources.">Parallel</label>
              <input id="parallel" class="short" type="number" value="2" title="Maximum simultaneous downloads.">
              <label class="help-label" title="Maximum connection/download attempts after a temporary failure.">Retries</label>
              <input id="retryAttempts" class="short" type="number" min="1" value="5" title="Number of attempts for temporary failures.">
              <label class="help-label" title="Time in seconds to wait between retry attempts.">Retry delay sec</label>
              <input id="retryDelay" class="short" type="number" min="0" value="20" title="Seconds between retry attempts.">
              <label class="help-label" title="Scans and lists candidate files without downloading them or saving the collection checkpoint."><input id="dryRun" type="checkbox"> Dry run (scan only)</label>
            </div>
          </div>

          <div class="full" style="margin-top:6px">
            <div class="row" style="justify-content:space-between;">
              <div class="row">
                <button type="button" id="previewBtn" onclick="previewCode()" title="Shows the collection logic used by this job.">Preview code</button>
                <button type="button" id="previewHideBtn" class="hidden" onclick="hidePreview()">Hide</button>
              </div>
              <span id="previewMsg" class="note"></span>
            </div>
            <pre id="previewBox" class="hidden"></pre>
          </div>

          <div class="full row" style="margin-top:4px">
            <label class="help-label" title="Normal interval, in minutes, between scheduled collection cycles.">Run every</label>
            <input id="interval" class="short" type="number" value="60" title="Minutes between normal runs.">
            <span>min</span>
            <label class="help-label" title="Optional short interval for testing. Keep 0 in production to use Run every.">Test sec</label>
            <input id="testSeconds" class="short" type="number" value="0" title="Test interval in seconds; 0 disables test mode.">
            <label class="help-label" title="Restricts executions to the period between From and To."><input id="timeWindow" type="checkbox" onchange="windowChanged()"> Time window</label>
            <span>From</span>
            <input id="startDate" class="medium" type="date">
            <input id="startHour" class="short" type="number" min="0" max="23" title="hour (0-23)">
            <span>:</span>
            <input id="startMinute" class="short" type="number" min="0" max="59" title="minute (0-59)">
            <span>To</span>
            <input id="endDate" class="medium" type="date">
            <input id="endHour" class="short" type="number" min="0" max="23" title="hour (0-23)">
            <span>:</span>
            <input id="endMinute" class="short" type="number" min="0" max="59" title="minute (0-59)">
            <label class="help-label" title="Enables the schedule as soon as it is created. If unchecked, it is saved stopped."><input id="enabled" type="checkbox"> Start immediately</label>
          </div>

          <div class="full row">
            <button id="submitBtn" class="enm-btn-primary" onclick="submitSchedule()">Add Schedule</button>
            <button id="cancelEditBtn" class="hidden" onclick="cancelEdit()">Cancel edit</button>
            <span id="formMsg" class="note"></span>
          </div>
        </div>
      </div>
      </div>
    </div>
  </section>
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

function localDate(d) {
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
}

function initDates() {
  const now = new Date();
  const later = new Date(now.getTime() + 60 * 60 * 1000);
  $('startDate').value = localDate(now);
  $('endDate').value = localDate(later);
  $('startHour').value = now.getHours();
  $('startMinute').value = now.getMinutes();
  $('endHour').value = later.getHours();
  $('endMinute').value = later.getMinutes();
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
  $('sessionName').value = session.name || '';
  $('sessionHost').value = session.host || '';
  $('sessionPort').value = session.port || 22;
  $('sessionUser').value = session.username || '';
  $('sessionTimeout').value = session.timeout || 30;
  $('sessionPassword').value = '';
  $('sessionPassword').placeholder = session.has_password ? 'password set' : '';
  renderedSessionId = session.id;
}

function renderSchedules() {
  const list = $('scheduleRows');
  list.innerHTML = '';
  if (!state.schedules.length) {
    list.innerHTML = '<div style="padding:10px;color:#718096;font-size:12px;">No schedules configured.</div>';
    return;
  }
  for (const schedule of state.schedules) {
    const row = document.createElement('div');
    row.className = 'enm-sched-row';
    row.dataset.id = schedule.id;
    if (schedule.id === selectedScheduleId) row.classList.add('selected');
    row.onclick = () => {
      selectedScheduleId = schedule.id;
      progressScheduleId = progressScheduleId || schedule.id;
      renderSchedules();
    };
    const dotClass = schedule.status_tag === 'active' ? 'active' : schedule.status_tag;
    const running = schedule.status === 'running';
    row.innerHTML = `
      <span class="enm-sched-dot ${escapeAttr(dotClass)}"></span>
      <span class="enm-sched-name">${escapeHtml(schedule.name)}</span>
      <span class="enm-sched-meta" title="${escapeAttr(schedule.last_next)}">
        <span class="enm-sched-type">${escapeHtml(schedule.type_label)}</span> |
        ${running ? '<span style="color:#f6ad55;">running...</span> |' : ''}
        ${escapeHtml(schedule.session_name)} |
        Every ${escapeHtml(schedule.every)}
        ${schedule.window && schedule.window !== 'always' ? ` | ${escapeHtml(schedule.window.replace(' -> ', ' -> '))}` : ''}
        ${schedule.last_next && schedule.last_next !== '-' ? ` | ${escapeHtml(schedule.last_next)}` : ''}
      </span>
      <div class="enm-sched-actions" onclick="event.stopPropagation()">
        <button onclick="showProgressFor('${escapeAttr(schedule.id)}')">Progress</button>
        ${running ? `<button class="enm-btn-danger" onclick="scheduleActionFor('${escapeAttr(schedule.id)}','stop-run')">Stop run</button>` : ''}
        ${schedule.enabled
          ? `<button onclick="scheduleActionFor('${escapeAttr(schedule.id)}','stop')">Stop schedule</button>`
          : `<button class="enm-btn-primary" onclick="scheduleActionFor('${escapeAttr(schedule.id)}','start')">Start</button>`}
        <button onclick="scheduleActionFor('${escapeAttr(schedule.id)}','run-now')">Run Now</button>
        <button onclick="editSchedule('${escapeAttr(schedule.id)}')">Edit</button>
        <button class="enm-btn-danger" onclick="deleteSchedule('${escapeAttr(schedule.id)}')">x</button>
      </div>
    `;
    list.appendChild(row);
  }
  if (selectedScheduleId && !state.schedules.some(item => item.id === selectedScheduleId)) {
    selectedScheduleId = null;
  }
}

async function saveSession() {
  const id = $('sessionSelect').value;
  if (!id) return setMsg('sessionMsg', 'Select a session first, or use "Add new".', true);
  const name = ($('sessionName').value || '').trim();
  if (!name) return setMsg('sessionMsg', 'Enter a session name.', true);
  try {
    await api('/api/session', {
      method: 'POST',
      body: JSON.stringify({
        id,
        name,
        host: $('sessionHost').value,
        port: Number($('sessionPort').value || 22),
        username: $('sessionUser').value,
        password: $('sessionPassword').value,
        timeout: Number($('sessionTimeout').value || 30)
      })
    });
    setMsg('sessionMsg', 'Session saved. Password is encrypted for this Windows user.');
    renderedSessionId = null;
    await refreshState();
    $('sessionSelect').value = id;
    fillSessionForm();
  } catch (err) {
    setMsg('sessionMsg', err.message, true);
  }
}

async function addSession() {
  const name = ($('sessionName').value || '').trim();
  if (!name) return setMsg('sessionMsg', 'Enter a session name to add.', true);
  try {
    const result = await api('/api/sessions', {
      method: 'POST',
      body: JSON.stringify({
        name,
        host: $('sessionHost').value,
        port: Number($('sessionPort').value || 5023),
        username: $('sessionUser').value,
        password: $('sessionPassword').value,
        timeout: Number($('sessionTimeout').value || 30)
      })
    });
    setMsg('sessionMsg', `Session '${name}' added. Password is encrypted for this Windows user.`);
    renderedSessionId = null;
    await refreshState();
    $('sessionSelect').value = result.id || name;
    fillSessionForm();
  } catch (err) {
    setMsg('sessionMsg', err.message, true);
  }
}

async function removeSession() {
  const id = $('sessionSelect').value;
  if (!id) return setMsg('sessionMsg', 'Select a session first.', true);
  const session = state.sessions.find(item => item.id === id);
  if (!confirm(`Remove session '${session?.name || id}'?`)) return;
  try {
    await api(`/api/sessions/${encodeURIComponent(id)}`, {method: 'DELETE'});
    setMsg('sessionMsg', 'Session removed.');
    renderedSessionId = null;
    await refreshState();
  } catch (err) {
    setMsg('sessionMsg', err.message, true);
  }
}

async function scheduleAction(action) {
  if (!selectedScheduleId) return setMsg('jobMsg', 'Select a schedule first.', true);
  return scheduleActionFor(selectedScheduleId, action);
}

async function scheduleActionFor(scheduleId, action) {
  selectedScheduleId = scheduleId;
  try {
    const result = await api(`/api/schedules/${scheduleId}/${action}`, {method: 'POST'});
    setMsg('jobMsg', result.message || 'OK');
    if (action === 'run-now') {
      showProgressFor(scheduleId);
    }
    await refreshState();
  } catch (err) {
    setMsg('jobMsg', err.message, true);
  }
}

async function deleteSelected() {
  if (!selectedScheduleId) return setMsg('jobMsg', 'Select a schedule first.', true);
  return deleteSchedule(selectedScheduleId);
}

async function deleteSchedule(scheduleId) {
  selectedScheduleId = scheduleId;
  const schedule = state.schedules.find(item => item.id === scheduleId);
  if (!confirm(`Remove schedule '${schedule?.name || scheduleId}'?`)) return;
  try {
    await api(`/api/schedules/${scheduleId}`, {method: 'DELETE'});
    selectedScheduleId = null;
    await refreshState();
  } catch (err) {
    setMsg('jobMsg', err.message, true);
  }
}

function editSelected() {
  if (!selectedScheduleId) return setMsg('jobMsg', 'Select a schedule first.', true);
  return editSchedule(selectedScheduleId);
}

function editSchedule(scheduleId) {
  selectedScheduleId = scheduleId;
  const schedule = state.schedules.find(item => item.id === scheduleId);
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
  const mdtSites = schedule.mdt?.sites || [];
  $('siteMode').value = mdtSites.length ? 'list' : 'all';
  $('sites').value = mdtSites.join(';');
  siteModeChanged();
  $('lookback').value = schedule.mdt?.initial_lookback_minutes || 90;
  $('grace').value = schedule.mdt?.grace_minutes || 30;
  $('parallel').value = schedule.mdt?.max_parallel_downloads || 2;
  $('retryAttempts').value = schedule.mdt?.retry_attempts || 5;
  $('retryDelay').value = schedule.mdt?.retry_delay_seconds || 20;
  $('dryRun').checked = !!schedule.mdt?.dry_run;
  $('timeWindow').checked = !!(schedule.start_time && schedule.end_time);
  if (schedule.start_time) setDateParts('start', schedule.start_time);
  if (schedule.end_time) setDateParts('end', schedule.end_time);
  typeChanged();
  windowChanged();
  $('name').scrollIntoView({behavior: 'smooth', block: 'nearest'});
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
  $(`${prefix}Hour`).value = Number(hour);
  $(`${prefix}Minute`).value = Number(minute);
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
      site_mode: $('siteMode').value,
      sites: $('siteMode').value === 'list' ? $('sites').value : '',
      initial_lookback_minutes: Number($('lookback').value || 90),
      grace_minutes: Number($('grace').value || 30),
      max_parallel_downloads: Number($('parallel').value || 2),
      retry_attempts: Number($('retryAttempts').value || 5),
      retry_delay_seconds: Number($('retryDelay').value || 20),
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
  showProgressFor(selectedScheduleId);
}

function showProgressFor(scheduleId) {
  selectedScheduleId = scheduleId;
  progressScheduleId = scheduleId;
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

function siteModeChanged() {
  const isList = $('siteMode').value === 'list';
  $('sitesRow').classList.toggle('hidden', !isList);
  $('loadSitesBtn').classList.toggle('hidden', !isList);
  updateSitesCount();
}

function updateSitesCount() {
  const raw = ($('sites').value || '').split(/[;,\r\n]+/).map(s => s.trim()).filter(Boolean);
  $('sitesCount').textContent = $('siteMode').value === 'list' ? `${raw.length} site(s)` : '';
}

function onSitesFile(event) {
  const file = event.target.files && event.target.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = () => {
    const names = String(reader.result || '').split(/[\r\n;,]+/).map(s => s.trim()).filter(Boolean);
    $('sites').value = names.join(';');
    updateSitesCount();
    setMsg('formMsg', `Loaded ${names.length} site(s) from ${file.name}.`);
  };
  reader.readAsText(file);
  event.target.value = '';
}

function typeChanged() {
  const isMdt = $('jobType').value === 'mdt_transfer';
  $('mdtFields').classList.toggle('hidden', !isMdt);
  $('scriptFields').classList.toggle('hidden', isMdt);
  $('previewBtn').textContent = isMdt ? 'Preview MDT logic' : 'Preview script';
  hidePreview();
}

async function pickFolder() {
  setMsg('formMsg', 'Opening folder picker on this machine...');
  try {
    const data = await api('/api/pick-folder?initial=' + encodeURIComponent($('localBase').value || ''));
    if (data.path) {
      $('localBase').value = data.path;
      setMsg('formMsg', 'Download folder selected.');
    } else {
      setMsg('formMsg', 'No folder selected.');
    }
  } catch (err) {
    setMsg('formMsg', 'Browse unavailable: ' + err.message, true);
  }
}

async function previewCode() {
  const jobType = $('jobType').value;
  const params = new URLSearchParams({job_type: jobType});
  if (jobType === 'python_script') {
    const scriptPath = ($('scriptPath').value || '').trim();
    if (!scriptPath) return setMsg('previewMsg', 'Select a Python script first.', true);
    params.set('path', scriptPath);
  } else {
    params.set('site_mode', $('siteMode').value);
    if ($('siteMode').value === 'list') params.set('sites', $('sites').value || '');
  }
  setMsg('previewMsg', 'Loading...');
  try {
    const data = await api('/api/preview?' + params.toString());
    const box = $('previewBox');
    box.textContent = (data.content || '') + (data.truncated ? '\n\n... (truncated preview) ...' : '');
    box.classList.remove('hidden');
    $('previewHideBtn').classList.remove('hidden');
    setMsg('previewMsg', data.title || 'Preview');
  } catch (err) {
    setMsg('previewMsg', err.message, true);
  }
}

function hidePreview() {
  $('previewBox').classList.add('hidden');
  $('previewBox').textContent = '';
  $('previewHideBtn').classList.add('hidden');
  setMsg('previewMsg', '');
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

function switchTab(tab) {
  const isSessions = tab === 'sessions';
  $('tabSessions').classList.toggle('hidden', !isSessions);
  $('tabCreate').classList.toggle('hidden', isSessions);
  $('tabSessionsBtn').classList.toggle('active', isSessions);
  $('tabCreateBtn').classList.toggle('active', !isSessions);
}

$('sessionSelect').addEventListener('change', () => {
  renderedSessionId = null;
  fillSessionForm();
});
switchTab('create');
initDates();
typeChanged();
siteModeChanged();
refreshState();
setInterval(refreshState, 2000);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    port = int(os.environ.get("PORT") or DEFAULT_PORT)
    server = ThreadingHTTPServer(("127.0.0.1", port), SchedulerHttpHandler)
    print(f"ENM MDT Scheduler running at http://127.0.0.1:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        scheduler.shutdown()
        server.server_close()
