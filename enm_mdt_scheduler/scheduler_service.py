from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from .collector import CollectionCancelled, CollectionConfig, EnmMdtCollector, SshConfig
from .models import EnmSession, ScheduledJob


ChangeCallback = Callable[[], None]
JobCallback = Callable[[str], None]


class SchedulerService:
    def __init__(
        self,
        schedules_path: Path,
        sessions: list[EnmSession],
        on_changed: Optional[ChangeCallback] = None,
        on_job_updated: Optional[JobCallback] = None,
    ) -> None:
        self.schedules_path = schedules_path
        self.sessions: dict[str, EnmSession] = {session.id: session for session in sessions}
        self.on_changed = on_changed or (lambda: None)
        self.on_job_updated = on_job_updated or (lambda _job_id: None)
        self.schedules: dict[str, ScheduledJob] = {}
        self.jobs: dict[str, dict] = {}
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._loop_thread: threading.Thread | None = None
        self._load_schedules()

    def set_sessions(self, sessions: list[EnmSession]) -> None:
        with self._lock:
            self.sessions = {session.id: session for session in sessions}

    def start_loop(self) -> None:
        if self._loop_thread and self._loop_thread.is_alive():
            return
        self._stop_event.clear()
        self._loop_thread = threading.Thread(target=self._loop, daemon=True)
        self._loop_thread.start()

    def shutdown(self) -> None:
        self._stop_event.set()
        if self._loop_thread and self._loop_thread.is_alive():
            self._loop_thread.join(timeout=2)

    def list_schedules(self) -> list[ScheduledJob]:
        with self._lock:
            return list(self.schedules.values())

    def add_schedule(self, schedule: ScheduledJob) -> str:
        with self._lock:
            schedule.id = schedule.id or str(uuid.uuid4())
            schedule.last_error = None
            schedule.is_running = False
            if schedule.enabled:
                schedule.next_run = self._format_dt(self._compute_next_run(schedule))
            self.schedules[schedule.id] = schedule
            self._save_schedules_locked()
        self.on_changed()
        return schedule.id

    def update_schedule(self, schedule_id: str, schedule: ScheduledJob) -> None:
        with self._lock:
            current = self.schedules.get(schedule_id)
            if not current:
                raise KeyError("Schedule not found")
            schedule.id = schedule_id
            schedule.enabled = current.enabled or schedule.enabled
            schedule.last_run = current.last_run
            schedule.last_job_id = current.last_job_id
            schedule.last_error = None
            schedule.is_running = current.is_running
            schedule.next_run = (
                self._format_dt(self._compute_next_run(schedule)) if schedule.enabled else None
            )
            self.schedules[schedule_id] = schedule
            self._save_schedules_locked()
        self.on_changed()

    def remove_schedule(self, schedule_id: str) -> None:
        with self._lock:
            if schedule_id not in self.schedules:
                return
            del self.schedules[schedule_id]
            self._save_schedules_locked()
        self.on_changed()

    def start_schedule(self, schedule_id: str) -> None:
        with self._lock:
            schedule = self.schedules.get(schedule_id)
            if not schedule:
                return
            schedule.enabled = True
            schedule.last_error = None
            schedule.next_run = self._format_dt(self._compute_next_run(schedule))
            self._save_schedules_locked()
        self.on_changed()

    def stop_schedule(self, schedule_id: str) -> None:
        with self._lock:
            schedule = self.schedules.get(schedule_id)
            if not schedule:
                return
            schedule.enabled = False
            schedule.next_run = None
            self._save_schedules_locked()
        self.on_changed()

    def run_now(self, schedule_id: str) -> str:
        with self._lock:
            schedule = self.schedules.get(schedule_id)
            if not schedule:
                raise KeyError("Schedule not found")
            if self.is_schedule_running(schedule):
                raise RuntimeError("This schedule already has a run in progress.")
            return self._start_job_locked(schedule)

    def cancel_job(self, job_id: str | None) -> tuple[bool, str]:
        if not job_id:
            return False, "No run in progress for this schedule."
        with self._lock:
            job = self.jobs.get(job_id)
            if not job or job.get("status") != "running":
                return False, "No run in progress for this schedule."
            process = job.get("process")
            collector = job.get("collector")
            cancel_event = job.get("cancel_event")
            job["cancel_requested"] = True
            if isinstance(cancel_event, threading.Event):
                cancel_event.set()
        if collector is not None:
            try:
                collector.cancel()
            except Exception:
                pass
            self._append_job_lines(job_id, ["[cancel] MDT Transfer cancellation requested."])
            return True, "MDT Transfer cancellation requested."
        if process is None:
            self._append_job_lines(job_id, ["[cancel] Cancellation requested."])
            return True, "Cancellation requested."
        try:
            process.terminate()
            self._append_job_lines(job_id, ["[cancel] Process termination requested."])
            return True, "Process termination requested."
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    def get_job(self, job_id: str | None) -> Optional[dict]:
        if not job_id:
            return None
        with self._lock:
            job = self.jobs.get(job_id)
            if not job:
                return None
            return {
                "status": job.get("status"),
                "lines": list(job.get("lines") or []),
                "schedule_id": job.get("schedule_id"),
                "cancel_requested": bool(job.get("cancel_requested")),
            }

    def is_schedule_running(self, schedule: ScheduledJob) -> bool:
        job = self.get_job(schedule.last_job_id)
        return bool(job and job.get("status") == "running")

    def _load_schedules(self) -> None:
        if not self.schedules_path.exists():
            return
        try:
            raw = json.loads(self.schedules_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict) and isinstance(raw.get("schedules"), list):
                items = raw["schedules"]
            elif isinstance(raw, dict):
                items = list(raw.values())
            elif isinstance(raw, list):
                items = raw
            else:
                items = []
            with self._lock:
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    schedule = ScheduledJob.from_dict(item)
                    schedule.is_running = False
                    self.schedules[schedule.id] = schedule
        except Exception:
            return

    def _save_schedules_locked(self) -> None:
        self.schedules_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            schedule_id: schedule.to_dict()
            for schedule_id, schedule in self.schedules.items()
        }
        tmp_file = self.schedules_path.with_suffix(".json.tmp")
        tmp_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        os.replace(tmp_file, self.schedules_path)

    def _loop(self) -> None:
        while not self._stop_event.wait(1):
            now = datetime.now()
            due: list[str] = []
            changed = False
            with self._lock:
                for schedule_id, schedule in self.schedules.items():
                    if not schedule.enabled:
                        continue
                    allowed, expired = self._within_window(schedule, now)
                    if expired:
                        schedule.enabled = False
                        schedule.next_run = None
                        schedule.last_error = f"Time window ended at {schedule.end_time}"
                        changed = True
                        continue
                    if not allowed:
                        if not schedule.next_run:
                            schedule.next_run = self._format_dt(self._compute_next_run(schedule))
                            changed = True
                        continue
                    if not schedule.next_run:
                        schedule.next_run = self._format_dt(self._compute_next_run(schedule))
                        changed = True
                    next_dt = self._parse_datetime(schedule.next_run)
                    if next_dt and now >= next_dt:
                        due.append(schedule_id)
                if changed:
                    self._save_schedules_locked()
            if changed:
                self.on_changed()
            for schedule_id in due:
                self._fire_schedule(schedule_id)

    def _fire_schedule(self, schedule_id: str) -> None:
        with self._lock:
            schedule = self.schedules.get(schedule_id)
            if not schedule or not schedule.enabled:
                return
            allowed, expired = self._within_window(schedule, datetime.now())
            if expired:
                schedule.enabled = False
                schedule.next_run = None
                schedule.last_error = f"Time window ended at {schedule.end_time}"
                self._save_schedules_locked()
                self.on_changed()
                return
            if not allowed:
                schedule.next_run = self._format_dt(self._compute_next_run(schedule))
                self._save_schedules_locked()
                self.on_changed()
                return
            if self.is_schedule_running(schedule):
                skipped_at = datetime.now().strftime("%H:%M:%S")
                self._append_job_lines(
                    schedule.last_job_id,
                    [
                        f"[schedule] Cycle {skipped_at} skipped because "
                        "the previous execution is still running."
                    ],
                )
                schedule.next_run = self._format_dt(self._compute_next_run(schedule))
                self._save_schedules_locked()
                self.on_changed()
                return
            self._start_job_locked(schedule)
            schedule.next_run = self._format_dt(self._compute_next_run(schedule))
            self._save_schedules_locked()
        self.on_changed()

    def _start_job_locked(self, schedule: ScheduledJob) -> str:
        snapshot = ScheduledJob.from_dict(schedule.to_dict())
        session = self.sessions.get(snapshot.session_id or "")
        session_snapshot = (
            EnmSession.from_dict(session.to_dict(include_password=True)) if session else None
        )
        job_id = str(uuid.uuid4())
        self.jobs[job_id] = {
            "status": "running",
            "lines": [],
            "schedule_id": schedule.id,
            "cancel_requested": False,
            "cancel_event": threading.Event(),
            "collector": None,
            "process": None,
        }
        schedule.last_run = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        schedule.last_job_id = job_id
        schedule.last_error = None
        schedule.is_running = True
        self._save_schedules_locked()
        target = self._run_mdt_transfer if snapshot.job_type == "mdt_transfer" else self._run_python
        threading.Thread(
            target=target,
            args=(job_id, snapshot, session_snapshot),
            daemon=True,
        ).start()
        self.on_changed()
        self.on_job_updated(job_id)
        return job_id

    def _run_mdt_transfer(
        self,
        job_id: str,
        schedule: ScheduledJob,
        session: EnmSession | None,
    ) -> None:
        collector = None
        try:
            if session is None:
                raise ValueError("Select an ENM session for MDT Transfer.")
            with self._lock:
                cancel_event = self.jobs.get(job_id, {}).get("cancel_event")
            if not isinstance(cancel_event, threading.Event):
                cancel_event = threading.Event()
            missing = [
                field
                for field, value in (
                    ("host", session.host),
                    ("username", session.username),
                    ("password", session.password),
                )
                if not value
            ]
            if missing:
                raise ValueError(
                    f"Session '{session.name}' is missing: " + ", ".join(missing)
                )
            cfg = CollectionConfig(
                ssh=SshConfig(
                    host=session.host,
                    port=session.port,
                    username=session.username,
                    password=session.password,
                    timeout=session.timeout,
                ),
                local_base=schedule.mdt.local_base,
                remote_bases=tuple(schedule.mdt.remote_bases),
                sites=tuple(schedule.mdt.sites),
                initial_lookback_minutes=schedule.mdt.initial_lookback_minutes,
                grace_minutes=schedule.mdt.grace_minutes,
                max_parallel_downloads=schedule.mdt.max_parallel_downloads,
                retry_attempts=schedule.mdt.retry_attempts,
                retry_delay_seconds=schedule.mdt.retry_delay_seconds,
                dry_run=schedule.mdt.dry_run,
            )
            mode = "dry-run" if cfg.dry_run else "download"
            site_info = (
                f"{len(cfg.sites)} site(s) from job list" if cfg.sites else "all discovered sites"
            )
            self._append_job_lines(
                job_id,
                [
                    f"[mdt] Starting MDT Transfer ({mode})",
                    f"[mdt] Session {session.name} {session.username}@{session.host}:{session.port}",
                    f"[mdt] Scope: {site_info}",
                    f"[mdt] Retry: {cfg.retry_attempts} attempt(s), {cfg.retry_delay_seconds}s delay",
                ],
            )
            collector = EnmMdtCollector(
                cfg,
                log=lambda line: self._append_job_lines(job_id, [line]),
                cancel_requested=cancel_event.is_set,
            )
            with self._lock:
                job = self.jobs.get(job_id)
                if job:
                    job["collector"] = collector
            result = collector.collect_once()
            if self._job_cancel_requested(job_id):
                raise CollectionCancelled("MDT Transfer cancelled")
            self._append_job_lines(
                job_id,
                [
                    "[mdt] Finished: "
                    f"scanned={result.scanned_files}, "
                    f"candidates={result.candidates}, "
                    f"downloaded={result.downloaded}, "
                    f"known={result.skipped_known}, "
                    f"existing={result.skipped_existing}, "
                    f"failed={result.failed}"
                ],
            )
            self._set_job_status(job_id, "done" if result.failed == 0 else "error")
        except CollectionCancelled:
            self._append_job_lines(job_id, ["[cancel] MDT Transfer cancelled."])
            self._set_job_status(job_id, "cancelled")
        except Exception as exc:  # noqa: BLE001
            self._append_job_lines(
                job_id,
                [f"[ERROR] {exc}", *traceback.format_exc().splitlines()],
            )
            self._set_job_status(job_id, "error")
        finally:
            if collector is not None:
                with self._lock:
                    job = self.jobs.get(job_id)
                    if job and job.get("collector") is collector:
                        job["collector"] = None

    def _run_python(
        self,
        job_id: str,
        schedule: ScheduledJob,
        session: EnmSession | None,
    ) -> None:
        try:
            script_path = Path(schedule.script_path)
            if not script_path.is_file():
                raise FileNotFoundError(str(script_path))
            env = os.environ.copy()
            if session:
                env.update(
                    {
                        "ENM_SESSION_NAME": session.name,
                        "ENM_HOST": session.host,
                        "ENM_PORT": str(session.port),
                        "ENM_USERNAME": session.username,
                        "ENM_PASSWORD": session.password,
                    }
                )
            process = subprocess.Popen(  # noqa: S603
                [sys.executable, str(script_path)],
                cwd=str(script_path.parent),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
            )
            with self._lock:
                job = self.jobs.get(job_id)
                if job:
                    job["process"] = process
            if process.stdout:
                for line in process.stdout:
                    self._append_job_lines(job_id, [line.rstrip("\n")])
            return_code = process.wait()
            if return_code == 0:
                self._set_job_status(job_id, "done")
            elif self._job_cancel_requested(job_id):
                self._set_job_status(job_id, "cancelled")
            else:
                self._append_job_lines(job_id, [f"[ERROR] Process exited {return_code}"])
                self._set_job_status(job_id, "error")
        except Exception as exc:  # noqa: BLE001
            self._append_job_lines(
                job_id,
                [f"[ERROR] {exc}", *traceback.format_exc().splitlines()],
            )
            self._set_job_status(job_id, "error")

    def _append_job_lines(self, job_id: str, lines: list[str]) -> None:
        if not lines:
            return
        stamped = [
            f"{datetime.now().strftime('%H:%M:%S')} {line}" if line else ""
            for line in lines
        ]
        with self._lock:
            job = self.jobs.get(job_id)
            if not job:
                return
            job.setdefault("lines", []).extend(stamped)
        self.on_job_updated(job_id)

    def _set_job_status(self, job_id: str, status: str) -> None:
        with self._lock:
            job = self.jobs.get(job_id)
            if not job:
                return
            job["status"] = status
            schedule_id = job.get("schedule_id")
            lines = list(job.get("lines") or [])
            if schedule_id in self.schedules:
                schedule = self.schedules[schedule_id]
                schedule.is_running = False
                schedule.last_error = None if status == "done" else self._job_error_summary(status, lines)
                self._save_schedules_locked()
        self.on_changed()
        self.on_job_updated(job_id)

    def _job_cancel_requested(self, job_id: str) -> bool:
        with self._lock:
            return bool(self.jobs.get(job_id, {}).get("cancel_requested"))

    @staticmethod
    def _job_error_summary(status: str, lines: list[str]) -> str:
        if status == "cancelled":
            return "cancelled"
        for line in reversed(lines):
            marker = "[ERROR]"
            if marker in line:
                return line.split(marker, 1)[1].strip() or status
        return status

    def _compute_next_run(self, schedule: ScheduledJob) -> datetime:
        now = datetime.now()
        start = self._parse_datetime(schedule.start_time)
        if start and now < start:
            return start
        return datetime.fromtimestamp(time.time() + schedule.effective_interval_seconds())

    def _within_window(
        self,
        schedule: ScheduledJob,
        now: datetime,
    ) -> tuple[bool, bool]:
        start = self._parse_datetime(schedule.start_time)
        end = self._parse_datetime(schedule.end_time)
        if start and now < start:
            return False, False
        if end and now > end:
            return False, True
        return True, False

    @staticmethod
    def _parse_datetime(value: str | None) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            try:
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None

    @staticmethod
    def _format_dt(value: datetime) -> str:
        return value.strftime("%Y-%m-%d %H:%M:%S")
