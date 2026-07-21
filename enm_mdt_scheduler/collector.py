from __future__ import annotations

import errno
import fnmatch
import os
import re
import socket
import stat
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from .state import DownloadState


try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]
    ZoneInfoNotFoundError = Exception  # type: ignore[assignment]


LogCallback = Callable[[str], None]
CancelCallback = Callable[[], bool]

DEFAULT_REMOTE_BASES = (
    "/ericsson/pmic1/CELLTRACE",
    "/ericsson/pmic2/CELLTRACE",
)

DEFAULT_PATTERNS = ("*.bin.gz", "*.gpb.gz")
SITE_RE = re.compile(r"(?:MeContext|ManagedElement)=([^,/]+)", re.IGNORECASE)
INVALID_WIN_CHARS_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


class CollectionCancelled(RuntimeError):
    pass


def _import_paramiko():
    try:
        import paramiko  # type: ignore

        return paramiko
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("paramiko is not installed. Run: pip install -r requirements.txt") from exc


def _tz_brazil():
    if ZoneInfo is None:
        return timezone(timedelta(hours=-3))
    try:
        return ZoneInfo("America/Sao_Paulo")
    except ZoneInfoNotFoundError:
        return timezone(timedelta(hours=-3))


def now_brazil() -> datetime:
    return datetime.now(_tz_brazil())


def day_folder_name(now: Optional[datetime] = None) -> str:
    return (now or now_brazil()).strftime("%d%m%Y")


def sanitize_folder_name(value: str) -> str:
    cleaned = INVALID_WIN_CHARS_RE.sub("_", value.strip())
    cleaned = cleaned.strip(" .")
    return cleaned or "UNKNOWN_SITE"


def extract_site_from_path(path: str) -> str:
    match = SITE_RE.search(path)
    if match:
        return match.group(1)
    parent = path.rstrip("/").split("/")[-2:-1]
    return parent[0] if parent else "UNKNOWN_SITE"


def join_remote(directory: str, name: str) -> str:
    return directory.rstrip("/") + "/" + name


def is_target_file(name: str, patterns: Iterable[str] = DEFAULT_PATTERNS) -> bool:
    lower = name.lower()
    return any(fnmatch.fnmatch(lower, pattern.lower()) for pattern in patterns)


@dataclass(frozen=True)
class SshConfig:
    host: str
    port: int
    username: str
    password: str
    timeout: int = 30


@dataclass(frozen=True)
class RemoteLogFile:
    remote_path: str
    filename: str
    site: str
    size: int
    mtime: int


@dataclass
class CollectionConfig:
    ssh: SshConfig
    local_base: str
    remote_bases: tuple[str, ...] = DEFAULT_REMOTE_BASES
    file_patterns: tuple[str, ...] = DEFAULT_PATTERNS
    sites: tuple[str, ...] = ()
    initial_lookback_minutes: int = 90
    grace_minutes: int = 30
    max_parallel_downloads: int = 2
    retry_attempts: int = 5
    retry_delay_seconds: int = 20
    dry_run: bool = False

    def state_path(self) -> Path:
        return Path(self.local_base) / "_state" / "downloaded_files.json"

    def allowed_sites(self) -> set[str]:
        return {site.strip().lower() for site in self.sites if site.strip()}


@dataclass
class CollectionResult:
    scanned_files: int = 0
    candidates: int = 0
    skipped_known: int = 0
    skipped_existing: int = 0
    downloaded: int = 0
    failed: int = 0
    oldest_failed_mtime: int = 0
    errors: list[str] = field(default_factory=list)


class EnmMdtCollector:
    def __init__(
        self,
        config: CollectionConfig,
        log: Optional[LogCallback] = None,
        cancel_requested: Optional[CancelCallback] = None,
    ) -> None:
        self.config = config
        self.log = log or (lambda _line: None)
        self.cancel_requested = cancel_requested or (lambda: False)
        self._cancel_event = threading.Event()
        self._active_lock = threading.RLock()
        self._active_clients: list[Any] = []
        self._active_sftps: list[Any] = []

    def _emit(self, line: str) -> None:
        self.log(line)

    def cancel(self) -> None:
        self._cancel_event.set()
        with self._active_lock:
            sftps = list(self._active_sftps)
            clients = list(self._active_clients)
        for sftp in sftps:
            try:
                sftp.close()
            except Exception:
                pass
        for client in clients:
            try:
                client.close()
            except Exception:
                pass

    def _is_cancel_requested(self) -> bool:
        return self._cancel_event.is_set() or bool(self.cancel_requested())

    def _raise_if_cancelled(self) -> None:
        if self._is_cancel_requested():
            raise CollectionCancelled("MDT Transfer cancelled")

    def _sleep_or_cancel(self, seconds: int) -> None:
        deadline = time.time() + max(0, seconds)
        while time.time() < deadline:
            self._raise_if_cancelled()
            time.sleep(max(0.0, min(0.5, deadline - time.time())))
        self._raise_if_cancelled()

    def _register_client(self, client: Any) -> None:
        with self._active_lock:
            self._active_clients.append(client)

    def _release_client(self, client: Any) -> None:
        with self._active_lock:
            if client in self._active_clients:
                self._active_clients.remove(client)
        try:
            client.close()
        except Exception:
            pass

    def _register_sftp(self, sftp: Any) -> None:
        with self._active_lock:
            self._active_sftps.append(sftp)

    def _release_sftp(self, sftp: Any) -> None:
        with self._active_lock:
            if sftp in self._active_sftps:
                self._active_sftps.remove(sftp)
        try:
            sftp.close()
        except Exception:
            pass

    def _connect(self):
        self._raise_if_cancelled()
        paramiko = _import_paramiko()
        cfg = self.config.ssh
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(
                cfg.host,
                port=cfg.port,
                username=cfg.username,
                password=cfg.password,
                timeout=cfg.timeout,
                banner_timeout=cfg.timeout,
                auth_timeout=cfg.timeout,
                look_for_keys=False,
                allow_agent=False,
            )
        except Exception:
            try:
                client.close()
            except Exception:
                pass
            raise
        self._register_client(client)
        return client

    def _open_sftp(self, client: Any):
        self._raise_if_cancelled()
        sftp = client.open_sftp()
        try:
            sftp.get_channel().settimeout(max(1, int(self.config.ssh.timeout)))
        except Exception:
            pass
        self._register_sftp(sftp)
        return sftp

    def _with_retries(self, label: str, operation: Callable[[], Any]) -> Any:
        attempts = max(1, int(self.config.retry_attempts))
        delay = max(0, int(self.config.retry_delay_seconds))
        for attempt in range(1, attempts + 1):
            self._raise_if_cancelled()
            try:
                result = operation()
            except CollectionCancelled:
                raise
            except Exception as exc:
                if attempt >= attempts or not self._is_transient_error(exc):
                    raise
                self._emit(
                    f"[retry] {label}: connection lost ({exc}); "
                    f"retrying in {delay}s ({attempt}/{attempts})"
                )
                self._sleep_or_cancel(delay)
                continue
            if attempt > 1:
                self._emit(f"[retry] {label}: reconnected, resuming")
            return result
        raise RuntimeError(f"{label} failed")

    @staticmethod
    def _is_transient_error(exc: Exception) -> bool:
        try:
            paramiko = _import_paramiko()
        except RuntimeError:
            paramiko = None

        if paramiko is not None:
            auth_errors = tuple(
                item
                for item in (
                    getattr(paramiko, "AuthenticationException", None),
                    getattr(paramiko, "BadAuthenticationType", None),
                    getattr(paramiko, "PartialAuthentication", None),
                )
                if item is not None
            )
            if auth_errors and isinstance(exc, auth_errors):
                return False
            ssh_exception = getattr(paramiko, "SSHException", None)
            if ssh_exception is not None and isinstance(exc, ssh_exception):
                return True

        if isinstance(exc, (EOFError, TimeoutError, ConnectionError, socket.timeout)):
            return True
        if isinstance(exc, OSError):
            if isinstance(exc, (FileNotFoundError, PermissionError)):
                return False
            transient_errnos = {
                value
                for value in (
                    getattr(errno, "ECONNABORTED", None),
                    getattr(errno, "ECONNREFUSED", None),
                    getattr(errno, "ECONNRESET", None),
                    getattr(errno, "EHOSTDOWN", None),
                    getattr(errno, "EHOSTUNREACH", None),
                    getattr(errno, "ENETDOWN", None),
                    getattr(errno, "ENETRESET", None),
                    getattr(errno, "ENETUNREACH", None),
                    getattr(errno, "EPIPE", None),
                    getattr(errno, "ETIMEDOUT", None),
                )
                if value is not None
            }
            if exc.errno in transient_errnos:
                return True
        message = str(exc).lower()
        return any(
            marker in message
            for marker in (
                "connection reset",
                "connection aborted",
                "connection lost",
                "connection refused",
                "server connection dropped",
                "socket is closed",
                "timed out",
                "timeout",
                "eof",
                "no existing session",
                "error reading ssh protocol banner",
            )
        )

    def scan(self, min_mtime: Optional[float]) -> list[RemoteLogFile]:
        return self._with_retries("scan", lambda: self._scan_once(min_mtime))

    def _scan_once(self, min_mtime: Optional[float]) -> list[RemoteLogFile]:
        client = self._connect()
        sftp = None
        found: list[RemoteLogFile] = []
        try:
            sftp = self._open_sftp(client)
            allowed = self.config.allowed_sites()
            if allowed:
                self._emit(f"[scan] Site filter ON: {len(allowed)} site(s) from job list")
            else:
                self._emit("[scan] Site filter OFF: collecting all discovered sites")
            for base in self.config.remote_bases:
                self._raise_if_cancelled()
                self._emit(f"[scan] Listing {base}")
                try:
                    site_entries = sftp.listdir_attr(base)
                except OSError as exc:
                    if self._is_transient_error(exc):
                        raise
                    self._emit(f"[scan] Cannot list {base}: {exc}")
                    continue
                for site_entry in site_entries:
                    self._raise_if_cancelled()
                    if not stat.S_ISDIR(site_entry.st_mode or 0):
                        continue
                    site_dir = join_remote(base, site_entry.filename)
                    site = extract_site_from_path(site_dir)
                    if allowed and site.strip().lower() not in allowed:
                        continue
                    try:
                        file_entries = sftp.listdir_attr(site_dir)
                    except OSError as exc:
                        if self._is_transient_error(exc):
                            raise
                        self._emit(f"[scan] Cannot list {site_dir}: {exc}")
                        continue
                    for file_entry in file_entries:
                        self._raise_if_cancelled()
                        if stat.S_ISDIR(file_entry.st_mode or 0):
                            continue
                        if not is_target_file(file_entry.filename, self.config.file_patterns):
                            continue
                        mtime = int(file_entry.st_mtime or 0)
                        if min_mtime is not None and mtime < min_mtime:
                            continue
                        remote_path = join_remote(site_dir, file_entry.filename)
                        found.append(
                            RemoteLogFile(
                                remote_path=remote_path,
                                filename=file_entry.filename,
                                site=site,
                                size=int(file_entry.st_size or 0),
                                mtime=mtime,
                            )
                        )
        finally:
            if sftp is not None:
                self._release_sftp(sftp)
            self._release_client(client)
        found.sort(key=lambda item: (item.mtime, item.remote_path))
        return found

    def local_path_for(self, item: RemoteLogFile, collected_at: datetime) -> Path:
        return (
            Path(self.config.local_base)
            / day_folder_name(collected_at)
            / sanitize_folder_name(item.site)
            / item.filename
        )

    def _download_one(self, item: RemoteLogFile, local_path: Path) -> None:
        self._with_retries(
            f"download {item.site}/{item.filename}",
            lambda: self._download_one_attempt(item, local_path),
        )

    def _download_one_attempt(self, item: RemoteLogFile, local_path: Path) -> None:
        self._raise_if_cancelled()
        local_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = local_path.with_name(local_path.name + ".part")
        client = self._connect()
        sftp = None
        try:
            sftp = self._open_sftp(client)
            self._raise_if_cancelled()
            sftp.get(item.remote_path, str(tmp_path))
            self._raise_if_cancelled()
            os.replace(tmp_path, local_path)
        finally:
            if sftp is not None:
                self._release_sftp(sftp)
            self._release_client(client)
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass

    def collect_once(self) -> CollectionResult:
        self._raise_if_cancelled()
        state_path = self.config.state_path()
        state = DownloadState.load(state_path)
        result = CollectionResult()
        scan_started = time.time()
        collected_at = now_brazil()

        if state.last_scan_epoch:
            min_mtime = max(0.0, state.last_scan_epoch - self.config.grace_minutes * 60)
            self._emit(
                "[state] Incremental scan from "
                f"{datetime.fromtimestamp(min_mtime).strftime('%Y-%m-%d %H:%M:%S')}"
            )
        else:
            # ENM keeps CELLTRACE/MDT logs only for a limited time. On a job's
            # first successful collection, take everything still available so
            # an arbitrary lookback window cannot discard logs before the
            # scheduler has had a chance to preserve them locally.
            min_mtime = 0.0
            self._emit("[state] First scan: collecting all available files")

        files = self.scan(min_mtime=min_mtime)
        self._raise_if_cancelled()
        result.scanned_files = len(files)
        self._emit(f"[scan] Found {len(files)} recent file(s)")

        tasks: list[tuple[RemoteLogFile, Path]] = []
        for item in files:
            self._raise_if_cancelled()
            result.candidates += 1
            local_path = self.local_path_for(item, collected_at)
            if state.has_same_file(item.remote_path, item.size, item.mtime):
                result.skipped_known += 1
                continue
            if local_path.exists():
                result.skipped_existing += 1
                state.mark_downloaded(
                    remote_path=item.remote_path,
                    local_path=str(local_path),
                    site=item.site,
                    size=item.size,
                    mtime=item.mtime,
                    downloaded_at=collected_at.isoformat(timespec="seconds"),
                )
                continue
            tasks.append((item, local_path))

        if not tasks:
            self._emit("[download] No new files to download")
        elif self.config.dry_run:
            self._emit(f"[dry-run] Would download {len(tasks)} new file(s)")
            for item, local_path in tasks:
                self._raise_if_cancelled()
                self._emit(f"[dry-run] {item.remote_path} -> {local_path}")
        else:
            self._emit(f"[download] Downloading {len(tasks)} new file(s)")
            workers = max(1, int(self.config.max_parallel_downloads))
            executor = ThreadPoolExecutor(max_workers=workers)
            future_map = {}
            try:
                for item, local_path in tasks:
                    self._raise_if_cancelled()
                    future_map[executor.submit(self._download_one, item, local_path)] = (
                        item,
                        local_path,
                    )
                pending = set(future_map)
                while pending:
                    self._raise_if_cancelled()
                    done, pending = wait(pending, timeout=0.5, return_when=FIRST_COMPLETED)
                    for future in done:
                        item, local_path = future_map[future]
                        try:
                            future.result()
                        except CollectionCancelled:
                            raise
                        except Exception as exc:  # noqa: BLE001
                            result.failed += 1
                            if (
                                not result.oldest_failed_mtime
                                or item.mtime < result.oldest_failed_mtime
                            ):
                                result.oldest_failed_mtime = item.mtime
                            message = f"[ERROR] {item.remote_path}: {exc}"
                            result.errors.append(message)
                            self._emit(message)
                            continue
                        result.downloaded += 1
                        state.mark_downloaded(
                            remote_path=item.remote_path,
                            local_path=str(local_path),
                            site=item.site,
                            size=item.size,
                            mtime=item.mtime,
                            downloaded_at=collected_at.isoformat(timespec="seconds"),
                        )
                        self._emit(f"[download] OK {item.site}: {item.filename}")
            except CollectionCancelled:
                for future in future_map:
                    future.cancel()
                raise
            finally:
                executor.shutdown(wait=True, cancel_futures=True)

        if self.config.dry_run:
            self._emit("[dry-run] State not saved")
        else:
            self._raise_if_cancelled()
            if result.failed:
                if result.oldest_failed_mtime:
                    resume_epoch = float(result.oldest_failed_mtime)
                    state.last_scan_epoch = (
                        min(state.last_scan_epoch, resume_epoch)
                        if state.last_scan_epoch
                        else resume_epoch
                    )
                    resume_from = max(
                        0.0,
                        state.last_scan_epoch - self.config.grace_minutes * 60,
                    )
                    self._emit(
                        "[state] Partial failures detected; next scan will resume from "
                        f"{datetime.fromtimestamp(resume_from).strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                else:
                    self._emit("[state] Partial failures detected; checkpoint not advanced")
            else:
                state.last_scan_epoch = scan_started
            state.save(state_path)
            self._emit(f"[state] Saved {state_path}")
        return result
