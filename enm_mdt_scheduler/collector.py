from __future__ import annotations

import fnmatch
import os
import re
import stat
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional

from .state import DownloadState


try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]
    ZoneInfoNotFoundError = Exception  # type: ignore[assignment]


LogCallback = Callable[[str], None]

DEFAULT_REMOTE_BASES = (
    "/ericsson/pmic1/CELLTRACE",
    "/ericsson/pmic2/CELLTRACE",
)

DEFAULT_PATTERNS = ("*.bin.gz", "*.gpb.gz")
SITE_RE = re.compile(r"(?:MeContext|ManagedElement)=([^,/]+)", re.IGNORECASE)
INVALID_WIN_CHARS_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


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
    initial_lookback_minutes: int = 90
    grace_minutes: int = 30
    max_parallel_downloads: int = 2
    dry_run: bool = False

    def state_path(self) -> Path:
        return Path(self.local_base) / "_state" / "downloaded_files.json"


@dataclass
class CollectionResult:
    scanned_files: int = 0
    candidates: int = 0
    skipped_known: int = 0
    skipped_existing: int = 0
    downloaded: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)


class EnmMdtCollector:
    def __init__(self, config: CollectionConfig, log: Optional[LogCallback] = None) -> None:
        self.config = config
        self.log = log or (lambda _line: None)

    def _emit(self, line: str) -> None:
        self.log(line)

    def _connect(self):
        paramiko = _import_paramiko()
        cfg = self.config.ssh
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
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
        return client

    def scan(self, min_mtime: Optional[float]) -> list[RemoteLogFile]:
        client = self._connect()
        sftp = client.open_sftp()
        found: list[RemoteLogFile] = []
        try:
            for base in self.config.remote_bases:
                self._emit(f"[scan] Listing {base}")
                try:
                    site_entries = sftp.listdir_attr(base)
                except OSError as exc:
                    self._emit(f"[scan] Cannot list {base}: {exc}")
                    continue
                for site_entry in site_entries:
                    if not stat.S_ISDIR(site_entry.st_mode or 0):
                        continue
                    site_dir = join_remote(base, site_entry.filename)
                    site = extract_site_from_path(site_dir)
                    try:
                        file_entries = sftp.listdir_attr(site_dir)
                    except OSError as exc:
                        self._emit(f"[scan] Cannot list {site_dir}: {exc}")
                        continue
                    for file_entry in file_entries:
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
            try:
                sftp.close()
            finally:
                client.close()
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
        local_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = local_path.with_name(local_path.name + ".part")
        client = self._connect()
        sftp = client.open_sftp()
        try:
            sftp.get(item.remote_path, str(tmp_path))
            os.replace(tmp_path, local_path)
        finally:
            try:
                sftp.close()
            finally:
                client.close()
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass

    def collect_once(self) -> CollectionResult:
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
            min_mtime = scan_started - self.config.initial_lookback_minutes * 60
            self._emit(
                "[state] First scan: looking back "
                f"{self.config.initial_lookback_minutes} minute(s)"
            )

        files = self.scan(min_mtime=min_mtime)
        result.scanned_files = len(files)
        self._emit(f"[scan] Found {len(files)} recent file(s)")

        tasks: list[tuple[RemoteLogFile, Path]] = []
        for item in files:
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
                self._emit(f"[dry-run] {item.remote_path} -> {local_path}")
        else:
            self._emit(f"[download] Downloading {len(tasks)} new file(s)")
            workers = max(1, int(self.config.max_parallel_downloads))
            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_map = {
                    executor.submit(self._download_one, item, local_path): (item, local_path)
                    for item, local_path in tasks
                }
                for future in as_completed(future_map):
                    item, local_path = future_map[future]
                    try:
                        future.result()
                    except Exception as exc:  # noqa: BLE001
                        result.failed += 1
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

        if self.config.dry_run:
            self._emit("[dry-run] State not saved")
        else:
            state.last_scan_epoch = scan_started
            state.save(state_path)
            self._emit(f"[state] Saved {state_path}")
        return result
