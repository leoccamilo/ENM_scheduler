from __future__ import annotations

import os
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

from .collector import DEFAULT_REMOTE_BASES


DEFAULT_LOCAL_BASE = str(Path(__file__).resolve().parents[1] / "MDT_Downloads")


@dataclass
class EnmSession:
    id: str
    name: str
    host: str = ""
    port: int = 22
    username: str = ""
    timeout: int = 30
    password: str = ""

    def to_dict(self, include_password: bool = False) -> dict[str, Any]:
        data = asdict(self)
        if not include_password:
            data.pop("password", None)
        return data

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "EnmSession":
        raw = dict(data or {})
        name = str(raw.get("name") or raw.get("id") or "ENM")
        return EnmSession(
            id=str(raw.get("id") or name),
            name=name,
            host=str(raw.get("host") or ""),
            port=int(raw.get("port") or 22),
            username=str(raw.get("username") or ""),
            timeout=int(raw.get("timeout") or 30),
            password=str(raw.get("password") or ""),
        )


@dataclass
class MdtTransferSettings:
    local_base: str = DEFAULT_LOCAL_BASE
    remote_bases: list[str] = field(default_factory=lambda: list(DEFAULT_REMOTE_BASES))
    initial_lookback_minutes: int = 90
    grace_minutes: int = 30
    max_parallel_downloads: int = 2
    dry_run: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(data: Optional[dict[str, Any]]) -> "MdtTransferSettings":
        raw = dict(data or {})
        remote_bases = raw.get("remote_bases") or list(DEFAULT_REMOTE_BASES)
        if isinstance(remote_bases, str):
            remote_bases = [
                item.strip()
                for item in remote_bases.replace("\n", ";").split(";")
                if item.strip()
            ]
        return MdtTransferSettings(
            local_base=str(raw.get("local_base") or DEFAULT_LOCAL_BASE),
            remote_bases=[str(item).rstrip("/") for item in remote_bases if str(item).strip()],
            initial_lookback_minutes=int(raw.get("initial_lookback_minutes") or 90),
            grace_minutes=int(raw.get("grace_minutes") or 30),
            max_parallel_downloads=int(raw.get("max_parallel_downloads") or 2),
            dry_run=bool(raw.get("dry_run", True)),
        )


@dataclass
class ScheduledJob:
    name: str = ""
    job_type: str = "mdt_transfer"
    session_id: Optional[str] = None
    script_path: str = ""
    interval_minutes: int = 60
    test_interval_seconds: int = 0
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    enabled: bool = False
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    mdt: MdtTransferSettings = field(default_factory=MdtTransferSettings)
    last_run: Optional[str] = None
    next_run: Optional[str] = None
    last_job_id: Optional[str] = None
    last_error: Optional[str] = None
    is_running: bool = False

    def effective_interval_seconds(self) -> int:
        if self.test_interval_seconds > 0:
            return max(1, int(self.test_interval_seconds))
        return max(1, int(self.interval_minutes)) * 60

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["mdt"] = self.mdt.to_dict()
        return data

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "ScheduledJob":
        raw = dict(data or {})
        script_path = str(raw.get("script_path") or "")
        name = str(raw.get("name") or os.path.basename(script_path) or "Schedule")
        return ScheduledJob(
            id=str(raw.get("id") or uuid.uuid4()),
            name=name,
            job_type=str(raw.get("job_type") or "mdt_transfer"),
            session_id=(
                str(raw.get("session_id"))
                if raw.get("session_id") not in (None, "")
                else None
            ),
            script_path=script_path,
            interval_minutes=max(1, int(raw.get("interval_minutes") or 60)),
            test_interval_seconds=max(0, int(raw.get("test_interval_seconds") or 0)),
            start_time=_optional_str(raw.get("start_time")),
            end_time=_optional_str(raw.get("end_time")),
            enabled=bool(raw.get("enabled", False)),
            mdt=MdtTransferSettings.from_dict(raw.get("mdt") or {}),
            last_run=_optional_str(raw.get("last_run")),
            next_run=_optional_str(raw.get("next_run")),
            last_job_id=_optional_str(raw.get("last_job_id")),
            last_error=_optional_str(raw.get("last_error")),
            is_running=bool(raw.get("is_running", False)),
        )


def _optional_str(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    return str(value)
