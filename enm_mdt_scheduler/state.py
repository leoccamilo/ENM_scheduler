from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class DownloadRecord:
    remote_path: str
    local_path: str
    site: str
    size: int
    mtime: int
    downloaded_at: str

    @staticmethod
    def from_dict(remote_path: str, data: dict[str, Any]) -> "DownloadRecord":
        return DownloadRecord(
            remote_path=remote_path,
            local_path=str(data.get("local_path") or ""),
            site=str(data.get("site") or ""),
            size=int(data.get("size") or 0),
            mtime=int(data.get("mtime") or 0),
            downloaded_at=str(data.get("downloaded_at") or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "local_path": self.local_path,
            "site": self.site,
            "size": self.size,
            "mtime": self.mtime,
            "downloaded_at": self.downloaded_at,
        }


@dataclass
class DownloadState:
    last_scan_epoch: float = 0.0
    downloaded: dict[str, DownloadRecord] = field(default_factory=dict)

    @staticmethod
    def load(path: str | os.PathLike[str]) -> "DownloadState":
        state_path = Path(path)
        if not state_path.exists():
            return DownloadState()
        raw = json.loads(state_path.read_text(encoding="utf-8"))
        downloaded: dict[str, DownloadRecord] = {}
        for remote_path, item in (raw.get("downloaded") or {}).items():
            if isinstance(item, dict):
                downloaded[str(remote_path)] = DownloadRecord.from_dict(str(remote_path), item)
        return DownloadState(
            last_scan_epoch=float(raw.get("last_scan_epoch") or 0.0),
            downloaded=downloaded,
        )

    def save(self, path: str | os.PathLike[str]) -> None:
        state_path = Path(path)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_scan_epoch": self.last_scan_epoch,
            "saved_at_epoch": time.time(),
            "downloaded": {
                remote_path: record.to_dict()
                for remote_path, record in sorted(self.downloaded.items())
            },
        }
        tmp_path = state_path.with_suffix(state_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        os.replace(tmp_path, state_path)

    def has_same_file(self, remote_path: str, size: int, mtime: int) -> bool:
        record = self.downloaded.get(remote_path)
        return bool(record and record.size == size and record.mtime == mtime)

    def mark_downloaded(
        self,
        *,
        remote_path: str,
        local_path: str,
        site: str,
        size: int,
        mtime: int,
        downloaded_at: str,
    ) -> None:
        self.downloaded[remote_path] = DownloadRecord(
            remote_path=remote_path,
            local_path=local_path,
            site=site,
            size=size,
            mtime=mtime,
            downloaded_at=downloaded_at,
        )

