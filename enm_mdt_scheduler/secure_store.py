from __future__ import annotations

import base64
import ctypes
import json
import os
from pathlib import Path
from typing import Mapping


_ENTROPY = b"ENM_MDT_SCHEDULER/session-password/v1"
_CRYPTPROTECT_UI_FORBIDDEN = 0x01


class SecureStoreError(RuntimeError):
    pass


class _DATA_BLOB(ctypes.Structure):
    _fields_ = [
        ("cbData", ctypes.c_ulong),
        ("pbData", ctypes.POINTER(ctypes.c_char)),
    ]


def _blob_from_bytes(data: bytes) -> tuple[_DATA_BLOB, ctypes.Array[ctypes.c_char]]:
    buffer = ctypes.create_string_buffer(data)
    return _DATA_BLOB(len(data), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_char))), buffer


def _require_windows() -> None:
    if os.name != "nt":
        raise SecureStoreError("Windows DPAPI secure storage is only available on Windows.")


def _protect(data: bytes) -> bytes:
    _require_windows()
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    in_blob, in_buffer = _blob_from_bytes(data)
    entropy_blob, entropy_buffer = _blob_from_bytes(_ENTROPY)
    out_blob = _DATA_BLOB()
    ok = crypt32.CryptProtectData(
        ctypes.byref(in_blob),
        None,
        ctypes.byref(entropy_blob),
        None,
        None,
        _CRYPTPROTECT_UI_FORBIDDEN,
        ctypes.byref(out_blob),
    )
    # Keep buffers alive until CryptProtectData returns.
    _ = (in_buffer, entropy_buffer)
    if not ok:
        raise ctypes.WinError()
    try:
        return ctypes.string_at(out_blob.pbData, out_blob.cbData)
    finally:
        kernel32.LocalFree(out_blob.pbData)


def _unprotect(data: bytes) -> bytes:
    _require_windows()
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    in_blob, in_buffer = _blob_from_bytes(data)
    entropy_blob, entropy_buffer = _blob_from_bytes(_ENTROPY)
    out_blob = _DATA_BLOB()
    ok = crypt32.CryptUnprotectData(
        ctypes.byref(in_blob),
        None,
        ctypes.byref(entropy_blob),
        None,
        None,
        _CRYPTPROTECT_UI_FORBIDDEN,
        ctypes.byref(out_blob),
    )
    _ = (in_buffer, entropy_buffer)
    if not ok:
        raise ctypes.WinError()
    try:
        return ctypes.string_at(out_blob.pbData, out_blob.cbData)
    finally:
        kernel32.LocalFree(out_blob.pbData)


class SecureStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load_passwords(self) -> dict[str, str]:
        if not self.path.exists():
            return {}
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        sessions = raw.get("sessions") if isinstance(raw, dict) else None
        if not isinstance(sessions, dict):
            return {}
        passwords: dict[str, str] = {}
        for session_id, item in sessions.items():
            if not isinstance(item, dict):
                continue
            token = item.get("password")
            if not token:
                continue
            try:
                encrypted = base64.b64decode(str(token).encode("ascii"))
                passwords[str(session_id)] = _unprotect(encrypted).decode("utf-8")
            except Exception:
                continue
        return passwords

    def save_passwords(self, passwords: Mapping[str, str]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "provider": "windows-dpapi-current-user",
            "sessions": {},
        }
        for session_id, password in sorted(passwords.items()):
            if not password:
                continue
            encrypted = _protect(str(password).encode("utf-8"))
            payload["sessions"][str(session_id)] = {
                "password": base64.b64encode(encrypted).decode("ascii"),
            }
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        os.replace(tmp_path, self.path)
