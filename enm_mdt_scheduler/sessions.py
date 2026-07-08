from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

from .models import EnmSession


FALLBACK_ENM_NAMES = ("ENMBARA", "ENMBARB", "ENMCTPA", "ENMCTPB")


def manager_sessions_db() -> Path:
    return Path.home() / ".securecrt_manager" / "sessions.db"


def load_manager_sessions(db_path: Path | None = None) -> list[EnmSession]:
    path = db_path or manager_sessions_db()
    if not path.exists():
        return []

    conn = sqlite3.connect(str(path))
    try:
        rows = conn.execute(
            """
            SELECT id, name, host, port, username, timeout
            FROM sessions
            ORDER BY name
            """
        ).fetchall()
    finally:
        conn.close()

    sessions = [
        EnmSession(
            id=str(name),
            name=str(name),
            host=str(host or ""),
            port=int(port or 22),
            username=str(username or ""),
            timeout=int(timeout or 30),
        )
        for _sid, name, host, port, username, timeout in rows
        if name
    ]
    enm_sessions = [session for session in sessions if session.name.upper().startswith("ENM")]
    return enm_sessions or sessions


def fallback_sessions() -> list[EnmSession]:
    return [EnmSession(id=name, name=name, port=5023, timeout=10) for name in FALLBACK_ENM_NAMES]


def merge_sessions(
    imported: Iterable[EnmSession],
    saved: Iterable[EnmSession],
) -> list[EnmSession]:
    merged: dict[str, EnmSession] = {session.id: session for session in imported}
    for session in saved:
        current = merged.get(session.id)
        if current:
            session.password = current.password or session.password
        merged[session.id] = session
    return sorted(merged.values(), key=lambda item: item.name.lower())
