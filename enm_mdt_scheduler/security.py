"""Safety guard for any command that could be sent to the ENM.

The scheduler must never forward commands that delete network elements or that
can damage the OSS/ENM management plane. Every remote-command job type (CLI,
amosbatch, ...) MUST route its command(s) through :func:`assert_command_safe`
before execution.

MDT Transfer jobs do not use this guard because they only perform read-only
SFTP operations (list + download); they never execute remote shell/MO commands.
"""
from __future__ import annotations

import re
from typing import Optional


# First-token verbs that delete/destroy or can harm the OS / ENM management.
_BLOCKED_VERBS = {
    # filesystem destruction
    "rm", "rmdir", "del", "delete", "erase", "unlink", "shred", "srm",
    "dd", "mkfs", "format", "fdisk", "parted", "truncate", "wipefs", "mv",
    # power / service / process control
    "shutdown", "reboot", "poweroff", "halt", "init",
    "systemctl", "service", "kill", "killall", "pkill",
    # ENM / moshell MO-destructive or service-affecting
    "rdel", "deletemo", "bl", "deb", "acc", "uv",
}

# Whole-command patterns that are dangerous regardless of position.
_BLOCKED_PATTERNS = [
    (re.compile(r"\brm\s+-\w*[rf]", re.I), "recursive/forced remove (rm -r/-f)"),
    (re.compile(r"\bcmedit\s+delete\b", re.I), "cmedit delete"),
    (re.compile(r"\bmml\b.*\bdelete\b", re.I), "mml delete"),
    (re.compile(r"\bmkfs\b", re.I), "filesystem format (mkfs)"),
    (re.compile(r"\bdd\b\s+if=", re.I), "raw disk copy (dd if=)"),
    (re.compile(r">\s*/dev/(sd|nvme|disk)", re.I), "write to raw disk device"),
    (re.compile(r":\s*\(\)\s*\{.*\}\s*;", re.S), "fork bomb"),
    (re.compile(r"\bchmod\s+-R\b", re.I), "recursive chmod"),
    (re.compile(r"\bchown\s+-R\b", re.I), "recursive chown"),
    (re.compile(r"\brm\b\s+-[^\s]*\s*/\s*($|;|&|\|)", re.I), "remove of root path"),
]

# Split a command line into independent segments (pipes, chains, sequences).
_SEGMENT_SPLIT = re.compile(r"\|\||&&|[;\n|]")


def check_command_safety(command: str) -> Optional[str]:
    """Return a human-readable reason if the command is unsafe, else ``None``."""
    text = (command or "").strip()
    if not text:
        return None

    for pattern, reason in _BLOCKED_PATTERNS:
        if pattern.search(text):
            return reason

    for segment in _SEGMENT_SPLIT.split(text):
        seg = segment.strip()
        if not seg:
            continue
        # Skip a leading directory change like `cd '/path'` so we inspect the
        # real command that follows it.
        first = re.split(r"\s+", seg, maxsplit=1)[0]
        base = first.rsplit("/", 1)[-1].lower()
        if base in ("cd", "pushd"):
            continue
        if base in _BLOCKED_VERBS:
            return f"blocked command '{base}'"
    return None


def assert_command_safe(command: str) -> None:
    """Raise :class:`ValueError` if the command must not be sent to the ENM."""
    reason = check_command_safety(command)
    if reason:
        raise ValueError(
            f"Command rejected for safety ({reason}). Commands that delete "
            "elements (del/rm) or can harm ENM management are not allowed."
        )


if __name__ == "__main__":  # pragma: no cover - manual self-check
    unsafe = [
        "rm -rf /",
        "cd '/home/shared/leo/output' && rm -f *.log",
        "amosbatch -p 70 t.txt s.txt out; del MeContext=ABC",
        "cmedit delete NetworkElement=ABC",
        "shutdown -h now",
        ":(){ :|:& };:",
        "dd if=/dev/zero of=/dev/sda",
    ]
    safe = [
        "amosbatch -p 70 -t 60 enb_eventos.txt Script_export_KPI_rop.txt output",
        "cd '/home/shared/leo/output' && egrep 'Object' *.log > Output.txt",
        "ls -1 *.log",
        "cmedit get NetworkElement=ABC",
    ]
    ok = True
    for cmd in unsafe:
        reason = check_command_safety(cmd)
        print(f"UNSAFE  {'BLOCKED ' + repr(reason):40} <- {cmd}")
        ok = ok and reason is not None
    for cmd in safe:
        reason = check_command_safety(cmd)
        print(f"SAFE    {'ALLOWED' if reason is None else 'FALSE-BLOCK ' + repr(reason):40} <- {cmd}")
        ok = ok and reason is None
    print("\nSELF-CHECK:", "PASS" if ok else "FAIL")
