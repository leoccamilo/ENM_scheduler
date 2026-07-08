# ENM Scheduler

Standalone local web scheduler for downloading Ericsson ENM MDT/CellTrace logs.

The first version follows the company manual collector strategy for remote
paths:

```text
/ericsson/pmic1/CELLTRACE
/ericsson/pmic2/CELLTRACE
```

It discovers all active `MeContext`/`ManagedElement` folders dynamically, so it
does not require a fixed `sites.txt`. It downloads only new `*.bin.gz` and
`*.gpb.gz` files and stores local state to avoid duplicate downloads.

Run:

```powershell
C:\CRT\.venv\Scripts\python.exe app.py
```

Or use:

```powershell
.\run_scheduler.bat
```

Open:

```text
http://127.0.0.1:8095
```

The UI follows the web scheduler style from `C:/MoB_Tool_Box`: compact job rows,
inline actions, time windows, progress logs and a dedicated `MDT Transfer` job
type. It imports saved ENM sessions from `%USERPROFILE%\.securecrt_manager`
when available. Passwords are kept only in memory.

For scheduler testing, enable `Dry run (scan only)` and set `Test sec` to a
small value such as `30`. For production scheduling, leave `Test sec` as `0`
and use `Every min` with the desired interval.

More details: [ENM_MDT_SCHEDULER.md](ENM_MDT_SCHEDULER.md).
