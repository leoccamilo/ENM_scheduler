# ENM MDT Scheduler

Standalone local web scheduler for Ericsson ENM MDT/CellTrace log downloads.

The company manual script in `TransferMDT/transfer_mdt_v2.py` downloads from:

```text
/ericsson/pmic1/CELLTRACE/
/ericsson/pmic2/CELLTRACE/
```

This tool keeps the same remote bases, but does not require a `sites.txt`.
It discovers all `MeContext`/`ManagedElement` folders under both CELLTRACE
directories and downloads only new `*.bin.gz` and `*.gpb.gz` files.

## Local Layout

The local layout follows the manual script:

```text
<LOCAL_BASE>/
  DDMMYYYY/
    SITE/
      A2026...celltracefile_DUL1_1.bin.gz
      A2026...celltracefile_CUCP0_1_1.gpb.gz
  _state/
    downloaded_files.json
```

`downloaded_files.json` stores `remote_path`, `size`, `mtime`, local path and
download timestamp. This is what prevents repeated downloads.

## Scan Rules

- First run: downloads files modified within the configured lookback window
  (`90` minutes by default).
- Later runs: scans from the previous run minus a grace window (`30` minutes by
  default), then skips anything already recorded in state.
- Remote bases stay limited to CELLTRACE:
  `/ericsson/pmic1/CELLTRACE` and `/ericsson/pmic2/CELLTRACE`.

## Run

```powershell
C:\CRT\.venv\Scripts\python.exe app.py
```

Open `http://127.0.0.1:8095`.

Use the Python 3.9 virtual environment from ENM Manager. This keeps scheduled
Python scripts on the same runtime expected by ENM scripting.

The app imports ENM sessions from `%USERPROFILE%\.securecrt_manager\sessions.db`
when available, so the same ENMs created in ENM Manager appear in the scheduler.
Host, port, username and timeout can be edited. Passwords are accepted in the
session panel but are kept only in memory and are not saved to `config.json`.

Schedules are saved locally in `schedules.json` and can run more than one job.
The current job types are:

- `MDT Transfer`: downloads new CELLTRACE MDT files.
- `Script (.py)`: runs a local Python script with optional ENM session
  environment variables.

## Scheduler Test

Use `Dry run (scan only)` while validating the schedule. In this mode the app
connects, scans CELLTRACE and reports what it would download, but it does not
transfer files and does not update `_state/downloaded_files.json`.

Use `Test sec` to run the scheduler in seconds during validation. When
`Test sec` is greater than `0`, it overrides `Every min`. For the real hourly
job, set `Test sec` back to `0` and use `Every min = 60`.
