# ENM MDT Scheduler

Standalone local web scheduler for Ericsson ENM MDT/CellTrace log downloads.

The company manual script in `TransferMDT/transfer_mdt_v2.py` downloads from:

```text
/ericsson/pmic1/CELLTRACE/
/ericsson/pmic2/CELLTRACE/
```

This tool keeps the same remote bases. Site scope is configurable per job:

- **Collect all**: discovers every `MeContext`/`ManagedElement` folder under
  both CELLTRACE directories (no `sites.txt` needed).
- **From site list**: restricts the run to the sites registered in the job,
  matching by site name (case-insensitive). The list can be typed or loaded
  from a `.txt` using any separator (comma, semicolon or one per line).

It downloads only new `*.bin.gz` and `*.gpb.gz` files.

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
cd C:\ENM_Scheduler
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe app.py
```

Open `http://127.0.0.1:8095`.

Use the project-local Python 3.9 virtual environment. The dependency versions in
`requirements.txt` are pinned for corporate Windows VMs where newer
`cryptography` wheels may fail to load their native Rust module.

ENM sessions are self-contained in `config.json` and fully managed in the
**Manage sessions** tab: add, edit (name, host/IP, port, user, password,
timeout) and remove connections. The connection `id` stays stable when you
rename, so existing schedules keep pointing to the right session. Passwords are
not saved to `config.json`; they are stored in `.enm_credentials.json` encrypted
with Windows DPAPI for the current Windows user.

Schedules are saved locally in `schedules.json` and can run more than one job.
The current job types are:

- `MDT Transfer`: downloads new CELLTRACE MDT files (read-only SFTP).
- `Script (.py)`: runs a local Python script with optional ENM session
  environment variables.

## Cancellation and retry

`Stop schedule` disables future executions. It does not interrupt a run that is
already active. `Stop run` requests cancellation of the active job. For MDT
Transfer this passes a cancellation signal into the collector, cancels queued
download futures and closes active SSH/SFTP handles so a blocking `sftp.get()`
can fail and unwind. Cancelled runs are marked as `cancelled`; partial `.part`
files are removed by the downloader cleanup path.

MDT Transfer retries transient SSH/SFTP/network failures during scan and per-file
download. Authentication/configuration errors are not retried. Retry attempts
and delay are configured per schedule through `Retries` and `Retry delay sec`.
Progress logs include retry messages such as connection lost, retrying,
reconnected and resuming.

If a schedule is due while its previous execution is still running, the due
cycle is skipped and written to the active job log. This is not treated as a
schedule error, and no second run is started for the same schedule.

When some downloads succeed but others fail, the state file is still saved with
the successful records. The scan checkpoint is not advanced to the full cycle
start time; instead, the next scan resumes from the oldest failed file, including
the configured grace window. This avoids losing files after retry exhaustion
while still preventing duplicates for completed files.

## Command safety

Any job type that sends commands to the ENM must route them through
`enm_mdt_scheduler/security.py` (`assert_command_safe`). Commands that delete
elements (`del`, `rm`, `rdel`, `deletemo`, `cmedit delete`, ...) or that can
harm the OS/ENM management plane (`shutdown`, `mkfs`, `dd if=`, fork bombs, ...)
are rejected before execution. `MDT Transfer` never runs remote commands, so it
is unaffected; the guard is enforced on schedule creation for command-bearing
job types.

## Scheduler Test

Use `Dry run (scan only)` while validating the schedule. In this mode the app
connects, scans CELLTRACE and reports what it would download, but it does not
transfer files and does not update `_state/downloaded_files.json`.

Use `Test sec` to run the scheduler in seconds during validation. When
`Test sec` is greater than `0`, it overrides `Every min`. For the real hourly
job, set `Test sec` back to `0` and use `Every min = 60`.
