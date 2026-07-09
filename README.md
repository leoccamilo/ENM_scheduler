# ENM Scheduler

Standalone local web scheduler for downloading Ericsson ENM MDT/CellTrace logs.

The first version follows the company manual collector strategy for remote
paths:

```text
/ericsson/pmic1/CELLTRACE
/ericsson/pmic2/CELLTRACE
```

For `MDT Transfer` jobs the site scope is configurable:

- **Collect all**: discovers every active `MeContext`/`ManagedElement` folder
  dynamically (no fixed `sites.txt` required).
- **From site list**: restricts collection to the sites registered in the job.
  The list can be typed or loaded from a `.txt` (any separator works: comma,
  semicolon or one per line).

It downloads only new `*.bin.gz` and `*.gpb.gz` files and stores local state to
avoid duplicate downloads. MDT files are saved under
`<Download folder>/DDMMYYYY/<site>/`. This date/site sub-foldering is specific to
MDT jobs; other job types write directly to the selected folder.

Install dependencies and run:

```powershell
cd C:\ENM_Scheduler
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe app.py
```

Or use:

```powershell
.\run_scheduler.bat
```

On corporate Windows VMs, use the project-local `.venv`. The dependency
versions are pinned because newer `cryptography` wheels can fail to load their
native Rust module on older Windows images. To repair a broken install:

```powershell
cd C:\ENM_Scheduler
.\.venv\Scripts\python.exe -m pip uninstall -y paramiko cryptography bcrypt pynacl cffi pycparser
.\.venv\Scripts\python.exe -m pip install --no-cache-dir --force-reinstall -r requirements.txt
.\.venv\Scripts\python.exe -c "import paramiko; print(paramiko.__version__)"
```

Open:

```text
http://127.0.0.1:8095
```

## UI

The interface is split into two tabs so the areas do not compete for vertical
space:

- **Create scheduler**: job list, progress logs and the "Add New Schedule" form
  (job type, session, MDT settings, download folder with a native `Browse...`
  picker, site scope, time window with a 24h date/hour/minute picker).
- **Manage sessions**: the ENM Sessions panel where you can add, edit (name,
  host/IP, port, user, password, timeout) and remove connections. Sessions are
  self-contained in `config.json`; the connection `id` stays stable when you
  rename, so existing schedules keep working. Passwords are stored outside
  `config.json` in `.enm_credentials.json`, encrypted with Windows DPAPI for the
  current Windows user.

The header shows the Amdocs logo.

For scheduler testing, enable `Dry run (scan only)` and set `Test sec` to a
small value such as `30`. For production scheduling, leave `Test sec` as `0`
and use `Every min` with the desired interval.

`Stop schedule` disables future cycles only. `Stop run` requests cancellation of
the active MDT Transfer run and closes active SFTP connections when possible.
Temporary ENM/SFTP connection drops are retried with the job's `Retries` and
`Retry delay sec` settings.

If a scheduled cycle becomes due while the previous run is still active, the new
cycle is skipped and logged in the active run. The tool does not start parallel
runs for the same schedule. When a run finishes with partial download failures,
successful downloads are still recorded, but the next scan resumes from the
oldest failed file instead of advancing the checkpoint to the full cycle time.

## Future improvements

- **CLI job type**: run a generic remote command over SSH and download matching
  files directly to the selected folder (no date/site sub-foldering).
- **amosbatch job type**: full ENM `amosbatch` flow (remote folder + amosbatch
  command + optional egrep + clean-before-first-run + glob download), mirroring
  the ENM Manager desktop tool. CLI and amosbatch are two distinct command
  types.

More details: [ENM_MDT_SCHEDULER.md](ENM_MDT_SCHEDULER.md).
