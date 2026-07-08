# ENM Scheduler

Standalone scheduler for downloading Ericsson ENM MDT/CellTrace logs.

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
python app.py
```

For scheduler testing, enable `Dry run (scan only)` and set `Test sec` to a
small value such as `30`. For production scheduling, leave `Test sec` as `0`
and use `Every min` with the desired interval.

More details: [ENM_MDT_SCHEDULER.md](ENM_MDT_SCHEDULER.md).
