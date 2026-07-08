# ENM MDT Scheduler

Standalone scheduler for Ericsson ENM MDT/CellTrace log downloads.

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
python app.py
```

Enter ENM host, port, user, password and local base folder. The password is not
saved to `config.json`.

