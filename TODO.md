# Status 2026-07-09

## Implemented

- ENM password persistence:
  - `config.json` still has no plaintext password.
  - `.enm_credentials.json` stores passwords encrypted with Windows DPAPI.
  - saving a session with an empty password field preserves the existing value.
  - removing a session removes its encrypted password on the next sync.

- Real MDT Transfer cancellation:
  - `Stop schedule` stops only future executions.
  - `Stop run` signals cancellation to the collector.
  - the collector checks cancellation during scan, task preparation, dry-run,
    downloads and before saving state.
  - futures that have not started are cancelled.
  - active SSH/SFTP handles are closed to interrupt `sftp.get()` when possible.
  - cancelled jobs are marked as `cancelled`.

- Temporary connection retry/reconnect:
  - scan and per-file download use configurable retry policy.
  - temporary network/SFTP/SSH errors are retried.
  - authentication/configuration errors are not retried.
  - logs show `connection lost`, `retrying`, `reconnected` and `resuming`.
  - `.part` files are removed when an attempt fails or is cancelled.

- Long-running cycle behavior:
  - if the next scheduled cycle is due while the previous run is still active,
    the new cycle is skipped and logged without marking the schedule as error.
  - on partial download failures, successful records are saved and the next scan
    resumes from the oldest failed file plus the configured grace window.

- UI text normalization:
  - remaining Portuguese text in the visible app UI and MDT preview was changed
    to English.

## Validation Done

- `python -m py_compile` for the main modules.
- DPAPI test with a dummy password in a temporary file:
  - the password was not saved as plaintext;
  - decrypted reading returned the expected value.

## Validation Still Dependent On ENM/VPN

- Save a real password, restart the app and run MDT Transfer without retyping it.
- Start a real download, click `Stop run` and confirm new files stop appearing.
- Drop VPN/network during a real download and confirm automatic reconnect.
