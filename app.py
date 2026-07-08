from __future__ import annotations

import json
import queue
import threading
import time
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from enm_mdt_scheduler.collector import (
    CollectionConfig,
    EnmMdtCollector,
    SshConfig,
)


APP_DIR = Path(__file__).resolve().parent
CONFIG_PATH = APP_DIR / "config.json"


DEFAULT_CONFIG = {
    "host": "",
    "port": 22,
    "username": "",
    "local_base": str(APP_DIR / "MDT_Downloads"),
    "interval_minutes": 60,
    "interval_seconds": 0,
    "initial_lookback_minutes": 90,
    "grace_minutes": 30,
    "max_parallel_downloads": 2,
    "dry_run": False,
    "remote_bases": [
        "/ericsson/pmic1/CELLTRACE",
        "/ericsson/pmic2/CELLTRACE",
    ],
}


class SchedulerApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("ENM MDT Scheduler")
        self.geometry("1040x720")
        self.minsize(900, 620)

        self.log_queue: queue.Queue[str] = queue.Queue()
        self.ui_queue: queue.Queue[tuple[str, str]] = queue.Queue()
        self.scheduler_thread: threading.Thread | None = None
        self.stop_event = threading.Event()
        self.worker_lock = threading.Lock()
        self.running_now = False

        self.vars: dict[str, tk.StringVar] = {}
        self.dry_run_var = tk.BooleanVar(value=False)
        self.status_var = tk.StringVar(value="Stopped")
        self.next_run_var = tk.StringVar(value="-")

        self._build_ui()
        self._load_config()
        self.after(200, self._drain_log_queue)

    def _build_ui(self) -> None:
        root = ttk.Frame(self, padding=12)
        root.pack(fill=tk.BOTH, expand=True)

        connection = ttk.LabelFrame(root, text="ENM connection")
        connection.pack(fill=tk.X)

        self._entry(connection, "Host", "host", 0, width=28)
        self._entry(connection, "Port", "port", 0, column=2, width=8)
        self._entry(connection, "User", "username", 0, column=4, width=18)
        self._entry(connection, "Password", "password", 0, column=6, width=18, show="*")

        settings = ttk.LabelFrame(root, text="Scheduler settings")
        settings.pack(fill=tk.X, pady=(10, 0))

        self._entry(settings, "Local base", "local_base", 0, width=64)
        ttk.Button(settings, text="Browse", command=self._browse_local_base).grid(
            row=0, column=2, padx=6, pady=6
        )
        self._entry(settings, "Every min", "interval_minutes", 1, width=8)
        self._entry(settings, "Test sec", "interval_seconds", 1, column=2, width=8)
        self._entry(settings, "First lookback min", "initial_lookback_minutes", 1, column=4, width=8)
        self._entry(settings, "Grace min", "grace_minutes", 1, column=6, width=8)
        self._entry(settings, "Parallel", "max_parallel_downloads", 2, width=8)
        ttk.Checkbutton(settings, text="Dry run (scan only)", variable=self.dry_run_var).grid(
            row=2, column=2, columnspan=2, padx=(8, 4), pady=6, sticky="w"
        )

        remote_frame = ttk.LabelFrame(root, text="Remote CELLTRACE bases")
        remote_frame.pack(fill=tk.X, pady=(10, 0))
        self.vars["remote_bases"] = tk.StringVar()
        remote_entry = ttk.Entry(remote_frame, textvariable=self.vars["remote_bases"])
        remote_entry.pack(fill=tk.X, padx=8, pady=8)

        actions = ttk.Frame(root)
        actions.pack(fill=tk.X, pady=(10, 0))
        ttk.Button(actions, text="Save Config", command=self._save_config).pack(side=tk.LEFT)
        ttk.Button(actions, text="Test Scan / Run Once", command=self._run_once_async).pack(
            side=tk.LEFT, padx=(8, 0)
        )
        ttk.Button(actions, text="Start Scheduler", command=self._start_scheduler).pack(
            side=tk.LEFT, padx=(8, 0)
        )
        ttk.Button(actions, text="Stop Scheduler", command=self._stop_scheduler).pack(
            side=tk.LEFT, padx=(8, 0)
        )
        ttk.Label(actions, text="Status:").pack(side=tk.LEFT, padx=(24, 4))
        ttk.Label(actions, textvariable=self.status_var).pack(side=tk.LEFT)
        ttk.Label(actions, text="Next:").pack(side=tk.LEFT, padx=(24, 4))
        ttk.Label(actions, textvariable=self.next_run_var).pack(side=tk.LEFT)

        log_frame = ttk.LabelFrame(root, text="Progress")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(10, 0))
        self.log_text = tk.Text(log_frame, wrap=tk.NONE, height=22)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        yscroll = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.log_text.yview)
        yscroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.configure(yscrollcommand=yscroll.set)

    def _entry(
        self,
        parent: ttk.Frame,
        label: str,
        key: str,
        row: int,
        column: int = 0,
        width: int = 16,
        show: str = "",
    ) -> None:
        ttk.Label(parent, text=label).grid(row=row, column=column, padx=(8, 4), pady=6, sticky="w")
        var = self.vars.setdefault(key, tk.StringVar())
        ttk.Entry(parent, textvariable=var, width=width, show=show).grid(
            row=row, column=column + 1, padx=(0, 8), pady=6, sticky="w"
        )

    def _load_config(self) -> None:
        data = dict(DEFAULT_CONFIG)
        if CONFIG_PATH.exists():
            try:
                loaded = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    data.update(loaded)
            except Exception as exc:  # noqa: BLE001
                self._log(f"[config] Could not load config.json: {exc}")
        for key, value in data.items():
            if key == "remote_bases":
                self.vars[key].set(";".join(str(item) for item in value))
            elif key == "dry_run":
                self.dry_run_var.set(bool(value))
            else:
                self.vars.setdefault(key, tk.StringVar()).set(str(value))
        self.vars.setdefault("password", tk.StringVar()).set("")

    def _save_config(self) -> None:
        try:
            config = self._config_dict(include_password=False)
            CONFIG_PATH.write_text(json.dumps(config, indent=2), encoding="utf-8")
            self._log(f"[config] Saved {CONFIG_PATH}")
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Config", str(exc))

    def _config_dict(self, include_password: bool) -> dict:
        data = {
            "host": self.vars["host"].get().strip(),
            "port": int(self.vars["port"].get().strip() or "22"),
            "username": self.vars["username"].get().strip(),
            "local_base": self.vars["local_base"].get().strip(),
            "interval_minutes": int(self.vars["interval_minutes"].get().strip() or "60"),
            "interval_seconds": int(self.vars["interval_seconds"].get().strip() or "0"),
            "initial_lookback_minutes": int(
                self.vars["initial_lookback_minutes"].get().strip() or "90"
            ),
            "grace_minutes": int(self.vars["grace_minutes"].get().strip() or "30"),
            "max_parallel_downloads": int(
                self.vars["max_parallel_downloads"].get().strip() or "2"
            ),
            "remote_bases": [
                item.strip().rstrip("/")
                for item in self.vars["remote_bases"].get().replace("\n", ";").split(";")
                if item.strip()
            ],
            "dry_run": bool(self.dry_run_var.get()),
        }
        if include_password:
            data["password"] = self.vars["password"].get()
        return data

    def _collector_config(self) -> CollectionConfig:
        data = self._config_dict(include_password=True)
        missing = [key for key in ("host", "username", "password", "local_base") if not data.get(key)]
        if missing:
            raise ValueError("Missing required field(s): " + ", ".join(missing))
        return CollectionConfig(
            ssh=SshConfig(
                host=data["host"],
                port=data["port"],
                username=data["username"],
                password=data["password"],
            ),
            local_base=data["local_base"],
            remote_bases=tuple(data["remote_bases"]),
            initial_lookback_minutes=data["initial_lookback_minutes"],
            grace_minutes=data["grace_minutes"],
            max_parallel_downloads=data["max_parallel_downloads"],
            dry_run=data["dry_run"],
        )

    def _effective_interval_seconds(self) -> int:
        data = self._config_dict(include_password=False)
        test_seconds = int(data.get("interval_seconds") or 0)
        if test_seconds > 0:
            return test_seconds
        return int(data["interval_minutes"]) * 60

    def _browse_local_base(self) -> None:
        start = self.vars["local_base"].get().strip() or str(APP_DIR)
        selected = filedialog.askdirectory(initialdir=start)
        if selected:
            self.vars["local_base"].set(selected)

    def _run_once_async(self) -> None:
        if self.running_now:
            messagebox.showinfo("Run", "A collection is already running.")
            return
        try:
            config = self._collector_config()
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Run", str(exc))
            return
        thread = threading.Thread(target=self._run_once_worker, args=(config,), daemon=True)
        thread.start()

    def _run_once_worker(self, config: CollectionConfig) -> None:
        with self.worker_lock:
            self.running_now = True
            self._set_status("Running")
            try:
                collector = EnmMdtCollector(config, log=self._log)
                mode = "dry-run" if config.dry_run else "download"
                self._log(f"[run] Starting collection ({mode})")
                result = collector.collect_once()
                self._log(
                    "[run] Finished: "
                    f"scanned={result.scanned_files}, "
                    f"downloaded={result.downloaded}, "
                    f"known={result.skipped_known}, "
                    f"existing={result.skipped_existing}, "
                    f"failed={result.failed}"
                )
            except Exception as exc:  # noqa: BLE001
                self._log(f"[ERROR] {exc}")
            finally:
                self.running_now = False
                if not self.scheduler_thread or not self.scheduler_thread.is_alive():
                    self._set_status("Stopped")

    def _start_scheduler(self) -> None:
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            messagebox.showinfo("Scheduler", "Scheduler is already running.")
            return
        try:
            config = self._collector_config()
            interval_s = self._effective_interval_seconds()
            if interval_s < 1:
                raise ValueError("Interval must be at least 1 second.")
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Scheduler", str(exc))
            return
        self.stop_event.clear()
        self.scheduler_thread = threading.Thread(
            target=self._scheduler_loop,
            args=(config, interval_s),
            daemon=True,
        )
        self.scheduler_thread.start()
        self._set_status("Scheduled")
        self._log("[scheduler] Started")

    def _stop_scheduler(self) -> None:
        self.stop_event.set()
        self._set_status("Stopping")
        self._set_next_run("-")
        self._log("[scheduler] Stop requested")

    def _scheduler_loop(self, config: CollectionConfig, interval_s: int) -> None:
        while not self.stop_event.is_set():
            self._run_once_worker(config)
            if self.stop_event.is_set():
                break
            next_epoch = time.time() + interval_s
            self._set_next_run(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(next_epoch)))
            self._set_status("Scheduled")
            if self.stop_event.wait(interval_s):
                break
        self._set_status("Stopped")
        self._set_next_run("-")
        self._log("[scheduler] Stopped")

    def _log(self, line: str) -> None:
        timestamp = time.strftime("%H:%M:%S")
        self.log_queue.put(f"{timestamp} {line}")

    def _set_status(self, value: str) -> None:
        self.ui_queue.put(("status", value))

    def _set_next_run(self, value: str) -> None:
        self.ui_queue.put(("next_run", value))

    def _drain_log_queue(self) -> None:
        try:
            while True:
                key, value = self.ui_queue.get_nowait()
                if key == "status":
                    self.status_var.set(value)
                elif key == "next_run":
                    self.next_run_var.set(value)
        except queue.Empty:
            pass
        try:
            while True:
                line = self.log_queue.get_nowait()
                self.log_text.insert(tk.END, line + "\n")
                self.log_text.see(tk.END)
        except queue.Empty:
            pass
        self.after(200, self._drain_log_queue)


if __name__ == "__main__":
    SchedulerApp().mainloop()
