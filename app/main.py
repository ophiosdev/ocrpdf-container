import json
import logging
import multiprocessing
import os
import shutil
import signal
import sqlite3
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import settings
from jobs import JobStatus
from monitor import run_monitor_logic
from worker import run_worker_logic


class Supervisor:
    def __init__(self):
        self.log = logging.getLogger("supervisor")
        self.processes = {}
        self.targets = {}
        self.shutdown = False
        self.force_shutdown = False

    def start_process(self, name, target):
        ctx = multiprocessing.get_context("spawn")
        p = ctx.Process(target=target, name=name)
        p.start()
        self.processes[name] = p
        self.targets[name] = target

    def get_status(self) -> dict:
        """Return health status dict: {workers_alive, queue_depth, last_error}.

        - workers_alive: number of worker child processes currently alive
        - queue_depth: number of queued jobs in the persistent job table
        - last_error: dict with last error job (job_id, file_path, finished_at) or None
        """
        cfg = settings.Config()
        workers_alive = sum(
            1
            for n, p in self.processes.items()
            if n.startswith("worker-") and p.is_alive()
        )
        queue_depth = 0
        last_error = None
        conn = None
        try:
            conn = sqlite3.connect(cfg.queue_db_path)
            cur = conn.execute(
                "SELECT COUNT(*) FROM ocr_jobs WHERE status = ?",
                (JobStatus.QUEUED.value,),
            )
            queue_depth = int(cur.fetchone()[0])
            cur = conn.execute(
                "SELECT job_id, file_path, finished_at FROM ocr_jobs WHERE status = ? ORDER BY finished_at DESC LIMIT 1",
                (JobStatus.ERROR.value,),
            )
            row = cur.fetchone()
            if row:
                last_error = {
                    "job_id": row[0],
                    "file_path": row[1],
                    "finished_at": row[2],
                }
        except Exception:
            # Swallow DB errors to keep health endpoint lightweight; report degraded state.
            last_error = {"error": "failed to read queue DB"}
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass

        return {
            "workers_alive": workers_alive,
            "queue_depth": queue_depth,
            "last_error": last_error,
        }

    def run(self):
        logging.basicConfig(level=logging.INFO, format="[SUPER]   %(message)s")
        _ = signal.signal(signal.SIGINT, self._handle_exit)
        _ = signal.signal(signal.SIGTERM, self._handle_exit)

        # Validate and log resolved configuration before starting child processes.
        try:
            cfg = settings.Config()
        except Exception as exc:
            self.log.error("Configuration validation failed: %s", exc)
            raise
        cfg.log(self.log)
        self._run_startup_checks(cfg, self.log)

        self.start_process("monitor", run_monitor_logic)
        for i in range(cfg.max_workers):
            self.start_process(f"worker-{i}", run_worker_logic)

        # Start a simple HTTP health endpoint in a background thread.
        try:
            server = ThreadingHTTPServer(
                ("127.0.0.1", 8000), self._make_health_handler()
            )
            server.supervisor = self  # pyright: ignore[reportAttributeAccessIssue]
            self._http_server = server  # pyright: ignore[reportUninitializedInstanceVariable]
            http_thread = threading.Thread(
                target=server.serve_forever, name="health-server", daemon=True
            )
            http_thread.start()
            self.log.info("Health endpoint listening on http://127.0.0.1:8000/health")
        except Exception:
            self.log.exception("Failed to start health endpoint")

        # Main supervision loop: restart crashed children unless we're shutting down.
        try:
            while not self.shutdown:
                for name, p in list(self.processes.items()):
                    if not p.is_alive() and not self.shutdown:
                        target = self.targets.get(name)
                        if target:
                            self.start_process(name, target)
                time.sleep(1)
        finally:
            # Perform graceful shutdown sequence once shutdown flag is set.
            self._graceful_shutdown()

    def _handle_exit(self, *_):
        if self.shutdown:
            self.force_shutdown = True
            self.log.warning("Shutdown signal received again; forcing shutdown")
            return
        self.log.info("Shutdown signal received")
        self.shutdown = True

    def _make_health_handler(self):
        supervisor = self

        class HealthHandler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                # Silence default http server logs; use supervisor logger instead.
                supervisor.log.info("[health] %s", format % args)

            def do_GET(self):
                if self.path != "/health":
                    self.send_response(404)
                    self.end_headers()
                    return
                status = supervisor.get_status()
                body = json.dumps(status).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                _ = self.wfile.write(body)

        return HealthHandler

    def _terminate_process(self, p, name, timeout=10):
        """Attempt graceful termination of process `p`, escalating to kill after `timeout` seconds."""
        if not p or not p.is_alive():
            return
        self.log.info("Terminating %s (pid=%s)", name, p.pid)
        try:
            p.terminate()
        except Exception:
            self.log.exception("Failed to send terminate to %s", name)
        # Wait for process to exit
        start = time.time()
        while time.time() - start < timeout:
            if not p.is_alive():
                self.log.info("Process %s exited", name)
                return
            time.sleep(0.5)
        # Escalate
        try:
            self.log.warning("Killing %s (pid=%s) after timeout", name, p.pid)
            p.kill()
        except Exception:
            self.log.exception("Failed to kill %s", name)

    def _graceful_shutdown(self) -> None:
        """Shutdown order:
        1. Stop monitor (so no new jobs are enqueued).
        2. Wait for queue to drain (bounded wait).
        3. Terminate worker processes (allow current jobs to finish briefly).
        4. Shutdown health endpoint and exit.
        """
        self.log.info("Beginning graceful shutdown")

        # 1) Stop monitor first
        monitor = self.processes.get("monitor")
        if monitor and monitor.is_alive():
            self._terminate_process(monitor, "monitor", timeout=10)

        # 2) Wait for queue to drain or timeout (unless force shutdown).
        drain_timeout = 0 if self.force_shutdown else 60
        start = time.time()
        while time.time() - start < drain_timeout and not self.force_shutdown:
            try:
                cfg = settings.Config()
                conn = sqlite3.connect(cfg.queue_db_path)
                cur = conn.execute(
                    "SELECT COUNT(*) FROM ocr_jobs WHERE status = ?",
                    (JobStatus.QUEUED.value,),
                )
                queued = int(cur.fetchone()[0])
                conn.close()
            except Exception:
                self.log.exception("Failed to read queue DB during shutdown")
                queued = 0
            if queued == 0:
                self.log.info("Queue drained")
                break
            self.log.info("Waiting for queue to drain: %d jobs remaining", queued)
            time.sleep(2)

        # 3) Terminate workers
        for name, p in list(self.processes.items()):
            if name.startswith("worker-"):
                try:
                    self._terminate_process(p, name, timeout=10)
                finally:
                    self.processes.pop(name, None)

        # 4) Shutdown health endpoint
        try:
            if hasattr(self, "_http_server") and self._http_server:
                self.log.info("Shutting down health endpoint")
                self._http_server.shutdown()
        except Exception:
            self.log.exception("Failed to shutdown health endpoint")

        self.log.info("Graceful shutdown complete")

    def _run_startup_checks(self, cfg: settings.Config, logger: logging.Logger) -> None:
        _ensure_dir(cfg.queue_db_path, logger)
        if cfg.work_dir:
            os.makedirs(cfg.work_dir, exist_ok=True)
            try:
                os.chmod(cfg.work_dir, 0o777)
            except Exception as exc:
                logger.warning("Failed to make %s writable: %r", cfg.work_dir, exc)
        _check_workdir_space(cfg.work_dir, logger)
        _check_tesseract_languages(cfg.ocr_langs, logger)


def _ensure_dir(path: str, logger: logging.Logger) -> None:
    logger.debug("Checking parent directory for %s", path)
    directory = os.path.dirname(path)
    if not directory:
        logger.debug("No parent directory derived from %s", path)
        return
    if not os.path.isdir(directory):
        logger.info("Creating directory %s", directory)
        os.makedirs(directory, exist_ok=True)
    if not os.access(directory, os.W_OK):
        try:
            logger.debug("Making %s writable", directory)
            os.chmod(directory, 0o777)
        except Exception as exc:
            logger.warning("Failed to make %s writable: %r", directory, exc)
    else:
        logger.debug("Directory is writable: %s", directory)


def _check_workdir_space(work_dir: str, logger: logging.Logger) -> None:
    logger.debug("Checking available space for WORK_DIR=%s", work_dir)
    if not work_dir:
        logger.debug("WORK_DIR not set; skipping space check")
        return
    required_bytes = int(os.getenv("WORK_DIR_MIN_SPACE_BYTES", "104857600"))
    try:
        usage = shutil.disk_usage(work_dir)
        available_bytes = usage.free
    except Exception as exc:
        logger.warning("Failed to check available space for %s: %r", work_dir, exc)
        return
    logger.debug(
        "WORK_DIR free bytes=%d required bytes=%d",
        available_bytes,
        required_bytes,
    )
    if available_bytes < required_bytes:
        logger.warning(
            "Warning: available space for WORK_DIR (%s) is less than required: %d bytes",
            work_dir,
            available_bytes,
        )


def _check_tesseract_languages(ocr_langs: str, logger: logging.Logger) -> None:
    logger.debug("Checking Tesseract languages for OCR_LANGS=%s", ocr_langs)
    if not ocr_langs:
        logger.debug("OCR_LANGS not set; skipping Tesseract language check")
        return
    tesseract_path = shutil.which("tesseract")
    if not tesseract_path:
        logger.debug("tesseract not found on PATH; skipping language check")
        return
    langs = [
        part.strip() for part in ocr_langs.replace(",", "+").split("+") if part.strip()
    ]
    if not langs:
        logger.debug("No Tesseract languages parsed; skipping check")
        return
    logger.info("Verifying Tesseract languages: %s", ocr_langs)
    try:
        output = subprocess.check_output(
            ["tesseract", "--list-langs"],
            stderr=subprocess.STDOUT,
            text=True,
        )
    except Exception as exc:
        logger.warning("Failed to list tesseract languages: %r", exc)
        return
    available = {line.strip() for line in output.splitlines() if line.strip()}
    missing = [lang for lang in langs if lang not in available]
    logger.debug("Tesseract available languages count=%d", len(available))
    logger.debug("Tesseract missing languages: %s", missing)
    if missing:
        logger.warning(
            "Warning: missing Tesseract language data for: %s",
            " ".join(missing),
        )


if __name__ == "__main__":
    Supervisor().run()
