import contextlib
import hashlib
import logging
import multiprocessing
import ntpath
import os
import queue
import shutil
import signal
import tempfile
import threading
import time
from pathlib import Path
from typing import BinaryIO
from uuid import uuid4

import dramatiq
import ocrmypdf
from dramatiq import Worker
from smbprotocol.connection import Connection
from smbprotocol.exceptions import EndOfFile
from smbprotocol.open import (
    CreateDisposition,
    CreateOptions,
    FilePipePrinterAccessMask,
    ImpersonationLevel,
    Open,
    ShareAccess,
)
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from typing_extensions import Buffer

import settings
from broker import create_broker
from interfaces import OcrExecutor, StatInfo, StorageAdapter, WorkDirManager
from jobs import JobStatus, SqliteJobStore
from retry_middleware import PermanentError
from tasks import DramatiqDispatcher, register_tasks

_DISPATCHER = None
_DIR_NAME_LENGTH = 12


def _parse_languages(ocr_langs: str) -> list[str]:
    if not ocr_langs:
        return []
    parts = (part.strip() for part in ocr_langs.replace(",", "+").split("+"))
    return [part for part in parts if part]


def _parse_verbosity(verbosity: str) -> ocrmypdf.Verbosity:
    normalized = (verbosity or "").strip().lower()
    mapping = {
        "quiet": ocrmypdf.Verbosity.quiet,
        "default": ocrmypdf.Verbosity.default,
        "debug": ocrmypdf.Verbosity.debug,
        "debug_all": ocrmypdf.Verbosity.debug_all,
    }
    return mapping.get(normalized, ocrmypdf.Verbosity.default)


def _file_md5(path: str) -> str:
    digest = hashlib.md5()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _log_pdf_text_summary(logger: logging.Logger, path: str) -> None:
    try:
        pdfinfo = ocrmypdf.pdfinfo.PdfInfo(path)  # pyright: ignore[reportArgumentType]
        pages = [page for page in pdfinfo.pages if page is not None]
        total_pages = len(pages)
        text_pages = sum(1 for page in pages if page.has_text)
        logger.info(
            "OCR output text summary: path=%s text_pages=%d total_pages=%d",
            path,
            text_pages,
            total_pages,
        )
    except Exception as exc:
        logger.warning(
            "Failed to inspect OCR output text layer: path=%s error=%r",
            path,
            exc,
        )


def _run_ocr_process(
    input_path: str,
    output_path: str,
    ocr_langs: str,
    ocr_output_type: str,
    ocr_rotate_pages: bool,
    ocr_deskew: bool,
    ocr_jobs: int,
    ocr_mode: str,
    ocr_verbosity: str,
    ocr_log_root: bool,
    ocr_keep_temp: bool,
    ocr_job_timeout: int,
    result_queue: multiprocessing.Queue,
) -> None:
    try:
        log_level = _parse_verbosity(ocr_verbosity)
        _ = ocrmypdf.configure_logging(log_level, manage_root_logger=ocr_log_root)
        logging.getLogger("worker").debug("OCR verbosity set to %s", log_level.name)
        logging.getLogger("worker").debug(
            "OCRmyPDF version: %s", getattr(ocrmypdf, "__version__", "unknown")
        )
        languages = _parse_languages(ocr_langs)
        logging.getLogger("worker").debug("OCR languages: %s", languages)
        work_folder = Path(os.path.dirname(input_path))
        logging.getLogger("worker").debug("OCR work folder: %s", work_folder)
        os.makedirs(work_folder, exist_ok=True)
        os.environ["TMPDIR"] = str(work_folder)
        os.environ["TMP"] = str(work_folder)
        os.environ["TEMP"] = str(work_folder)
        tempfile.tempdir = str(work_folder)
        logging.getLogger("worker").debug("OCR temp base set to %s", work_folder)
        options = ocrmypdf.OcrOptions(
            input_file=input_path,
            output_file=output_path,
            languages=languages,
            output_type=ocr_output_type,
            work_folder=work_folder,
            rotate_pages=ocr_rotate_pages,
            deskew=ocr_deskew,
            clean=True,
            clean_final=True,
            jobs=ocr_jobs,
            mode=ocr_mode,  # pyright: ignore[reportArgumentType]
            progress_bar=False,
            keep_temporary_files=ocr_keep_temp,
            tesseract_timeout=float(ocr_job_timeout),
            tesseract_non_ocr_timeout=float(ocr_job_timeout),
        )
        exit_code = ocrmypdf.ocr(options)
        result_queue.put(("exit_code", int(exit_code)))
    except Exception as exc:
        try:
            result_queue.put(("exception", exc))
        except Exception:
            result_queue.put(("exception_repr", repr(exc)))


def _job_dir(file_path: str) -> str:
    cfg = settings.Config()
    job_id = hashlib.sha1(file_path.encode("utf-8")).hexdigest()[:_DIR_NAME_LENGTH]
    return os.path.join(cfg.work_dir, job_id)


def _is_job_dir_name(name: str) -> bool:
    """Return True if the directory name looks like a job dir (_DIR_NAME_LENGTH hex chars)."""
    if not isinstance(name, str) or len(name) != _DIR_NAME_LENGTH:
        return False
    try:
        _ = int(name, 16)
        return True
    except ValueError:
        return False


def _cleanup_stale_workdirs() -> None:
    """Remove stale job directories older than WORK_DIR_MAX_AGE_SECONDS.

    Only removes directories that match the job-id naming pattern to avoid
    accidental deletion of unrelated files. Errors are logged but do not stop
    startup.
    """
    logger = logging.getLogger("worker")
    cfg = settings.Config()
    work_dir = cfg.work_dir
    try:
        os.makedirs(work_dir, exist_ok=True)
    except Exception:
        logger.exception("Failed to ensure WORK_DIR exists: %r", work_dir)
        return

    try:
        now = time.time()
        for name in os.listdir(work_dir):
            if not _is_job_dir_name(name):
                continue
            path = os.path.join(work_dir, name)
            if not os.path.isdir(path):
                continue
            try:
                mtime = os.path.getmtime(path)
            except Exception:
                logger.exception("Could not stat workdir %r", path)
                continue
            age = now - mtime
            if age > cfg.work_dir_max_age_seconds:
                try:
                    shutil.rmtree(path)
                    logger.info("Removed stale workdir %s (age=%.0fsec)", path, age)
                except Exception:
                    logger.exception("Failed to remove stale workdir %r", path)
    except Exception:
        logger.exception("Unexpected error during stale workdir cleanup")


class SmbStorageAdapter(StorageAdapter):
    def __init__(
        self,
        unc_path: str,
        username: str | None,
        password: str | None,
        enforce_encryption: bool = False,
    ):
        server, share = self._parse_unc(unc_path)
        self._server = server
        self._share = share
        self._client = self._connect(
            server,
            share,
            username,
            password,
            enforce_encryption,
        )

    def _parse_unc(self, unc_path: str) -> tuple[str, str]:
        if not unc_path.startswith("\\"):
            raise ValueError("SMB_UNC_PATH must be a UNC path (e.g. \\\\server\\share)")
        parts = [p for p in unc_path.split("\\") if p]
        if len(parts) < 2:  # noqa: PLR2004
            raise ValueError("SMB_UNC_PATH must include server and share")
        return parts[0], parts[1]

    def _connect(self, server, share, username, password, enforce_encryption):
        conn = Connection(uuid4(), server, port=445)
        conn.connect()
        session = Session(conn, username, password, enforce_encryption)
        session.connect()
        tree = TreeConnect(session, rf"\\{server}\{share}")
        tree.connect()

        class _SmbClient:
            def __init__(self, tree_handle):
                self._tree = tree_handle

            def open_file(self, path, mode):
                # Apply recommended Open.create() template depending on
                # whether the caller wants to read or write.
                # Read:  GENERIC_READ,  FILE_SHARE_READ|WRITE|DELETE, FILE_OPEN,
                #        FILE_NON_DIRECTORY_FILE
                # Write: GENERIC_WRITE, FILE_SHARE_READ|WRITE,
                #        FILE_OVERWRITE_IF, FILE_NON_DIRECTORY_FILE
                is_read = "r" in mode and "w" not in mode

                if is_read:
                    desired_access = FilePipePrinterAccessMask.GENERIC_READ
                    share_access = (
                        ShareAccess.FILE_SHARE_READ
                        | ShareAccess.FILE_SHARE_WRITE
                        | ShareAccess.FILE_SHARE_DELETE
                    )
                    create_disposition = CreateDisposition.FILE_OPEN
                else:
                    desired_access = FilePipePrinterAccessMask.GENERIC_WRITE
                    share_access = (
                        ShareAccess.FILE_SHARE_READ | ShareAccess.FILE_SHARE_WRITE
                    )
                    create_disposition = CreateDisposition.FILE_OVERWRITE_IF

                create_options = CreateOptions.FILE_NON_DIRECTORY_FILE

                open_ = Open(self._tree, path)
                # Ensure that if the create operation fails we close the
                # Open object to avoid leaking handles.
                try:
                    _ = open_.create(
                        ImpersonationLevel.Impersonation,
                        desired_access,
                        0,
                        share_access,
                        create_disposition,
                        create_options,
                    )
                    return open_
                except Exception:
                    with contextlib.suppress(Exception):
                        _ = open_.close()
                    raise

            def exists(self, path):
                open_ = None
                try:
                    open_ = Open(self._tree, path)
                    _ = open_.create(
                        ImpersonationLevel.Impersonation,
                        FilePipePrinterAccessMask.GENERIC_READ,
                        0,
                        0,
                        1,
                        0,
                    )
                    return True
                except Exception:
                    return False
                finally:
                    if open_:
                        with contextlib.suppress(Exception):
                            _ = open_.close()

            def stat(self, path):
                open_ = Open(self._tree, path)
                _ = open_.create(
                    ImpersonationLevel.Impersonation,
                    FilePipePrinterAccessMask.GENERIC_READ,
                    0,
                    0,
                    1,
                    0,
                )
                try:
                    return type(
                        "StatInfo",
                        (),
                        {
                            "end_of_file": open_.end_of_file,
                            "last_write_time": open_.last_write_time,
                        },
                    )()
                finally:
                    _ = open_.close()

        return _SmbClient(tree)

    def _normalize_path(self, path: str) -> str:
        norm = ntpath.normpath(path)
        parts = [p for p in norm.split("\\") if p]
        if (
            len(parts) >= 2  # noqa: PLR2004
            and parts[0].lower() == self._server.lower()
            and parts[1].lower() == self._share.lower()
        ):
            parts = parts[2:]
        return "\\".join(parts)

    def open(self, path: str, mode: str):
        rel_path = self._normalize_path(path)
        return _SmbFile(self._client.open_file(rel_path, mode=mode), mode=mode)

    def exists(self, path: str) -> bool:
        return self._client.exists(self._normalize_path(path))

    def stat(self, path: str) -> StatInfo:
        info = self._client.stat(self._normalize_path(path))
        size = info.end_of_file  # pyright: ignore[reportAttributeAccessIssue]
        ts = info.last_write_time  # pyright: ignore[reportAttributeAccessIssue]
        mtime = ts.timestamp() if hasattr(ts, "timestamp") else float(ts)
        return StatInfo(size=size, mtime=mtime)

    def rename(self, src: str, dst: str) -> None:
        # Currently unused, but kept for optional workflows.
        self._client.rename(  # pyright: ignore[reportAttributeAccessIssue]
            self._normalize_path(src),
            self._normalize_path(dst),
        )


class _SmbFile(BinaryIO):
    def __init__(self, handle, mode: str):
        self._handle = handle
        self._offset = 0
        self._mode = mode

    def read(self, size: int = -1) -> bytes:
        if "r" not in self._mode:
            raise OSError("File not open for reading")
        length = size if size >= 0 else 0x7FFFFFFF
        try:
            data = self._handle.read(self._offset, length)
        except EndOfFile:
            return b""
        self._offset += len(data)
        return data

    def write(self, data: Buffer) -> int:
        if "w" not in self._mode:
            raise OSError("File not open for writing")
        written = self._handle.write(data, self._offset)
        self._offset += written
        return written

    def close(self) -> None:
        # Close the underlying SMB handle but do not raise from close
        # to avoid masking original exceptions when used in a
        # context manager or finally block.
        with contextlib.suppress(Exception):
            self._handle.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        with contextlib.suppress(Exception):
            self.close()
            return None


class LocalWorkDirManager(WorkDirManager):
    def __init__(self, storage: StorageAdapter):
        self._storage = storage

    def stage_input(self, job) -> tuple[str, str, str]:
        cfg = settings.Config()
        base = ntpath.splitext(ntpath.basename(job.file_path))[0]
        output_name = base + cfg.output_suffix
        output_path = ntpath.join(ntpath.dirname(job.file_path), output_name)
        job_dir = _job_dir(job.file_path)
        os.makedirs(job_dir, exist_ok=True)
        work_input = os.path.join(job_dir, ntpath.basename(job.file_path))
        work_output = os.path.join(job_dir, output_name)

        if not self._storage.exists(job.file_path):
            raise FileNotFoundError("Input file not found")

        s1 = self._storage.stat(job.file_path)
        time.sleep(cfg.stability_seconds)
        s2 = self._storage.stat(job.file_path)
        if s1.size != s2.size or s1.mtime != s2.mtime:
            raise RuntimeError("File not stable yet")
        if s2.size == 0:
            raise RuntimeError("Input file empty")
        with (
            self._storage.open(job.file_path, "rb") as src,
            open(work_input, "wb") as dst,
        ):
            shutil.copyfileobj(src, dst)
        if os.path.getsize(work_input) == 0:
            raise RuntimeError("Input file empty after copy")

        return work_input, work_output, output_path

    def restore_output(self, job, work_output: str, output_path: str) -> None:
        with (
            open(work_output, "rb") as src,
            self._storage.open(output_path, "wb") as dst,
        ):
            shutil.copyfileobj(src, dst)

    def cleanup(self, job) -> None:
        cfg = settings.Config()
        if cfg.ocr_keep_temp:
            logging.getLogger("worker").info(
                "OCR_KEEP_TEMP enabled; preserving work dir %s",
                _job_dir(job.file_path),
            )
            return
        shutil.rmtree(_job_dir(job.file_path), ignore_errors=True)


class OcrmypdfExecutor(OcrExecutor):
    def run(self, job, input_path: str, output_path: str) -> None:
        cfg = settings.Config()

        ctx = multiprocessing.get_context("spawn")
        result_queue = ctx.Queue()
        p = ctx.Process(
            target=_run_ocr_process,
            args=(
                input_path,
                output_path,
                cfg.ocr_langs,
                cfg.ocr_output_type,
                cfg.ocr_rotate_pages,
                cfg.ocr_deskew,
                cfg.ocr_jobs,
                cfg.ocr_mode,
                cfg.ocr_verbosity,
                cfg.ocr_log_root,
                cfg.ocr_keep_temp,
                cfg.ocr_job_timeout,
                result_queue,
            ),
        )
        p.start()
        p.join(timeout=cfg.ocr_job_timeout)
        if p.is_alive():
            p.terminate()
            p.join(timeout=5)
            raise TimeoutError("OCR job timed out")

        result = None
        try:
            result = result_queue.get_nowait()
        except queue.Empty:
            result = None

        if result is None:
            raise RuntimeError(
                f"OCR job failed without result (exit code {p.exitcode})"
            )

        kind, payload = result
        if kind == "exception":
            raise RuntimeError(f"OCR job raised {payload!r}") from payload
        if kind == "exception_repr":
            raise RuntimeError(f"OCR job raised {payload}")
        if kind == "exit_code":
            if payload == ocrmypdf.ExitCode.already_done_ocr:
                logging.getLogger("worker").warning(
                    "OCR skipped due to existing text; consider OCR_MODE=redo or OCR_MODE=force"
                )
                return
            if payload != ocrmypdf.ExitCode.ok:
                raise RuntimeError(f"OCR job failed with ExitCode {payload}")
            return
        raise RuntimeError(f"OCR job returned unknown result {result!r}")


def process_job(job_id: str, file_path: str) -> None:
    cfg = settings.Config()
    store = SqliteJobStore(cfg.queue_db_path)
    if _DISPATCHER is None:
        raise RuntimeError("Dispatcher not initialized")
    job = store.get_job(job_id, _DISPATCHER)
    base = ntpath.splitext(ntpath.basename(file_path))[0]
    output_name = base + cfg.output_suffix
    output_path = ntpath.join(ntpath.dirname(file_path), output_name)
    storage = SmbStorageAdapter(
        cfg.smb_unc_path,
        cfg.smb_username,
        cfg.smb_password,
        cfg.smb_enforce_encryption,
    )
    work_mgr = LocalWorkDirManager(storage)
    if storage.exists(output_path):
        job.update_status(JobStatus.FINISHED)
        work_mgr.cleanup(job)
        return

    job.update_status(JobStatus.RUNNING)
    executor = OcrmypdfExecutor()

    try:
        try:
            work_input, work_output, output_path = work_mgr.stage_input(job)
            logger = logging.getLogger("worker")
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    "OCR input staged: path=%s size_bytes=%d md5=%s",
                    work_input,
                    os.path.getsize(work_input),
                    _file_md5(work_input),
                )
            executor.run(job, work_input, work_output)
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    "OCR output staged: path=%s size_bytes=%d md5=%s",
                    work_output,
                    os.path.getsize(work_output),
                    _file_md5(work_output),
                )
                _log_pdf_text_summary(logger, work_output)
            work_mgr.restore_output(job, work_output, output_path)
            job.update_status(JobStatus.FINISHED)
        finally:
            # Ensure local staging is removed even if OCR or restore fails.
            try:
                work_mgr.cleanup(job)
            except Exception:
                logging.getLogger("worker").exception(
                    "Failed to cleanup workdir for %s",
                    job.file_path,  # pyright: ignore[reportAttributeAccessIssue]
                )
    except Exception as exc:
        # Update persisted job status first.
        job.update_status(JobStatus.ERROR)

        # Optional: move failed input to a `failed/` folder next to the
        # original file. This behavior is controlled by the
        # `MOVE_FAILED_TO_DIR` setting. When enabled we attempt a best-effort
        # move using the storage adapter; failures to move are logged but do
        # not mask the original error.
        try:
            if cfg.move_failed_to_dir:
                dirname = ntpath.dirname(job.file_path)  # pyright: ignore[reportAttributeAccessIssue]
                base = ntpath.basename(job.file_path)  # pyright: ignore[reportAttributeAccessIssue]
                failed_dir = ntpath.join(dirname, "failed")
                failed_path = ntpath.join(failed_dir, base)
                # Ensure the storage adapter exposes rename; if not, skip.
                try:
                    storage.rename(job.file_path, failed_path)  # pyright: ignore[reportAttributeAccessIssue]
                    logging.getLogger("worker").info(
                        "Moved failed input %s -> %s",
                        job.file_path,  # pyright: ignore[reportAttributeAccessIssue]
                        failed_path,
                    )
                except Exception:
                    # If rename isn't supported (or fails), try to copy-then-delete
                    # via open/read/write if available. Best-effort only.
                    try:
                        with (
                            storage.open(job.file_path, "rb") as src,  # pyright: ignore[reportAttributeAccessIssue]
                            storage.open(
                                failed_path,
                                "wb",
                            ) as dst,
                        ):
                            shutil.copyfileobj(src, dst)
                        # Attempt to remove original if storage supports rename for deletions
                        with contextlib.suppress(Exception):
                            storage.rename(job.file_path, failed_path + ".deleted")  # pyright: ignore[reportAttributeAccessIssue]
                        logging.getLogger("worker").info(
                            "Copied failed input to %s",
                            failed_path,
                        )
                    except Exception:
                        logging.getLogger("worker").exception(
                            "Failed to move/copy failed input %s",
                            job.file_path,  # pyright: ignore[reportAttributeAccessIssue]
                        )
        except Exception:
            logging.getLogger("worker").exception(
                "Unexpected error while handling failed-input move for %s",
                job.file_path,  # pyright: ignore[reportAttributeAccessIssue]
            )

        # Classify permanent errors so retry middleware can avoid retrying them.
        # Conservative classification:
        # - File-not-found on input is permanent (don't retry)
        # - PermissionError / access errors are permanent
        try:
            msg = str(exc) if exc is not None else ""
        except Exception:
            msg = ""

        if (
            isinstance(exc, (FileNotFoundError, PermissionError))
            or msg == "Input file not found"
        ):
            raise PermanentError(msg) from exc

        # Otherwise re-raise the original exception to allow retries.
        raise


def run_worker_logic() -> None:
    logging.basicConfig(level=logging.INFO, format="[WORKER]  %(message)s")
    worker_logger = logging.getLogger("worker")
    smb_logger = logging.getLogger("smbprotocol")
    if worker_logger.isEnabledFor(logging.DEBUG):
        smb_logger.setLevel(logging.DEBUG)
    else:
        smb_logger.setLevel(logging.WARNING)
    # Validate and log config for this process (worker does not log secrets).
    try:
        cfg = settings.Config()
    except Exception:
        logging.getLogger("worker").exception("Invalid configuration")
        raise
    cfg.log(logging.getLogger("worker"))

    # Startup cleanup: remove stale work dirs older than WORK_DIR_MAX_AGE_SECONDS
    _cleanup_stale_workdirs()

    broker = create_broker(cfg)
    ocr_pdf_job = register_tasks(broker, cfg.retry_max)
    global _DISPATCHER  # noqa: PLW0603
    _DISPATCHER = DramatiqDispatcher(ocr_pdf_job)
    dramatiq.set_broker(broker)

    worker = Worker(broker, worker_threads=1)

    shutdown_event = threading.Event()

    def _handle_exit(signum, frame):
        shutdown_event.set()
        logging.getLogger("worker").info(
            "Shutdown signal %s received, stopping worker", signum
        )
        try:
            worker.stop()
        except Exception:
            logging.getLogger("worker").exception("Error stopping worker")

    _ = signal.signal(signal.SIGINT, _handle_exit)
    _ = signal.signal(signal.SIGTERM, _handle_exit)

    worker.start()
    try:
        while not shutdown_event.is_set():
            time.sleep(1)
    finally:
        # Ensure the worker is stopped if we exit this function for any reason.
        try:
            worker.stop()
        except Exception:
            logging.getLogger("worker").exception(
                "Error stopping worker during cleanup"
            )
        try:
            if hasattr(worker, "join"):
                worker.join()
            elif hasattr(worker, "threads"):
                for thread in worker.threads:  # pyright: ignore[reportAttributeAccessIssue]
                    thread.join()
        except Exception:
            logging.getLogger("worker").exception("Error joining worker threads")
