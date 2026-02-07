import logging
import os

from singleton import Singleton


class Config(metaclass=Singleton):
    __slots__ = (
        "_smb_unc_path",
        "_smb_username",
        "_smb_password",
        "_watch_tree",
        "_smb_wait_for_handlers",
        "_smb_enforce_encryption",
        "_queue_db_path",
        "_work_dir",
        "_output_suffix",
        "_stability_seconds",
        "_max_workers",
        "_ocr_jobs",
        "_ocr_langs",
        "_ocr_output_type",
        "_ocr_rotate_pages",
        "_ocr_deskew",
        "_ocr_mode",
        "_ocr_verbosity",
        "_ocr_log_root",
        "_ocr_keep_temp",
        "_move_failed_to_dir",
        "_retry_max",
        "_retry_backoff_base",
        "_ocr_job_timeout",
        "_work_dir_max_age_seconds",
    )

    def __init__(self) -> None:
        self._smb_unc_path = os.getenv("SMB_UNC_PATH", "")
        self._smb_username = os.getenv("SMB_USERNAME")
        self._smb_password = os.getenv("SMB_PASSWORD")
        self._watch_tree = os.getenv("WATCH_TREE", "false").lower() == "true"
        self._smb_wait_for_handlers = (
            os.getenv("SMB_WAIT_FOR_HANDLERS", "true").lower() == "true"
        )
        self._smb_enforce_encryption = (
            os.getenv("SMB_ENFORCE_ENCRYPTION", "false").lower() == "true"
        )

        self._queue_db_path = os.getenv("QUEUE_DB_PATH", "/data/ocr_queue.sqlite")
        self._work_dir = os.getenv("WORK_DIR", "/data/work")
        self._output_suffix = os.getenv("OUTPUT_SUFFIX", "_ocr.pdf")
        self._stability_seconds = int(os.getenv("STABILITY_SECONDS", "5"))
        self._work_dir_max_age_seconds = int(
            os.getenv("WORK_DIR_MAX_AGE_SECONDS", str(24 * 3600))
        )

        self._max_workers = int(os.getenv("MAX_WORKERS", "2"))
        self._ocr_jobs = int(os.getenv("OCR_JOBS", "2"))
        self._ocr_langs = os.getenv("OCR_LANGS", "deu+eng")
        self._ocr_output_type = os.getenv("OCR_OUTPUT_TYPE", "pdf")
        self._ocr_rotate_pages = os.getenv("OCR_ROTATE_PAGES", "true").lower() == "true"
        self._ocr_deskew = os.getenv("OCR_DESKEW", "true").lower() == "true"
        self._ocr_mode = os.getenv("OCR_MODE", "skip").strip().lower()
        self._ocr_verbosity = os.getenv("OCR_VERBOSITY", "default").strip().lower()
        self._ocr_log_root = os.getenv("OCR_LOG_ROOT", "false").lower() == "true"
        self._ocr_keep_temp = os.getenv("OCR_KEEP_TEMP", "false").lower() == "true"

        self._retry_max = int(os.getenv("RETRY_MAX", "4"))
        self._retry_backoff_base = int(os.getenv("RETRY_BACKOFF_BASE", "2"))
        self._ocr_job_timeout = int(os.getenv("OCR_JOB_TIMEOUT", "3600"))

        # Failure handling policy toggle.
        # If True, the worker will attempt to move failed input files into a sibling
        # `failed/` directory next to the original file. Default is False to keep the
        # workflow non-destructive and to only record failures in the persistent DB.
        self._move_failed_to_dir = (
            os.getenv("MOVE_FAILED_TO_DIR", "false").lower() == "true"
        )
        self.validate()

    @property
    def smb_unc_path(self) -> str:
        return self._smb_unc_path

    @property
    def smb_username(self) -> str | None:
        return self._smb_username

    @property
    def smb_password(self) -> str | None:
        return self._smb_password

    @property
    def watch_tree(self) -> bool:
        return self._watch_tree

    @property
    def smb_wait_for_handlers(self) -> bool:
        return self._smb_wait_for_handlers

    @property
    def smb_enforce_encryption(self) -> bool:
        return self._smb_enforce_encryption

    @property
    def queue_db_path(self) -> str:
        return self._queue_db_path

    @property
    def work_dir(self) -> str:
        return self._work_dir

    @property
    def output_suffix(self) -> str:
        return self._output_suffix

    @property
    def stability_seconds(self) -> int:
        return self._stability_seconds

    @property
    def max_workers(self) -> int:
        return self._max_workers

    @property
    def ocr_jobs(self) -> int:
        return self._ocr_jobs

    @property
    def ocr_langs(self) -> str:
        return self._ocr_langs

    @property
    def ocr_output_type(self) -> str:
        return self._ocr_output_type

    @property
    def ocr_rotate_pages(self) -> bool:
        return self._ocr_rotate_pages

    @property
    def ocr_deskew(self) -> bool:
        return self._ocr_deskew

    @property
    def ocr_mode(self) -> str:
        return self._ocr_mode

    @property
    def ocr_verbosity(self) -> str:
        return self._ocr_verbosity

    @property
    def ocr_log_root(self) -> bool:
        return self._ocr_log_root

    @property
    def ocr_keep_temp(self) -> bool:
        return self._ocr_keep_temp

    @property
    def move_failed_to_dir(self) -> bool:
        return self._move_failed_to_dir

    @property
    def retry_max(self) -> int:
        return self._retry_max

    @property
    def retry_backoff_base(self) -> int:
        return self._retry_backoff_base

    @property
    def ocr_job_timeout(self) -> int:
        return self._ocr_job_timeout

    @property
    def work_dir_max_age_seconds(self) -> int:
        return self._work_dir_max_age_seconds

    def validate(self) -> None:
        """Validate configuration values and raise ValueError on invalid settings.

        This function performs light, fail-fast validation for common misconfiguration
        (types/ranges and required values). It is safe to call at process startup.
        """
        errors = []
        if not isinstance(self.max_workers, int) or self.max_workers < 1:
            errors.append("MAX_WORKERS must be an integer >= 1")
        if not isinstance(self.ocr_jobs, int) or self.ocr_jobs < 1:
            errors.append("OCR_JOBS must be an integer >= 1")
        if not isinstance(self.retry_max, int) or self.retry_max < 0:
            errors.append("RETRY_MAX must be a non-negative integer")
        if not isinstance(self.retry_backoff_base, int) or self.retry_backoff_base < 1:
            errors.append("RETRY_BACKOFF_BASE must be an integer >= 1")
        if not isinstance(self.stability_seconds, int) or self.stability_seconds < 0:
            errors.append("STABILITY_SECONDS must be an integer >= 0")
        if not isinstance(self.ocr_job_timeout, int) or self.ocr_job_timeout <= 0:
            errors.append("OCR_JOB_TIMEOUT must be an integer > 0")
        valid_modes = {"default", "skip", "redo", "force"}
        if not isinstance(self.ocr_mode, str) or self.ocr_mode not in valid_modes:
            errors.append("OCR_MODE must be one of default|skip|redo|force")
        valid_verbosity = {"quiet", "default", "debug", "debug_all"}
        if (
            not isinstance(self.ocr_verbosity, str)
            or self.ocr_verbosity not in valid_verbosity
        ):
            errors.append("OCR_VERBOSITY must be one of quiet|default|debug|debug_all")
        if (
            not isinstance(self.work_dir_max_age_seconds, int)
            or self.work_dir_max_age_seconds < 0
        ):
            errors.append("WORK_DIR_MAX_AGE_SECONDS must be an integer >= 0")
        if not self.queue_db_path:
            errors.append("QUEUE_DB_PATH must be set to a valid filesystem path")
        if not self.work_dir:
            errors.append("WORK_DIR must be set to a valid filesystem path")
        if not self.smb_unc_path:
            errors.append(
                "SMB_UNC_PATH must be set to a UNC path (e.g. \\\\server\\share)"
            )

        if errors:
            raise ValueError("Invalid configuration: " + "; ".join(errors))

    def log(self, logger) -> None:
        """Log the resolved config (excluding secrets) at INFO level.

        Callers must provide a logger instance. This prints a stable view of
        configuration to help with diagnostics while avoiding leaking secrets.
        """
        if not logger.isEnabledFor(logging.INFO):
            return
        values = {
            "smb_unc_path": self.smb_unc_path,
            "smb_username": self.smb_username,
            "watch_tree": self.watch_tree,
            "smb_wait_for_handlers": self.smb_wait_for_handlers,
            "smb_enforce_encryption": self.smb_enforce_encryption,
            "queue_db_path": self.queue_db_path,
            "work_dir": self.work_dir,
            "output_suffix": self.output_suffix,
            "stability_seconds": self.stability_seconds,
            "max_workers": self.max_workers,
            "ocr_jobs": self.ocr_jobs,
            "ocr_langs": self.ocr_langs,
            "ocr_output_type": self.ocr_output_type,
            "ocr_rotate_pages": self.ocr_rotate_pages,
            "ocr_deskew": self.ocr_deskew,
            "ocr_mode": self.ocr_mode,
            "ocr_verbosity": self.ocr_verbosity,
            "ocr_log_root": self.ocr_log_root,
            "ocr_keep_temp": self.ocr_keep_temp,
            "move_failed_to_dir": self.move_failed_to_dir,
            "retry_max": self.retry_max,
            "retry_backoff_base": self.retry_backoff_base,
            "ocr_job_timeout": self.ocr_job_timeout,
            "work_dir_max_age_seconds": self.work_dir_max_age_seconds,
        }
        # Log each key=value pair on a single line for easy grepping.
        logger.info("Resolved configuration:")
        for k in sorted(values.keys()):
            logger.info("  %s=%r", k, values[k])
