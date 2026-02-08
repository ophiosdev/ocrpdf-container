import sys
from pathlib import Path

import pytest

APP_ROOT = Path(__file__).resolve().parents[1] / "app"
sys.path.append(str(APP_ROOT))

import settings  # noqa: E402
from retry_middleware import (  # noqa: E402
    PermanentError,
    _has_exception_type,
    configure_retries,
)
from worker import LocalWorkDirManager  # noqa: E402


@pytest.mark.error(
    id="E002",
    description="PermanentError is detected through exception chaining.",
)
def test_permanent_error_detected_in_chain() -> None:
    try:
        try:
            raise PermanentError("missing input")
        except PermanentError as exc:
            raise RuntimeError("wrapped") from exc
    except RuntimeError as exc:
        assert _has_exception_type(exc, PermanentError)


@pytest.mark.error(
    id="E002",
    description="Missing input raises FileNotFoundError early in staging.",
)
def test_stage_input_missing_file_raises_filenotfound(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SMB_UNC_PATH", r"\\server\share")
    monkeypatch.setenv("QUEUE_DB_PATH", str(tmp_path / "ocr_queue.sqlite"))
    monkeypatch.setenv("WORK_DIR", str(tmp_path / "work"))
    settings.Config._instance = None

    class FakeStorage:
        def exists(self, path: str) -> bool:
            return False

    class Job:
        def __init__(self, file_path: str) -> None:
            self.file_path = file_path

    manager = LocalWorkDirManager(FakeStorage())
    job = Job(r"\\server\share\missing.pdf")

    with pytest.raises(FileNotFoundError, match="Input file not found"):
        manager.stage_input(job)


@pytest.mark.error(
    id="E002",
    description="Retries middleware skips PermanentError and chained PermanentError.",
)
def test_retries_middleware_skips_permanent_error() -> None:
    class DummyBroker:
        def __init__(self) -> None:
            self.middleware = []

        def add_middleware(self, mw) -> None:
            self.middleware.append(mw)

    broker = DummyBroker()
    configure_retries(broker, retry_max=1, backoff_base=2)
    retries = next(
        (mw for mw in broker.middleware if mw.__class__.__name__ == "Retries"),
        None,
    )
    assert retries is not None, "Retries middleware not attached"

    assert retries.retry_when(0, PermanentError("missing")) is False

    try:
        try:
            raise PermanentError("missing")
        except PermanentError as exc:
            raise RuntimeError("wrapped") from exc
    except RuntimeError as exc:
        chained_exc = exc

    assert retries.retry_when(0, chained_exc) is False
