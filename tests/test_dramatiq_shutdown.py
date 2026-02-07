import importlib
import sqlite3
import sys
import time
from pathlib import Path

import dramatiq
import pytest
from dramatiq import Worker
from dramatiq.common import dq_name

APP_ROOT = Path(__file__).resolve().parents[1] / "app"
sys.path.append(str(APP_ROOT))


def _load_modules():
    import broker  # noqa: E402
    import dramatiq_sqlite  # noqa: E402
    import settings  # noqa: E402

    importlib.reload(dramatiq_sqlite)
    importlib.reload(settings)
    importlib.reload(broker)
    return settings, broker


@pytest.mark.error(
    id="E001",
    description="Patched dramatiq_sqlite requeue succeeds on shutdown.",
)
def test_shutdown_requeue_handles_delay_queue(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "ocr_queue.sqlite"
    monkeypatch.setenv("QUEUE_DB_PATH", str(db_path))
    monkeypatch.setenv("SMB_UNC_PATH", r"\\server\share")
    settings, broker = _load_modules()
    settings.Config._instance = None

    broker_instance = broker.create_broker(settings.Config())
    dramatiq.set_broker(broker_instance)

    @dramatiq.actor(queue_name="default")
    def noop():
        return None

    worker = Worker(broker_instance, worker_threads=1)
    worker.start()
    try:
        noop.send_with_options(delay=60_000)

        delay_queue_name = dq_name("default")
        deadline = time.time() + 5
        delay_consumer = None
        while time.time() < deadline:
            delay_consumer = worker.consumers.get(delay_queue_name)
            if delay_consumer and delay_consumer.delay_queue.qsize() > 0:
                break
            time.sleep(0.05)

        assert delay_consumer is not None, "Delay queue consumer not started"
        assert (
            delay_consumer.delay_queue.qsize() > 0
        ), "Delayed message not staged in delay queue"
    finally:
        worker.stop()
        settings.Config._instance = None


@pytest.mark.error(
    id="E001",
    description="Unpatched dramatiq_sqlite requeue fails on shutdown.",
)
@pytest.mark.xfail(
    raises=sqlite3.OperationalError,
    reason="Upstream dramatiq_sqlite requeue bug",
    strict=True,
)
def test_shutdown_requeue_fails_without_patch(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "ocr_queue.sqlite"
    monkeypatch.setenv("QUEUE_DB_PATH", str(db_path))
    monkeypatch.setenv("SMB_UNC_PATH", r"\\server\share")
    monkeypatch.setenv("DISABLE_DRAMATIQ_SQLITE_REQUEUE_PATCH", "1")
    settings, broker = _load_modules()
    settings.Config._instance = None

    broker_instance = broker.create_broker(settings.Config())
    dramatiq.set_broker(broker_instance)

    @dramatiq.actor(queue_name="default")
    def noop():
        return None

    worker = Worker(broker_instance, worker_threads=1)
    worker.start()
    try:
        noop.send_with_options(delay=60_000)

        delay_queue_name = dq_name("default")
        deadline = time.time() + 5
        delay_consumer = None
        while time.time() < deadline:
            delay_consumer = worker.consumers.get(delay_queue_name)
            if delay_consumer and delay_consumer.delay_queue.qsize() > 0:
                break
            time.sleep(0.05)

        assert delay_consumer is not None, "Delay queue consumer not started"
        assert (
            delay_consumer.delay_queue.qsize() > 0
        ), "Delayed message not staged in delay queue"
    finally:
        worker.stop()
        settings.Config._instance = None
