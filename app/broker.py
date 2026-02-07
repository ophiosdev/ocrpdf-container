import contextlib
import os
import time

import dramatiq_sqlite
from dramatiq_sqlite import SQLiteBroker

import settings
from retry_middleware import configure_retries


def _patch_dramatiq_sqlite_requeue() -> None:
    if os.getenv("DISABLE_DRAMATIQ_SQLITE_REQUEUE_PATCH") is not None:
        return
    if getattr(dramatiq_sqlite, "_ocrmypdf_requeue_patched", False):
        return

    consumer_cls = getattr(dramatiq_sqlite, "SQLiteConsumer", None)
    if consumer_cls is None:
        return

    def _requeue(self, messages):
        messages = list(messages)
        if not messages:
            return

        message_ids = [m.message_id for m in messages]
        placeholders = ", ".join("?" for _ in message_ids)
        sql = f"""
        UPDATE dramatiq_messages
        SET state = 'queued', mtime = ?
        WHERE message_id IN ({placeholders})
        """
        with self.db as cursor:
            dramatiq_sqlite.logger.debug("Batch update of messages for requeue.")
            cursor.execute(sql, (int(time.time()), *message_ids))

    consumer_cls.requeue = _requeue
    dramatiq_sqlite._ocrmypdf_requeue_patched = True  # pyright: ignore[reportAttributeAccessIssue]  # noqa: SLF001


def create_broker(config: settings.Config | None = None) -> SQLiteBroker:
    cfg = config or settings.Config()
    _patch_dramatiq_sqlite_requeue()
    broker = SQLiteBroker(url=f"sqlite:///{cfg.queue_db_path}")
    # Configure retry/backoff middleware for the broker. This is a best-effort
    # attachment: configure_retries will log and no-op if dramatiq or the
    # middleware is not available in this environment (useful for tests).
    with contextlib.suppress(Exception):
        configure_retries(broker, cfg.retry_max, cfg.retry_backoff_base)
    return broker
