import contextlib

from dramatiq_sqlite import SQLiteBroker

import settings
from retry_middleware import configure_retries


def create_broker(config: settings.Config | None = None) -> SQLiteBroker:
    cfg = config or settings.Config()
    broker = SQLiteBroker(url=f"sqlite:///{cfg.queue_db_path}")
    # Configure retry/backoff middleware for the broker. This is a best-effort
    # attachment: configure_retries will log and no-op if dramatiq or the
    # middleware is not available in this environment (useful for tests).
    with contextlib.suppress(Exception):
        configure_retries(broker, cfg.retry_max, cfg.retry_backoff_base)
    return broker
