"""Retry/backoff middleware configuration and helpers.

This module provides a small compatibility layer around Dramatiq's
Retries middleware and a `PermanentError` exception type used to mark
non-retryable failures.

The runtime wiring is defensive: we try to instantiate the built-in
`Retries` middleware when available and adapt to common constructor
signatures. If Dramatiq is not installed or the middleware shape differs
we log a warning and skip middleware installation (workers will still
run but without automated backoff).
"""

from __future__ import annotations

import inspect
import logging
from typing import Callable


class PermanentError(Exception):
    """Raise this from worker code to indicate a permanent failure.

    PermanentError signals that the failed message should NOT be
    retried. The retry configuration below will try to mark this
    exception class as non-retryable when attaching the middleware.
    """


def _has_exception_type(
    exc: BaseException | None, exc_type: type[BaseException]
) -> bool:
    """Return True if exc or any chained exception matches exc_type."""
    seen: set[int] = set()
    current = exc
    while current is not None:
        exc_id = id(current)
        if exc_id in seen:
            break
        seen.add(exc_id)
        if isinstance(current, exc_type):
            return True
        current = current.__cause__ or current.__context__
    return False


def _binary_backoff_fn(backoff_base: int) -> Callable[[int], int]:
    def backoff(attempt: int) -> int:
        try:
            a = int(attempt)
        except Exception:
            a = 0
        # Binary exponential backoff: base ** attempt
        delay = backoff_base**a
        # Cap at 1 hour (3600s)
        return min(int(delay), 3600)

    return backoff


def configure_retries(broker, retry_max: int, backoff_base: int) -> None:
    """Attach a retries middleware to `broker` using binary exponential backoff.

    The concrete Dramatiq `Retries` middleware has changed shape across
    versions. This function performs runtime introspection to provide a
    best-effort configuration:

    - prefer providing a `backoff` callable that implements
      `delay = backoff_base ** attempt` capped at 3600s
    - set the configured max retries where supported
    - mark `PermanentError` as non-retryable using available hooks
      (e.g. `fatal_exceptions`, `exclude`, or `retry_when`)

    If the middleware or broker API is not present we log and return
    without raising to keep startup resilient.
    """
    logger = logging.getLogger("retry")

    try:
        from dramatiq.middleware import Retries  # noqa: PLC0415
    except Exception as exc:  # pragma: no cover - optional dependency
        logger.warning("Dramatiq Retries middleware not available: %s", exc)
        return

    try:
        existing = getattr(broker, "middleware", None)
        if isinstance(existing, list) and any(
            isinstance(mw, Retries) for mw in existing
        ):
            logger.info("Retries middleware already attached; skipping")
            return
    except Exception:
        pass

    backoff = _binary_backoff_fn(backoff_base)

    kwargs = {}
    try:
        sig = inspect.signature(Retries.__init__)
        params = sig.parameters
        if "backoff" in params:
            kwargs["backoff"] = backoff
        if "max_retries" in params:
            kwargs["max_retries"] = retry_max
        elif "max_attempts" in params:
            kwargs["max_attempts"] = retry_max

        # Prefer to mark PermanentError as fatal/non-retryable if the
        # middleware exposes such an option.
        if "fatal_exceptions" in params:
            kwargs["fatal_exceptions"] = (PermanentError,)
        elif "exclude" in params:
            kwargs["exclude"] = (PermanentError,)
        elif "retry_when" in params:
            # Some versions accept a callable to decide retryability.
            # Dramatiq 2.x passes (attempt, exc); accept both shapes.
            def retry_when(*args) -> bool:
                exc = None
                if len(args) == 1:
                    exc = args[0]
                elif len(args) >= 2:  # noqa: PLR2004
                    exc = args[1]
                return not _has_exception_type(exc, PermanentError)

            kwargs["retry_when"] = retry_when
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("Could not introspect Retries constructor: %s", exc)

    try:
        mw = Retries(**kwargs)
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("Failed to instantiate Retries middleware: %s", exc)
        return

    # Attach to broker using common extension points.
    try:
        if hasattr(broker, "add_middleware"):
            broker.add_middleware(mw)
        elif hasattr(broker, "middleware") and isinstance(broker.middleware, list):
            broker.middleware.append(mw)
        else:
            logger.warning(
                "Broker does not support middleware attachment; skipping retry middleware"
            )
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("Failed to attach retry middleware to broker: %s", exc)
