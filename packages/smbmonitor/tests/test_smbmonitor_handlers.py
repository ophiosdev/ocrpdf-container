import asyncio
import os

import pytest

from smbmonitor import FileAction, SmbMonitor


def _unc_path() -> str:
    unc_path = os.environ.get("SMB_UNC_PATH", r"\\server\share\folder\sub")
    return unc_path


async def handler_full(action, server, share, watch_path, filename) -> None:
    return None


async def handler_legacy(action, filename) -> None:
    return None


async def handler_invalid(action, server, share) -> None:
    return None


def test_accepts_full_handler() -> None:
    monitor = SmbMonitor(_unc_path(), [handler_full])
    assert monitor._handler_specs[0][1] == "full"
    assert monitor._server
    assert monitor._share


def test_accepts_legacy_handler() -> None:
    monitor = SmbMonitor(_unc_path(), [handler_legacy])
    assert monitor._handler_specs[0][1] == "legacy"


def test_rejects_invalid_handler() -> None:
    with pytest.raises(TypeError):
        SmbMonitor(_unc_path(), [handler_invalid])


def test_parse_unc_path_default() -> None:
    monitor = SmbMonitor(r"\\server\share\folder\sub", [handler_full])
    assert monitor._server == "server"
    assert monitor._share == "share"
    assert monitor._watch_path == "folder\\sub"


class _Field:
    def __init__(self, value):
        self.value = value


class _Event:
    def __init__(self, action, filename):
        self.fields = {
            "action": _Field(action),
            "file_name": _Field(filename),
        }


def test_use_gather_waits_for_handlers() -> None:
    async def _run() -> None:
        started = asyncio.Event()
        proceed = asyncio.Event()
        done = asyncio.Event()

        async def handler(action, server, share, watch_path, filename) -> None:
            started.set()
            await proceed.wait()
            done.set()

        monitor = SmbMonitor(_unc_path(), [handler], use_gather=True)
        event = _Event(FileAction.FILE_ACTION_ADDED, "file.pdf")

        consumer_task = asyncio.create_task(monitor._consumer())
        await monitor._queue.put([event])

        await asyncio.wait_for(started.wait(), timeout=1.0)
        join_task = asyncio.create_task(monitor._queue.join())
        await asyncio.sleep(0)
        assert not join_task.done()

        proceed.set()
        await asyncio.wait_for(join_task, timeout=1.0)
        await asyncio.wait_for(done.wait(), timeout=1.0)

        monitor._stop_event.set()
        await asyncio.wait_for(consumer_task, timeout=2.0)

    asyncio.run(_run())


def test_use_gather_false_does_not_block_queue_join() -> None:
    async def _run() -> None:
        started = asyncio.Event()
        proceed = asyncio.Event()
        done = asyncio.Event()

        async def handler(action, server, share, watch_path, filename) -> None:
            started.set()
            await proceed.wait()
            done.set()

        monitor = SmbMonitor(_unc_path(), [handler], use_gather=False)
        event = _Event(FileAction.FILE_ACTION_ADDED, "file.pdf")

        consumer_task = asyncio.create_task(monitor._consumer())
        await monitor._queue.put([event])

        await asyncio.wait_for(started.wait(), timeout=1.0)
        join_task = asyncio.create_task(monitor._queue.join())
        await asyncio.wait_for(join_task, timeout=1.0)
        assert join_task.done()

        proceed.set()
        await asyncio.wait_for(done.wait(), timeout=1.0)

        monitor._stop_event.set()
        await asyncio.wait_for(consumer_task, timeout=2.0)

    asyncio.run(_run())


def test_stop_graceful_waits_for_active_tasks() -> None:
    async def _run() -> None:
        monitor = SmbMonitor(_unc_path(), [handler_full])
        release = asyncio.Event()

        async def _task() -> None:
            await release.wait()

        monitor._watcher_task = asyncio.create_task(_task())
        monitor._consumer_task = asyncio.create_task(_task())

        stop_task = asyncio.create_task(monitor.stop(graceful=True))
        await asyncio.sleep(0)
        assert not stop_task.done()
        assert monitor._stop_event.is_set()

        release.set()
        await asyncio.wait_for(stop_task, timeout=1.0)
        assert monitor._watcher_task.done()
        assert monitor._consumer_task.done()

    asyncio.run(_run())


def test_stop_graceful_cancels_pending_tasks_after_timeout() -> None:
    async def _run() -> None:
        monitor = SmbMonitor(_unc_path(), [handler_full])
        started = asyncio.Event()

        async def _never_finishes() -> None:
            started.set()
            await asyncio.Future()

        monitor._watcher_task = asyncio.create_task(_never_finishes())
        await asyncio.wait_for(started.wait(), timeout=1.0)

        await monitor.stop(graceful=True)

        with pytest.raises(asyncio.CancelledError):
            await monitor._watcher_task

        assert monitor._watcher_task.cancelled()

    asyncio.run(_run())


def test_stop_graceful_handles_missing_consumer_task() -> None:
    async def _run() -> None:
        monitor = SmbMonitor(_unc_path(), [handler_full])
        finished = asyncio.Event()

        async def _task() -> None:
            finished.set()

        monitor._watcher_task = asyncio.create_task(_task())
        monitor._consumer_task = None

        await monitor.stop(graceful=True)

        assert monitor._stop_event.is_set()
        await asyncio.wait_for(finished.wait(), timeout=1.0)

    asyncio.run(_run())
