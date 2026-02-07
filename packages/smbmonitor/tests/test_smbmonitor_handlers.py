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
