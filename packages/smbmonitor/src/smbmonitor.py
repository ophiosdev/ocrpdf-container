import asyncio
import logging
import ntpath
from inspect import Signature, signature
from typing import Awaitable, Callable, List, Tuple
from uuid import uuid4

from smbprotocol.change_notify import (
    ChangeNotifyFlags,
    CompletionFilter,
    FileAction,
    FileSystemWatcher,
)
from smbprotocol.connection import Connection
from smbprotocol.open import (
    CreateDisposition,
    CreateOptions,
    DirectoryAccessMask,
    FileAttributes,
    ImpersonationLevel,
    Open,
    ShareAccess,
)
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect

log = logging.getLogger(__name__)


class SmbMonitor:
    """
    Monitors an SMB share for file events, processing them with async handlers while minimizing watcher recreation time.

    Args:
        unc_path (str): UNC path to monitor (e.g., '\\\\server\\share\\path').
        handlers (List[Callable[..., Awaitable[None]]]): Async handlers receiving either
            (action, server, share, watch_path, filename) or legacy (action, filename).
        username (Optional[str]): SMB authentication username.
        password (Optional[str]): SMB authentication password.
        watch_tree (Optional[bool]): Whether to watch the entire directory tree (True) or just the specified path (False). Defaults to False.
        use_gather (Optional[bool]): Whether to await handlers with asyncio.gather (True) or fire-and-forget tasks (False). Defaults to True.
        enforce_encryption (Optional[bool]): Whether to require SMB session encryption. Defaults to False.
        port (Optional[int]): The port number to connect to (default is 445 for SMB).
    """

    def __init__(
        self,
        unc_path: str,
        handlers: List[Callable[..., Awaitable[None]]],
        username: str | None = None,
        password: str | None = None,
        watch_tree: bool = False,
        use_gather: bool = True,
        enforce_encryption: bool = False,
        port: int = 445,
    ):
        if not handlers:
            raise ValueError("At least one handler must be specified")

        # Validate each handler's signature
        handler_specs: List[Tuple[Callable[..., Awaitable[None]], str]] = []
        for handler in handlers:
            # Check if the handler is callable
            if not callable(handler):
                raise TypeError(f"Handler {handler} is not callable")

            # Check if the handler is an async function
            if not asyncio.iscoroutinefunction(handler):  # pyright: ignore[reportDeprecated]
                raise TypeError(f"Handler {handler} is not a coroutine function")

            # Inspect the handler's signature
            def _can_bind_args(sig: Signature, arg_count: int) -> bool:
                try:
                    _ = sig.bind(*([object()] * arg_count))
                except TypeError:
                    return False
                return True

            sig = signature(handler)
            accepts_full = _can_bind_args(sig, 5)
            accepts_legacy = _can_bind_args(sig, 2)

            if accepts_full:
                handler_specs.append((handler, "full"))
            elif accepts_legacy:
                handler_specs.append((handler, "legacy"))
            else:
                raise TypeError(
                    f"Handler {handler} must accept either 5 arguments "
                    "(action, server, share, watch_path, filename) or legacy "
                    "2 arguments (action, filename)"
                )

        self._unc_path = unc_path
        self._handler_specs = handler_specs
        self._username = username
        self._password = password
        self._watch_tree = watch_tree
        self._use_gather = use_gather
        self._enforce_encryption = enforce_encryption
        self._server, self._share, self._watch_path = self._parse_unc_path(unc_path)
        self._stop_event = asyncio.Event()
        self._watcher_task = None
        self._consumer_task = None
        self._queue = asyncio.Queue()
        self._port = port
        self._uuid = uuid4()

    def _parse_unc_path(
        self,
        unc_path: str,
    ) -> tuple[str, str, str]:
        """Parse the UNC path into server, share, and watch_path components."""
        unc_path = ntpath.normpath(unc_path)
        parts = [p for p in unc_path.split("\\") if p]
        if len(parts) < 2:  # noqa: PLR2004
            raise ValueError(
                "The SMB path specified must contain the server and share to connect to"
            )
        server, share = parts[0], parts[1]
        watch_path = "\\".join(parts[2:])
        return server, share, watch_path

    async def _watcher(self):
        """Task to create watchers and capture events, managing the SMB connection."""
        conn = Connection(
            guid=self._uuid,
            port=self._port,
            server_name=self._server,
        )
        conn.connect()
        try:
            session = Session(
                conn,
                self._username,
                self._password,
                self._enforce_encryption,
            )
            session.connect()
            tree = TreeConnect(session, rf"\\{self._server}\{self._share}")
            tree.connect()
            open_ = Open(tree, self._watch_path)
            _ = open_.create(
                ImpersonationLevel.Impersonation,
                DirectoryAccessMask.GENERIC_READ,
                FileAttributes.FILE_ATTRIBUTE_DIRECTORY,
                ShareAccess.FILE_SHARE_READ,
                CreateDisposition.FILE_OPEN_IF,
                CreateOptions.FILE_DIRECTORY_FILE,
            )
            if not open_.connected:
                raise RuntimeError("Failed to connect to the specified path")

            while not self._stop_event.is_set():
                watcher = FileSystemWatcher(open_)
                _ = watcher.start(
                    completion_filter=(
                        CompletionFilter.FILE_NOTIFY_CHANGE_FILE_NAME
                        | CompletionFilter.FILE_NOTIFY_CHANGE_LAST_WRITE
                        | CompletionFilter.FILE_NOTIFY_CHANGE_SIZE
                    ),
                    flags=ChangeNotifyFlags.SMB2_WATCH_TREE
                    if self._watch_tree
                    else ChangeNotifyFlags.NONE,
                )
                _ = await asyncio.to_thread(watcher.wait)
                events = watcher.result
                await self._queue.put(events)
            log.info("Stopping watcher")
        finally:
            conn.disconnect()  # Ensures connection closes when task ends

    async def _consumer(self):
        """Task to process events from the queue, continuing until queue is empty."""

        async def _run_handler(coro: Awaitable[None]) -> None:
            try:
                await coro
            except Exception:
                log.exception("Error in SMB event handler")

        while not self._stop_event.is_set():
            try:
                events = await asyncio.wait_for(
                    self._queue.get(),
                    timeout=1.0,
                )
            except asyncio.TimeoutError:
                continue
            # Schedule handlers for each event as non-blocking tasks.
            # Build a canonical 5-tuple payload for each watcher event and
            # forward it to registered handlers. Legacy handlers (2-arg)
            # are detected at init and called with (action, filename) only.
            for event in events:
                filename = event.fields["file_name"].value
                action = event.fields["action"].value

                # Only handle the known/allowed FileAction values.
                if action not in (
                    FileAction.FILE_ACTION_ADDED,
                    FileAction.FILE_ACTION_REMOVED,
                    FileAction.FILE_ACTION_MODIFIED,
                    FileAction.FILE_ACTION_RENAMED_OLD_NAME,
                    FileAction.FILE_ACTION_RENAMED_NEW_NAME,
                ):
                    continue

                payload = (
                    action,
                    self._server,
                    self._share,
                    self._watch_path,
                    filename,
                )

                if self._use_gather:
                    coros = []
                    for handler, mode in self._handler_specs:
                        if mode == "legacy":
                            coros.append(handler(action, filename))
                        else:
                            coros.append(handler(*payload))
                    if coros:
                        results = await asyncio.gather(
                            *coros,
                            return_exceptions=True,
                        )
                        for result in results:
                            if isinstance(result, Exception):
                                log.exception(
                                    "Error in SMB event handler",
                                    exc_info=result,
                                )
                else:
                    for handler, mode in self._handler_specs:
                        if mode == "legacy":
                            coro = handler(action, filename)
                        else:
                            coro = handler(*payload)
                        _ = asyncio.create_task(_run_handler(coro))  # noqa: RUF006
            self._queue.task_done()
        log.info("Stopping consumer")

    async def start(self) -> None:
        """Start monitoring the SMB share asynchronously without blocking the caller."""
        if self._watcher_task and not self._watcher_task.done():
            log.warning("Monitoring already in progress")
            return

        log.info(
            f"Starting monitoring for '{self._watch_path}' on '{self._server}\\{self._share}'"
        )
        self._stop_event.clear()
        self._watcher_task = asyncio.create_task(self._watcher())
        self._consumer_task = asyncio.create_task(self._consumer())

    async def stop(
        self,
        graceful: bool | None = True,
    ) -> None:
        """
        Stop monitoring by setting the stop event, allowing tasks to finish processing.

        Args:
            graceful (Optional[bool]): Whether to wait for tasks to finish processing before stopping. Defaults to True.
        """
        if not self._watcher_task or self._watcher_task.done():
            log.warning("No active monitoring tasks to stop")
            return

        log.info("Stopping SMB monitoring")
        if graceful:
            self._stop_event.set()
            _, pending = await asyncio.wait(
                (self._watcher_task, self._consumer_task),  # pyright: ignore[reportArgumentType, reportCallIssue]
                timeout=2.0,
                return_when=asyncio.ALL_COMPLETED,
            )
            for task in pending:
                task.cancel()
        else:
            _ = self._watcher_task.cancel() if self._consumer_task else None
            _ = self._consumer_task.cancel() if self._consumer_task else None

    def is_running(self) -> bool:
        """Check if the monitoring tasks are currently running."""
        return self._watcher_task and not self._watcher_task.done()  # pyright: ignore[reportReturnType]
