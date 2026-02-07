import asyncio
import logging
import ntpath

from smbprotocol.change_notify import FileAction

import settings
from broker import create_broker
from interfaces import EventSource, FileEvent, FileEventHandler, FileEventType
from jobs import JobExistsError, SqliteJobStore
from smbmonitor import SmbMonitor
from tasks import DramatiqDispatcher, register_tasks


def run_monitor_logic() -> None:
    logging.basicConfig(level=logging.INFO, format="[MONITOR] %(message)s")
    # Validate and log config for this process (monitor does not log secrets).
    logger = logging.getLogger("monitor")
    smb_logger = logging.getLogger("smbprotocol")
    if not logger.isEnabledFor(logging.DEBUG):

        class _SmbNoiseFilter(logging.Filter):
            def filter(self, record: logging.LogRecord) -> bool:
                if not record.name.startswith("smbprotocol"):
                    return True
                message = record.getMessage()
                if "Change Notify" in message:
                    return False
                return "SMB2 " not in message

        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            handler.addFilter(_SmbNoiseFilter())
        smb_logger.addFilter(_SmbNoiseFilter())
    try:
        cfg = settings.Config()
    except Exception:
        logger.exception("Invalid configuration")
        raise
    cfg.log(logger)

    broker = create_broker(cfg)
    ocr_pdf_job = register_tasks(broker, cfg.retry_max)
    # Ensure DB/schema initialized before we might enqueue messages.
    store = SqliteJobStore(cfg.queue_db_path, auto_init=True)
    dispatcher = DramatiqDispatcher(ocr_pdf_job)

    class SmbEventSource(EventSource):
        def __init__(self, unc_path, username, password, watch_tree):
            self._monitor = SmbMonitor(
                unc_path,
                [self._on_raw_event],
                username,
                password,
                watch_tree=watch_tree,
                use_gather=cfg.smb_wait_for_handlers,
                enforce_encryption=cfg.smb_enforce_encryption,
            )
            self._user_handlers = []

        def subscribe(self, handler: FileEventHandler) -> None:
            self._user_handlers.append(handler)

        async def start(self) -> None:
            await self._monitor.start()

        async def stop(self) -> None:
            await self._monitor.stop()

        async def _on_raw_event(
            self, action, server, share, watch_path, filename
        ) -> None:
            """
            Adapter for legacy SMB callbacks:

            Accepts the legacy signature `(action, server, share, watch_path, filename)`,
            maps low-level `action` values to `FileEventType` (handles rename as
            REMOVED/ADDED), builds a canonical `FileEvent` and forwards it to
            subscribers.

            This function is tolerant to receiving either a low-level `FileAction`
            value or a higher-level `FileEventType` (some caller layers may
            already have converted the action). In either case we produce a
            `FileEvent` and forward it to all user handlers concurrently.
            """
            # Map low-level FileAction values to our FileEventType.
            action_map = {
                FileAction.FILE_ACTION_ADDED: FileEventType.ADDED,
                FileAction.FILE_ACTION_REMOVED: FileEventType.REMOVED,
                FileAction.FILE_ACTION_MODIFIED: FileEventType.MODIFIED,
                FileAction.FILE_ACTION_RENAMED_OLD_NAME: FileEventType.REMOVED,
                FileAction.FILE_ACTION_RENAMED_NEW_NAME: FileEventType.ADDED,
            }

            # Determine the FileEventType. If `action` is already a
            # FileEventType (upstream converted it), use it directly; otherwise
            # map from FileAction.
            if isinstance(action, FileEventType):
                fe_type = action
            else:
                if action not in action_map:
                    return
                fe_type = action_map[action]

            event = FileEvent(
                path=self._canonicalize_path(server, share, watch_path, filename),
                event=fe_type,
                source="smb",
            )

            if not self._user_handlers:
                return

            # Forward the FileEvent to all user handlers concurrently.
            _ = await asyncio.gather(
                *(handler(event) for handler in self._user_handlers)
            )

        def _canonicalize_path(self, server, share, watch_path, filename) -> str:
            parts = [server, share] + ([watch_path] if watch_path else []) + [filename]
            unc = "\\" + "\\".join(p for p in parts if p)
            return ntpath.normpath(unc)

    async def on_file_added(event: FileEvent) -> None:
        if event.event != FileEventType.ADDED:
            return
        if not event.path.lower().endswith(".pdf"):
            return
        if event.path.lower().endswith(cfg.output_suffix.lower()):
            logger.info("Skipping output file %s", event.path)
            return
        try:
            job = await asyncio.to_thread(store.create_job, event.path, dispatcher)
        except JobExistsError:
            return
        _ = job.execute()

    source = SmbEventSource(
        cfg.smb_unc_path,
        cfg.smb_username,
        cfg.smb_password,
        cfg.watch_tree,
    )
    source.subscribe(on_file_added)

    async def runner() -> None:
        await source.start()
        try:
            _ = await asyncio.Event().wait()
        except asyncio.CancelledError:
            logger.debug("Monitor task cancelled; shutting down")
            return

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        logger.debug("Monitor interrupted; shutting down")
        return
