# smbmonitor

Local async SMB share monitor built on `smbprotocol`. Provides a simple `SmbMonitor` class that forwards file events to user-defined async handlers.

Handler signature: `async def handler(action, server, share, watch_path, filename) -> None`, where `action` is a `smbprotocol.change_notify.FileAction`.

Notes:

- Legacy 2-argument handlers of the form `async def handler(action, filename)` are still supported for backward compatibility; they will be detected by signature and invoked with only `(action, filename)`.
- Handlers should be non-blocking and enqueue work quickly — when `use_gather=True` the consumer awaits handlers; when `use_gather=False` handlers run in background tasks.
- `use_gather=True` (default) awaits handlers with `asyncio.gather`; set `use_gather=False` to fire-and-forget handler tasks and keep the consumer loop moving.

## Tests

Install dev dependencies and run pytest:

```bash
pip install -e ./packages/smbmonitor[dev]
pytest packages/smbmonitor/tests
```

The tests expect SMB connection environment variables to be set:
`SMB_UNC_PATH`, `SMB_USERNAME`, `SMB_PASSWORD`, and optionally `WATCH_TREE`.

Example:

- `SMB_UNC_PATH=\\\\server\\share\\folder` (UNC path format)
- `WATCH_TREE=true` to watch subdirectories (omit or set `false` for non-recursive)
