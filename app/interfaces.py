from dataclasses import dataclass
from enum import Enum
from typing import BinaryIO, List, NamedTuple, Optional, Protocol


class StrEnum(str, Enum):
    pass


class FileEventType(StrEnum):
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"


@dataclass(frozen=True)
class FileEvent:
    path: str
    event: FileEventType
    source: str
    size: int | None = None
    mtime: float | None = None
    etag: str | None = None


@dataclass(frozen=True)
class StatInfo:
    size: int
    mtime: float


class EventSource(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    def subscribe(self, handler: "FileEventHandler") -> None: ...


class FileEventHandler(Protocol):
    async def __call__(self, event: FileEvent) -> None: ...


class Job(Protocol):
    def update_status(self, status) -> None: ...
    def execute(self) -> str: ...


class JobStore(Protocol):
    def create_job(self, path: str, dispatcher) -> Job: ...
    def get_job(self, job_id: str, dispatcher) -> Job: ...
    def update_status(self, job_id: str, status) -> None: ...
    def get_job_by_path(self, path: str) -> Optional["JobRow"]: ...
    def get_jobs_by_path(self, path: str) -> List["JobRow"]: ...


class JobRow(NamedTuple):
    job_id: str
    file_path: str
    message_id: str
    status: str
    queued_at: int
    started_at: int
    finished_at: int


class JobDispatcher(Protocol):
    def enqueue(self, job) -> str: ...


class OcrExecutor(Protocol):
    def run(self, job, input_path: str, output_path: str) -> None: ...


class WorkDirManager(Protocol):
    def stage_input(self, job) -> tuple[str, str, str]: ...
    def restore_output(self, job, work_output: str, output_path: str) -> None: ...
    def cleanup(self, job) -> None: ...


class StorageAdapter(Protocol):
    # Must return file-like BinaryIO objects.
    def open(self, path: str, mode: str) -> BinaryIO: ...
    def exists(self, path: str) -> bool: ...
    def stat(self, path: str) -> StatInfo: ...
    def rename(self, src: str, dst: str) -> None: ...
