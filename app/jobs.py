import dataclasses
import ntpath
import os
import sqlite3
import time
import uuid
from typing import List

from interfaces import Job, JobDispatcher, JobRow, JobStore, StrEnum


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    FINISHED = "finished"
    ERROR = "error"


class JobExistsError(Exception): ...


@dataclasses.dataclass(frozen=True)
class DramatiqJob:
    job_id: str
    file_path: str
    message_id: str
    store: JobStore
    dispatcher: JobDispatcher

    def to_message(self, actor):
        message = actor.message(self.job_id, self.file_path)
        return message.copy(message_id=self.message_id)

    def update_status(self, status: JobStatus) -> None:
        self.store.update_status(self.job_id, status)

    def execute(self) -> str:
        return self.dispatcher.enqueue(self)


class SqliteJobStore(JobStore):
    def __init__(self, db_path: str, auto_init: bool = True):
        self.db_path = db_path
        if auto_init:
            self.init_schema()

    def init_schema(self) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            _ = conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ocr_jobs (
                    job_id TEXT PRIMARY KEY,
                    file_path TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    queued_at INTEGER,
                    started_at INTEGER,
                    finished_at INTEGER
                )
                """
            )
            _ = conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ocr_jobs_status ON ocr_jobs(status)"
            )
            _ = conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ocr_jobs_message_id ON ocr_jobs(message_id)"
            )
            _ = conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ocr_jobs_path ON ocr_jobs(file_path)"
            )
            _ = conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_ocr_jobs_active_path
                ON ocr_jobs(file_path)
                WHERE status IN ('queued', 'running')
                """
            )

    def create_job(self, path: str, dispatcher: JobDispatcher) -> Job:
        job_id = str(uuid.uuid4())
        message_id = str(uuid.uuid4())
        now = int(time.time())
        with sqlite3.connect(self.db_path) as conn:
            try:
                _ = conn.execute(
                    """
                    INSERT INTO ocr_jobs
                    (job_id, file_path, message_id, status, queued_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (job_id, path, message_id, JobStatus.QUEUED.value, now),
                )
            except sqlite3.IntegrityError as ie:
                raise JobExistsError(path) from ie
        return DramatiqJob(
            job_id=job_id,
            file_path=path,
            message_id=message_id,
            store=self,
            dispatcher=dispatcher,
        )

    def create_job_from_event(
        self,
        server: str,
        share: str,
        watch_path: str,
        filename: str,
        dispatcher: JobDispatcher,
    ) -> Job:
        full_path = self.canonicalize_path(server, share, watch_path, filename)
        return self.create_job(full_path, dispatcher)

    def get_job(self, job_id: str, dispatcher: JobDispatcher) -> Job:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                "SELECT job_id, file_path, message_id FROM ocr_jobs WHERE job_id = ?",
                (job_id,),
            )
            row = cur.fetchone()
            if not row:
                raise KeyError(job_id)
            return DramatiqJob(
                job_id=row[0],
                file_path=row[1],
                message_id=row[2],
                store=self,
                dispatcher=dispatcher,
            )

    def update_status(self, job_id: str, status: JobStatus) -> None:
        now = int(time.time())
        column = (
            "finished_at"
            if status in (JobStatus.FINISHED, JobStatus.ERROR)
            else "started_at"
        )
        with sqlite3.connect(self.db_path) as conn:
            _ = conn.execute(
                f"UPDATE ocr_jobs SET status = ?, {column} = ? WHERE job_id = ?",
                (status.value, now, job_id),
            )

    def get_job_by_path(self, path: str) -> JobRow | None:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                "SELECT job_id, file_path, message_id, status, queued_at, started_at, finished_at"
                " FROM ocr_jobs WHERE file_path = ? ORDER BY queued_at DESC LIMIT 1",
                (path,),
            )
            row = cur.fetchone()
            return JobRow(*row) if row else None

    def get_jobs_by_path(self, path: str) -> List[JobRow]:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                "SELECT job_id, file_path, message_id, status, queued_at, started_at, finished_at"
                " FROM ocr_jobs WHERE file_path = ? ORDER BY queued_at DESC",
                (path,),
            )
            return [JobRow(*row) for row in cur.fetchall()]

    def canonicalize_path(
        self,
        server: str,
        share: str,
        watch_path: str,
        filename: str,
    ) -> str:
        parts = [server, share] + ([watch_path] if watch_path else []) + [filename]
        unc = "\\" + "\\".join(p for p in parts if p)
        return ntpath.normpath(unc)
