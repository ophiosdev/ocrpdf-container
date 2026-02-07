import dramatiq

from interfaces import JobDispatcher


class DramatiqDispatcher(JobDispatcher):
    def __init__(self, actor):
        self.actor = actor

    def enqueue(self, job) -> str:
        message = job.to_message(self.actor)
        broker = getattr(self.actor, "broker", None) or dramatiq.get_broker()
        m = broker.enqueue(message)
        return m.message_id


def register_tasks(broker, retry_max: int):
    dramatiq.set_broker(broker)

    @dramatiq.actor(max_retries=retry_max, queue_name="default")
    def ocr_pdf_job(job_id: str, file_path: str) -> None:
        # This runs in the worker process only.
        from worker import process_job  # noqa: PLC0415

        process_job(job_id, file_path)

    return ocr_pdf_job
