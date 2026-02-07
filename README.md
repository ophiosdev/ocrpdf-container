# 📄✨ OCRPDF SMB Pipeline

Turn noisy scans into searchable PDFs the moment they hit your SMB share. This service watches a network folder, queues new PDFs, runs OCR in isolated worker processes, and writes `_ocr.pdf` outputs right beside the originals. 🚀

## 🧭 What this system does

- **Watches SMB** for new PDFs and reacts fast without blocking file events.
- **Queues work durably** with a SQLite-backed broker (Dramatiq + `dramatiq-sqlite`).
- **Processes in parallel** using worker processes with controlled OCR concurrency.
- **Stages files locally** for OCR stability, then restores results back to SMB.
- **Dedupes jobs** so the same file won’t be OCR’d twice at the same time.

## 🧩 How it flows

1) **Monitor** detects a new `*.pdf` on the SMB share.  
2) **Job store** creates a deduped job record in SQLite.  
3) **Queue** dispatches the job to a worker process.  
4) **Worker** copies the PDF to `WORK_DIR`, runs OCRmyPDF, and writes the result back.  
5) **Status** is tracked in SQLite for visibility and retries.

## 🌟 Highlights

- **Non-blocking SMB event handling** with async dispatch.
- **Process isolation** for OCRmyPDF (safe parallelism).
- **Stability check** before OCR (waits for file size/mtime to settle).
- **Retries with backoff** for transient failures.
- **Config validation** at startup (fail fast on bad settings).
- **Health endpoint** on `http://127.0.0.1:8000/health` returning JSON: `{workers_alive, queue_depth, last_error}`.
  - Binds to `127.0.0.1:8000` by default; update the bind address in `app/main.py` for external access.

## 🔁 Reliability and retries

Jobs are persisted to a SQLite-backed queue, so work survives restarts and crashes. If a worker or the host goes down mid-OCR, the queued job remains in `ocr_queue.sqlite`, and the supervisor will resume unfinished work on the next start. Retries use exponential backoff (see `RETRY_MAX` and `RETRY_BACKOFF_BASE`) to handle transient SMB or OCR failures without losing files.

## ⚙️ Configuration parameters

All configuration is via environment variables.

| Variable                   | Default                  | Description                                                      |
| -------------------------- | ------------------------ | ---------------------------------------------------------------- |
| `SMB_UNC_PATH`             | `""`                     | UNC path to the SMB share, e.g. `\\server\share` **(required)**. |
| `SMB_USERNAME`             | unset                    | SMB username.                                                    |
| `SMB_PASSWORD`             | unset                    | SMB password.                                                    |
| `WATCH_TREE`               | `false`                  | Watch subdirectories recursively when `true`.                    |
| `SMB_WAIT_FOR_HANDLERS`    | `true`                   | Wait for handlers to finish before acknowledging SMB events.     |
| `SMB_ENFORCE_ENCRYPTION`   | `false`                  | Require SMB encryption when `true`.                              |
| `QUEUE_DB_PATH`            | `/data/ocr_queue.sqlite` | SQLite DB path for the job queue and history.                    |
| `WORK_DIR`                 | `/data/work`             | Local staging directory for OCR processing.                      |
| `WORK_DIR_MAX_AGE_SECONDS` | `86400`                  | Cleanup threshold for stale work dirs (seconds).                 |
| `OUTPUT_SUFFIX`            | `_ocr.pdf`               | Output file suffix written next to original.                     |
| `STABILITY_SECONDS`        | `5`                      | File-stability wait before OCR (size/mtime check).               |
| `MAX_WORKERS`              | `2`                      | Number of worker processes.                                      |
| `OCR_JOBS`                 | `2`                      | OCRmyPDF internal job count per file.                            |
| `OCR_LANGS`                | `deu+eng`                | Tesseract languages for OCR.                                     |
| `OCR_OUTPUT_TYPE`          | `pdf`                    | OCRmyPDF output type (`pdf`, `pdfa`, etc.).                      |
| `OCR_ROTATE_PAGES`         | `true`                   | Auto-rotate pages during OCR.                                    |
| `OCR_DESKEW`               | `true`                   | Deskew pages during OCR.                                         |
| `OCR_MODE`                 | `skip`                   | OCR mode: `default`, `skip`, `redo`, or `force`.                 |
| `OCR_VERBOSITY`            | `default`                | OCRmyPDF verbosity: `quiet`, `default`, `debug`, `debug_all`.    |
| `OCR_LOG_ROOT`             | `false`                  | Emit OCR logs at root logger when `true`.                        |
| `OCR_KEEP_TEMP`            | `false`                  | Keep OCRmyPDF temp files when `true`.                            |
| `OCR_JOB_TIMEOUT`          | `3600`                   | Hard timeout (seconds) per OCR job.                              |
| `MOVE_FAILED_TO_DIR`       | `false`                  | Move failed inputs into a sibling `failed/` dir.                 |
| `RETRY_MAX`                | `4`                      | Max retry attempts for OCR jobs.                                 |
| `RETRY_BACKOFF_BASE`       | `2`                      | Base for exponential backoff between retries.                    |
