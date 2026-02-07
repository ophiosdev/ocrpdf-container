#!/usr/bin/env sh
set -eu

# Entrypoint for the OCRMYPDF supervisor container.
# Responsibilities:
# - Ensure persistent `QUEUE_DB_PATH` directory exists and is writable.
# - Ensure `WORK_DIR` exists and has available space.
# - Verify Tesseract language data for `OCR_LANGS` (best-effort).
# - Launch the supervisor `app/main.py`.

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  cat <<'EOF'
OCRMYPDF supervisor container

Configuration environment variables:
  SMB_UNC_PATH              UNC path to watch (e.g. \\server\share). Required.
  SMB_USERNAME              SMB account username.
  SMB_PASSWORD              SMB account password.
  WATCH_TREE                true/false to watch directories recursively (default: false).
  SMB_WAIT_FOR_HANDLERS     true/false to await SMB handlers per event (default: true).
  SMB_ENFORCE_ENCRYPTION    true/false to require SMB encryption (default: false).
  QUEUE_DB_PATH             Path to SQLite queue DB (default: /data/ocr_queue.sqlite).
  WORK_DIR                  Local staging directory for OCR (default: /data/work).
  OUTPUT_SUFFIX             Suffix appended to output PDFs (default: _ocr.pdf).
  STABILITY_SECONDS         Seconds between file stat checks (default: 5).
  WORK_DIR_MAX_AGE_SECONDS  Remove work dirs older than this (default: 86400).
  MAX_WORKERS               Number of worker processes (default: 2).
  OCR_JOBS                  OCRmyPDF internal parallelism per job (default: 2).
  OCR_LANGS                 Tesseract languages (default: deu+eng).
  OCR_OUTPUT_TYPE           Output format (default: pdfa).
  OCR_ROTATE_PAGES          true/false to enable rotation (default: true).
  OCR_DESKEW                true/false to enable deskew (default: true).
  OCR_MODE                  OCR processing mode: default|skip|redo|force (default: skip).
  OCR_VERBOSITY             OCRmyPDF logging: quiet|default|debug|debug_all (default: default).
  OCR_LOG_ROOT              true/false to enable root logger output (default: false).
  OCR_KEEP_TEMP             true/false to retain OCRmyPDF temp files (default: false).
  RETRY_MAX                 Max retries for failed jobs (default: 4).
  RETRY_BACKOFF_BASE        Retry backoff base (default: 2).
  OCR_JOB_TIMEOUT           Max seconds per OCR job (default: 3600).
  MOVE_FAILED_TO_DIR        true/false to move failed inputs to failed/ (default: false).
  WORK_DIR_MIN_SPACE_BYTES  Minimum free bytes required in WORK_DIR (default: 104857600).

Use docker run -e VAR=value to override defaults.
EOF
  exit 0
fi

echo "Starting supervisor (app/main.py)"
exec python3 app/main.py
