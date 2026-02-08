ARG OCRMYPDF_IMAGE_VERSION=latest
FROM jbarlow83/ocrmypdf-alpine:${OCRMYPDF_IMAGE_VERSION}

# Default runtime configuration is defined in app/settings.py.

WORKDIR /app

# Copy application code into the image
COPY . /app

# Install Python dependencies into a virtual environment (PEP 668 compliance).
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
# hadolint ignore=DL3018
RUN apk add --no-cache py3-pip \
    && if [ ! -x "$VIRTUAL_ENV/bin/pip" ]; then \
        python3 -m venv --system-site-packages "$VIRTUAL_ENV"; \
    fi \
    && "$VIRTUAL_ENV/bin/pip" install --no-cache-dir -r requirements.txt

# Try to install language packs with the system package manager if they are
# not already present. This step is best-effort and wrapped so it won't
# break the build on unknown base images.
# hadolint ignore=DL3008,DL3018
RUN set -eux; \
    if command -v apt-get >/dev/null 2>&1; then \
        apt-get update && apt-get install -y --no-install-recommends \
            tesseract-ocr-eng tesseract-ocr-deu || true; \
        rm -rf /var/lib/apt/lists/*; \
    elif command -v apk >/dev/null 2>&1; then \
        apk add --no-cache tesseract-ocr curl || true; \
    fi

# Ensure tessdata for required languages exists (best-effort download if missing)
# hadolint ignore=DL4001
RUN set -eux; \
    TESSDATA_DIR=${TESSDATA_DIR:-/usr/share/tessdata}; \
    mkdir -p "$TESSDATA_DIR"; \
    for L in eng deu; do \
      if [ ! -f "$TESSDATA_DIR/${L}.traineddata" ]; then \
        echo "Downloading tessdata for $L"; \
        if command -v curl >/dev/null 2>&1; then \
          curl -fsSL -o "$TESSDATA_DIR/${L}.traineddata" "https://raw.githubusercontent.com/tesseract-ocr/tessdata/master/${L}.traineddata" || true; \
        elif command -v wget >/dev/null 2>&1; then \
          wget -q -O "$TESSDATA_DIR/${L}.traineddata" "https://raw.githubusercontent.com/tesseract-ocr/tessdata/master/${L}.traineddata" || true; \
        else \
          echo "curl/wget not available; cannot download tessdata for ${L}"; \
        fi; \
      fi; \
    done

# Create the persistent data mountpoint and working directory and make them
# writable. The deployment should mount a volume at /data to persist the
# queue DB and workdir between restarts.
VOLUME ["/data"]

# Copy entrypoint that verifies requirements and launches the supervisor
COPY start.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]

# By default the entrypoint expects either an UNC path as the first argument
# or the `SMB_UNC_PATH` environment variable to be set. The entrypoint will
# start the supervisor at `app/main.py` which manages monitor + worker child
# processes.
CMD ["--help"]
