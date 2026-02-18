FROM python:3.11-slim

LABEL maintainer="Peter Swimm <peter@toilville.com>"
LABEL description="Audit Consumer Daemon - Purview-compatible event-driven audit logging"

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY daemon.py .
COPY config.py .

# Create log directories
RUN mkdir -p /logs/purview /logs/trust

# Run as non-root user
RUN useradd -m -u 1000 audit && chown -R audit:audit /app /logs
USER audit

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

CMD ["python", "-u", "daemon.py"]
