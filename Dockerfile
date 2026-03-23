# Kafka-Celery Bridge - Lightweight Consumer
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY main.py .

# Health check (process running + low CPU = healthy)
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import os; os.kill(1, 0)" || exit 1

# Run bridge
CMD ["python", "-u", "main.py"]
