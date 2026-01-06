# Production Dockerfile for Legal Tender
# Code is baked into image, optimized for deployment

FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy Dagster configuration files
COPY workspace.yaml .
COPY dagster.yaml .

# Copy source code into image
COPY src/ ./src/

# Create non-root user for security
RUN useradd -m -u 1000 dagster && \
    chown -R dagster:dagster /app && \
    mkdir -p /app/compute_logs /app/storage /app/data && \
    chown -R dagster:dagster /app/compute_logs /app/storage /app/data

USER dagster

# Environment variables
ENV PYTHONPATH=/app \
    DAGSTER_HOME=/app

# Default command (overridden in docker-compose)
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]
