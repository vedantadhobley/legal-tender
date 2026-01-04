# Production Dockerfile for Legal Tender
# Code is baked into image, optimized for deployment

FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    postgresql-client \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install MongoDB Database Tools for dump/restore (from mongo-dumps branch)
RUN wget -qO- https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian12-x86_64-100.10.0.tgz | tar xz -C /tmp \
    && mv /tmp/mongodb-database-tools-*/bin/* /usr/local/bin/ \
    && rm -rf /tmp/mongodb-database-tools-*

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
    mkdir -p /app/compute_logs /app/storage /app/data /app/data/mongo && \
    chown -R dagster:dagster /app/compute_logs /app/storage /app/data

USER dagster

# Environment variables
ENV PYTHONPATH=/app \
    DAGSTER_HOME=/app

# Default command (overridden in docker-compose)
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]
