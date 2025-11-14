# Multi-stage Dockerfile for Legal Tender

# Base stage with common dependencies
FROM python:3.11-slim AS base

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    postgresql-client \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install MongoDB Database Tools from official MongoDB releases
RUN wget -qO- https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian12-x86_64-100.10.0.tgz | tar xz -C /tmp \
    && mv /tmp/mongodb-database-tools-*/bin/* /usr/local/bin/ \
    && rm -rf /tmp/mongodb-database-tools-*

# Copy requirements and install Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Set PYTHONPATH so src modules can be imported
ENV PYTHONPATH=/app
ENV DAGSTER_HOME=/app

# Production stage
FROM base AS production

# Copy Dagster configuration files
COPY workspace.yaml ./
COPY dagster.yaml ./

# Copy source code
COPY src/ ./src/

# Create non-root user for security
RUN useradd -m -u 1000 dagster && \
    chown -R dagster:dagster /app && \
    mkdir -p /app/compute_logs /app/storage && \
    chown -R dagster:dagster /app/compute_logs /app/storage

USER dagster

# Default command (overridden in docker-compose.yml)
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]

# Development stage (for local development with hot reloading)
FROM base AS development

# Copy Dagster configuration files
COPY workspace.yaml ./
COPY dagster.yaml ./

# In development, source code is mounted via volume
# This allows for hot reloading without rebuilding the image

# Default command
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]
