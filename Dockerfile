# Production Dockerfile for Legal Tender
# Code is baked into image, optimized for deployment

ARG GO_IMAGE=golang:1.26.5-bookworm@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd

FROM --platform=linux/amd64 ${GO_IMAGE} AS go-builder

WORKDIR /src
COPY go.mod go.sum ./
COPY scripts/go-build.env scripts/build-go.sh ./scripts/
COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN bash scripts/build-go.sh dependencies && mkdir /out && bash scripts/build-go.sh build /out/legal-tender

FROM --platform=linux/amd64 python:3.11-slim-bookworm

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    postgresql-client-15 \
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
COPY orchestration/ ./orchestration/
COPY contracts/ ./contracts/
COPY --from=go-builder /out/legal-tender /usr/local/bin/legal-tender

# Create non-root user for security
RUN useradd -m -u 1000 dagster && \
    chown -R dagster:dagster /app && \
    mkdir -p /app/compute_logs /app/storage /app/data && \
    chown -R dagster:dagster /app/compute_logs /app/storage /app/data

USER dagster

# Environment variables
ENV PYTHONPATH=/app \
    DAGSTER_HOME=/app \
    LEGAL_TENDER_BINARY=/usr/local/bin/legal-tender

# Default command (overridden in docker-compose)
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]
