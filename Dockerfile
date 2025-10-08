FROM prefecthq/prefect:3.4.22-python3.11

WORKDIR /app


# Install only what's needed for health checks
RUN apt-get update && apt-get install -y --no-install-recommends \
	curl \
	ca-certificates \
	&& rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt


# Copy prefect.yaml into the image
COPY prefect.yaml ./

COPY src/ ./src/

CMD ["python", "-m", "src"]
