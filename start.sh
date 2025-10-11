#!/bin/bash

# Legal Tender - Docker Compose Startup Script
# Usage:
#   ./start.sh           - Start in production mode
#   ./start.sh -dev      - Start in development mode (hot-reload)
#   ./start.sh -v        - Start in production mode (wipe volumes)
#   ./start.sh -dev -v   - Start in dev mode (wipe volumes)

# Parse arguments
DEV_MODE=false
WIPE_VOLUMES=false

for arg in "$@"; do
  case $arg in
    -d|--dev)
      DEV_MODE=true
      shift
      ;;
    -v|--volumes)
      WIPE_VOLUMES=true
      shift
      ;;
    *)
      ;;
  esac
done

# Shutdown existing containers
if [[ "$WIPE_VOLUMES" == true ]]; then
  echo "🗑️  Stopping containers and wiping volumes..."
  docker compose down -v
else
  echo "🛑 Stopping containers..."
  docker compose down
fi

# Start containers based on mode
if [[ "$DEV_MODE" == true ]]; then
  echo "🚀 Starting in DEVELOPMENT mode (hot-reload enabled)..."
  docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build -d
else
  echo "🚀 Starting in PRODUCTION mode..."
  docker compose up --build -d
fi

echo ""
echo "✅ Services started!"
echo "   Dagster UI: http://localhost:3000"
echo "   Mongo Express: http://localhost:8081"
if [[ "$DEV_MODE" == true ]]; then
  echo "   Mode: DEVELOPMENT (code changes auto-reload)"
  echo "   MongoDB: localhost:27017 (exposed)"
  echo "   PostgreSQL: localhost:5432 (exposed)"
else
  echo "   Mode: PRODUCTION (rebuild required for changes)"
fi