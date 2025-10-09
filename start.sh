#!/bin/bash
if [[ "$1" == "-v" ]]; then
  docker compose down -v
else
  docker compose down
fi

docker compose up --build -d