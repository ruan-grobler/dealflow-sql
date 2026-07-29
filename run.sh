#!/usr/bin/env bash
# DealFlow SQL: one-command reproduce.
# Starts PostgreSQL in Docker, applies the schema, loads the CSVs,
# and runs all 10 analysis queries.
#
# Usage:
#   ./run.sh          start + schema + load + queries
#   ./run.sh stop     stop the container (data is kept)
#   ./run.sh clean    remove the container entirely
set -euo pipefail

cd "$(dirname "$0")"

CONTAINER=dealflow-db
IMAGE=postgres:16
PORT=5433

if [[ "${1:-}" == "stop" ]]; then
  docker stop "$CONTAINER"
  echo "Stopped. Restart with: docker start $CONTAINER"
  exit 0
fi

if [[ "${1:-}" == "clean" ]]; then
  docker rm -f "$CONTAINER" 2>/dev/null || true
  echo "Removed container $CONTAINER."
  exit 0
fi

# Password lives only in .env (gitignored). Generate one on first run.
if [[ ! -f .env ]]; then
  echo "POSTGRES_USER=dealflow" > .env
  echo "POSTGRES_DB=dealflow" >> .env
  echo "POSTGRES_PASSWORD=$(openssl rand -hex 16)" >> .env
  echo "Created .env with a generated password."
fi
set -a
source .env
set +a

PSQL=(docker exec -i "$CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1)

if [[ -z "$(docker ps -q -f name="^${CONTAINER}$")" ]]; then
  if [[ -n "$(docker ps -aq -f name="^${CONTAINER}$")" ]]; then
    docker start "$CONTAINER"
  else
    docker run -d --name "$CONTAINER" \
      -e POSTGRES_USER="$POSTGRES_USER" \
      -e POSTGRES_DB="$POSTGRES_DB" \
      -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
      -p "$PORT":5432 \
      -v "$PWD/data":/data:ro \
      "$IMAGE"
  fi
fi

# Readiness must be checked over TCP: on first boot the postgres entrypoint
# runs a temporary init server on the unix socket only, and that one answers
# pg_isready before the real server is up.
echo "Waiting for PostgreSQL..."
for _ in $(seq 1 60); do
  if docker exec "$CONTAINER" pg_isready -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker exec "$CONTAINER" pg_isready -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"

echo "Applying schema..."
"${PSQL[@]}" < schema.sql

echo "Loading CSVs..."
"${PSQL[@]}" < load.sql

echo "Running queries..."
"${PSQL[@]}" -e < queries.sql

echo
echo "Done. DB is on localhost:$PORT (user $POSTGRES_USER, db $POSTGRES_DB)."
echo "Stop with: ./run.sh stop"
