#!/usr/bin/env bash
# DealFlow SQL: one-command reproduce.
# Starts PostgreSQL in Docker, applies the schema, loads the CSVs,
# and runs all 10 analysis queries.
#
# Safe to run as many times as you like: the schema drops and recreates its
# tables, and the load truncates before inserting, so every run ends in the
# same state.
#
# Usage:
#   ./run.sh           start + schema + load + all 10 queries
#   ./run.sh results   the same, then regenerate results.html from the real output
#   ./run.sh psql      open an interactive psql shell on the loaded database
#   ./run.sh stop      stop the container (data is kept)
#   ./run.sh clean     remove the container and its data volume
set -euo pipefail

cd "$(dirname "$0")"

CONTAINER=dealflow-db
IMAGE=postgres:16
PORT=5433
CMD="${1:-run}"

case "$CMD" in
  run|results|psql|stop|clean|help|-h|--help) ;;
  *) echo "Unknown command: $CMD"; sed -n '10,15p' "$0"; exit 2 ;;
esac

if [[ "$CMD" == "help" || "$CMD" == "-h" || "$CMD" == "--help" ]]; then
  sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

# Docker has to be running before anything else is worth trying.
if ! docker info >/dev/null 2>&1; then
  echo "Docker is not running."
  echo "Open Docker Desktop, wait for the whale icon to stop animating, then run ./run.sh again."
  exit 1
fi

if [[ "$CMD" == "stop" ]]; then
  docker stop "$CONTAINER" >/dev/null 2>&1 || true
  echo "Stopped. Start it again with: ./run.sh"
  exit 0
fi

if [[ "$CMD" == "clean" ]]; then
  # -v also drops the anonymous data volume, so nothing is left dangling.
  docker rm -f -v "$CONTAINER" >/dev/null 2>&1 || true
  echo "Removed container $CONTAINER and its data volume."
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
# shellcheck disable=SC1091
source .env
set +a

PSQL=(docker exec -i "$CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1)

if [[ -z "$(docker ps -q -f name="^${CONTAINER}$")" ]]; then
  if [[ -n "$(docker ps -aq -f name="^${CONTAINER}$")" ]]; then
    docker start "$CONTAINER" >/dev/null
    echo "Started existing container $CONTAINER."
  else
    echo "Creating container $CONTAINER on $IMAGE (first run pulls the image)..."
    docker run -d --name "$CONTAINER" \
      -e POSTGRES_USER="$POSTGRES_USER" \
      -e POSTGRES_DB="$POSTGRES_DB" \
      -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
      -p "$PORT":5432 \
      -v "$PWD/data":/data:ro \
      "$IMAGE" >/dev/null
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
if ! docker exec "$CONTAINER" pg_isready -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"; then
  echo "PostgreSQL did not come up within 60 seconds. Logs:"
  docker logs --tail 30 "$CONTAINER"
  exit 1
fi

# An existing container keeps the password it was created with. If .env was
# regenerated since then, every psql call would fail on authentication with a
# confusing error, so catch it here and say what to do.
if ! "${PSQL[@]}" -c 'SELECT 1' >/dev/null 2>&1; then
  echo "Could not authenticate against the existing $CONTAINER container."
  echo "The password in .env does not match the one the container was created with."
  echo "Fix it with:  ./run.sh clean && ./run.sh"
  exit 1
fi

if [[ "$CMD" == "psql" ]]; then
  exec docker exec -it "$CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"
fi

echo "Applying schema..."
"${PSQL[@]}" < schema.sql

echo "Loading CSVs..."
"${PSQL[@]}" < load.sql

echo "Running queries..."
"${PSQL[@]}" -e < queries.sql

if [[ "$CMD" == "results" ]]; then
  echo
  echo "Building results.html..."
  python3 build_results.py
fi

echo
echo "Done. DB is on localhost:$PORT (user $POSTGRES_USER, db $POSTGRES_DB)."
echo "Open a SQL shell with: ./run.sh psql"
echo "Stop with: ./run.sh stop"
