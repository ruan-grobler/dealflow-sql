#!/usr/bin/env bash
# DealFlow SQL: build the whole warehouse from nothing, in one command.
#
#   ./run.sh              full rebuild: container, schema, 1.4M row feed, star, gates
#   ./run.sh --scale 0.05 the same pipeline on 5 percent of the volume (a smoke test)
#   ./run.sh analytics    re-run the 14 analysis queries against the built warehouse
#   ./run.sh quality      re-run the 51 data quality assertions
#   ./run.sh benchmark    re-run the performance case studies
#   ./run.sh results      rebuild results.html from live query output
#   ./run.sh psql         open a SQL shell on the warehouse
#   ./run.sh stop         stop the container, keep the data
#   ./run.sh clean        remove the container and its volume
#
# Every step is idempotent. Running the whole thing twice ends in the same
# state as running it once, which is the property that makes a failed load
# safe to retry rather than something to unpick by hand.
set -euo pipefail
cd "$(dirname "$0")"

CONTAINER=dealflow-db
IMAGE=postgres:16
PORT=5433
SCALE=1.0

CMD=run
while [[ $# -gt 0 ]]; do
  case "$1" in
    --scale) SCALE="$2"; shift 2 ;;
    run|analytics|quality|benchmark|results|psql|stop|clean) CMD="$1"; shift ;;
    help|-h|--help) sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown argument: $1"; echo "Try: ./run.sh --help"; exit 2 ;;
  esac
done

if ! docker info >/dev/null 2>&1; then
  echo "Docker is not running. Start Docker Desktop, wait for the whale to settle, then try again."
  exit 1
fi

if [[ "$CMD" == "stop" ]]; then
  docker stop "$CONTAINER" >/dev/null 2>&1 || true
  echo "Stopped. Start again with: ./run.sh"
  exit 0
fi

if [[ "$CMD" == "clean" ]]; then
  # -v takes the anonymous data volume with it, so nothing is left dangling.
  docker rm -f -v "$CONTAINER" >/dev/null 2>&1 || true
  echo "Removed container $CONTAINER and its data volume."
  exit 0
fi

# The password lives only in .env, which is gitignored. Generated on first run,
# never printed, never passed on a command line where ps(1) could read it.
if [[ ! -f .env ]]; then
  { echo "POSTGRES_USER=dealflow"
    echo "POSTGRES_DB=dealflow"
    echo "POSTGRES_PASSWORD=$(openssl rand -hex 16)"; } > .env
  echo "Created .env with a generated password."
fi
set -a
# shellcheck disable=SC1091
source .env
set +a

# pg_stat_statements has to be loaded at server start, so it is a container
# flag rather than something a session can turn on. It is what makes the
# workload ranking in PERFORMANCE.md a measurement instead of a guess.
if [[ -z "$(docker ps -q -f name="^${CONTAINER}$")" ]]; then
  if [[ -n "$(docker ps -aq -f name="^${CONTAINER}$")" ]]; then
    docker start "$CONTAINER" >/dev/null
  else
    echo "Creating container $CONTAINER on $IMAGE (the first run pulls the image)..."
    docker run -d --name "$CONTAINER" \
      -e POSTGRES_USER="$POSTGRES_USER" \
      -e POSTGRES_DB="$POSTGRES_DB" \
      -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
      -p "$PORT":5432 "$IMAGE" \
      -c shared_preload_libraries=pg_stat_statements >/dev/null
  fi
fi

# Readiness has to be checked over TCP. On first boot the postgres entrypoint
# runs a temporary init server on the unix socket only, and that one answers
# pg_isready before the real server is listening.
printf 'Waiting for PostgreSQL'
for _ in $(seq 1 90); do
  if docker exec "$CONTAINER" pg_isready -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
    break
  fi
  printf '.'; sleep 1
done
echo
if ! docker exec "$CONTAINER" pg_isready -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
  echo "PostgreSQL did not come up within 90 seconds. Container logs:"
  docker logs --tail 30 "$CONTAINER"
  exit 1
fi

PSQL=(docker exec -i -e PGPASSWORD="$POSTGRES_PASSWORD" "$CONTAINER"
      psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1)

# An existing container keeps the password it was created with. If .env was
# regenerated since, every call below would fail on authentication with a
# confusing error, so say what to do instead.
if ! "${PSQL[@]}" -q -c 'SELECT 1' >/dev/null 2>&1; then
  echo "Cannot authenticate against the existing $CONTAINER container."
  echo "The password in .env is not the one the container was created with."
  echo "Fix it with:  ./run.sh clean && ./run.sh"
  exit 1
fi

step() { printf '\n\033[1m==> %s\033[0m\n' "$1"; }

case "$CMD" in
  psql)      exec docker exec -it -e PGPASSWORD="$POSTGRES_PASSWORD" "$CONTAINER" \
                  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" ;;
  analytics) step "Analysis queries"; "${PSQL[@]}" -e -P pager=off < sql/05_analytics.sql; exit 0 ;;
  benchmark) step "Performance case studies"; python3 benchmark.py; exit 0 ;;
  results)   step "Rebuilding results.html"; python3 build_results.py; exit 0 ;;
  quality)
    step "Data quality assertions: WAREHOUSE battery (our correctness contract, must be clean)"
    python3 run_quality.py --battery warehouse
    step "Data quality assertions: STAGING battery (the supplier scorecard, findings expected)"
    # The staging battery MEASURES the source systems, so its findings are the
    # deliverable and not a build failure. Its exit code is deliberately
    # tolerated here; the warehouse battery above is the one that gates.
    python3 run_quality.py --battery staging || true
    exit 0 ;;
esac

START=$(date +%s)

step "1/8  Foundation, landing zone and typed staging (sql/01_staging.sql)"
"${PSQL[@]}" -q < sql/01_staging.sql

step "2/8  Generating and loading the synthetic feed at scale $SCALE (generate_data.py)"
mkdir -p out
python3 generate_data.py --schema raw --truncate --scale "$SCALE" \
        --ledger-out out/generator_ledger.json

step "3/8  Dimensional model: dimensions, partitioned fact, working tables (sql/02_warehouse.sql)"
"${PSQL[@]}" -q < sql/02_warehouse.sql

step "4/8  Load: raw to staging to intermediate, plus both Type 2 dimensions (sql/03_load.sql)"
"${PSQL[@]}" -q < sql/03_load.sql

step "5/8  Facts, materialized aggregate, quality gate, watermarks (sql/04_facts.sql)"
"${PSQL[@]}" -q < sql/04_facts.sql

step "6/8  Data quality assertion framework (sql/07_quality.sql)"
"${PSQL[@]}" -q < sql/07_quality.sql

step "7/8  Warehouse correctness contract: 30 assertions, all must pass"
python3 run_quality.py --battery warehouse

step "8/8  Analysis queries (sql/05_analytics.sql)"
"${PSQL[@]}" -e -P pager=off < sql/05_analytics.sql > out/analytics_output.txt
echo "  14 queries ran. Full output in out/analytics_output.txt"

ELAPSED=$(( $(date +%s) - START ))

printf '\n\033[1m==> Done in %dm %02ds.\033[0m\n' $((ELAPSED / 60)) $((ELAPSED % 60))
"${PSQL[@]}" -q -c "
SELECT 'mart.fct_deal_stage_event' AS table_name, count(*) AS rows FROM mart.fct_deal_stage_event
UNION ALL SELECT 'mart.fct_deal_pipeline', count(*) FROM mart.fct_deal_pipeline
UNION ALL SELECT 'mart.dim_broker (SCD2 versions)', count(*) FROM mart.dim_broker
UNION ALL SELECT 'mart.dim_property (SCD2 versions)', count(*) FROM mart.dim_property
UNION ALL SELECT 'mart.mv_broker_month', count(*) FROM mart.mv_broker_month
ORDER BY 2 DESC;"

cat <<EOF
Database is on localhost:$PORT (user $POSTGRES_USER, database $POSTGRES_DB).
  SQL shell            ./run.sh psql
  performance studies  ./run.sh benchmark
  supplier scorecard   ./run.sh quality
EOF
