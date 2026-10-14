#!/usr/bin/env bash
# A throwaway PostgreSQL cluster for local tests: port 54332, user "rategate", no password,
# local connections only, data outside the repo. It never touches any other PostgreSQL server.
# To remove it: stop it, then delete the data folder printed by "status".
#
# Usage: scripts/pg_local.sh start | stop | status
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/env.sh"

# The server programs: an installed PostgreSQL (Windows or Debian/Ubuntu layout), else PATH.
PG_BIN=""
for d in /c/Program\ Files/PostgreSQL/*/bin /usr/lib/postgresql/*/bin; do
  if [ -x "$d/initdb$EXE" ] && [ -x "$d/pg_ctl$EXE" ]; then PG_BIN="$d"; fi
done
if [ -z "$PG_BIN" ] && command -v initdb > /dev/null; then PG_BIN="$(dirname "$(command -v initdb)")"; fi
if [ -z "$PG_BIN" ]; then
  echo "PostgreSQL server programs (initdb, pg_ctl) not found" >&2
  exit 1
fi

pg_ctl() { "$PG_BIN/pg_ctl$EXE" -D "$RATEGATE_PG_DATA" "$@"; }

case "${1:-status}" in
  start)
    if [ ! -f "$RATEGATE_PG_DATA/PG_VERSION" ]; then
      mkdir -p "$RATEGATE_PG_DATA"
      "$PG_BIN/initdb$EXE" -D "$RATEGATE_PG_DATA" -U rategate -A trust -E UTF8 --no-locale \
        > "$RATEGATE_OUT/pg_initdb.log"
    fi
    if pg_ctl status > /dev/null 2>&1; then
      echo "PostgreSQL already running on port $RATEGATE_PG_PORT"
    else
      # The server gets no stdin/stdout/stderr of ours. Otherwise, on Windows it inherits the
      # caller's pipe and keeps it open, so e.g. `scripts/test.sh | tail` would never end.
      pg_ctl -w -l "$RATEGATE_OUT/pg_server.log" \
        -o "-p $RATEGATE_PG_PORT -c listen_addresses=localhost -c max_connections=100" start \
        < /dev/null > /dev/null 2>&1
      echo "PostgreSQL started on port $RATEGATE_PG_PORT"
    fi
    ;;
  stop)
    if pg_ctl status > /dev/null 2>&1; then
      pg_ctl -w -m fast stop > /dev/null
      echo "PostgreSQL stopped"
    else
      echo "PostgreSQL not running"
    fi
    ;;
  status)
    echo "data: $RATEGATE_PG_DATA"
    pg_ctl status || true
    ;;
  *)
    echo "usage: $0 start | stop | status" >&2
    exit 2
    ;;
esac
