#!/usr/bin/env bash
# Runs every test. The PostgreSQL tests need a server: set RATEGATE_TEST_DB to its URL (any
# database; each test creates and drops its own), or leave it unset to use the throwaway
# local cluster (scripts/pg_local.sh), started here if needed.
#
# Usage: scripts/test.sh [go test flags]      e.g. scripts/test.sh -race -count=1
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/env.sh"
cd "$ROOT"

if [ -z "${RATEGATE_TEST_DB:-}" ]; then
  bash scripts/pg_local.sh start
  export RATEGATE_TEST_DB="$RATEGATE_LOCAL_DB"
fi
unformatted="$(gofmt -l .)"
if [ -n "$unformatted" ]; then
  echo "not gofmt-formatted:" >&2
  echo "$unformatted" >&2
  exit 1
fi
go vet ./...
go test "$@" ./...
