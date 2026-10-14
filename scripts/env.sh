# Settings shared by the scripts. Sourced (not run) after the caller sets ROOT.

EXE=""
case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*)
    EXE=".exe"
    # Go from MSYS2 (pacman -S mingw-w64-x86_64-go) needs its GOROOT spelled out.
    if [ -d /c/msys64/mingw64/bin ]; then
      export PATH="/c/msys64/mingw64/bin:$PATH"
      [ -d /c/msys64/mingw64/lib/go ] && export GOROOT="${GOROOT:-C:/msys64/mingw64/lib/go}"
    fi
    ;;
esac

# Build and test output: inside the repo normally, but outside it when the repo is in a
# OneDrive folder (OneDrive locks and uploads synced build folders).
RATEGATE_OUT_DEFAULT="$ROOT/.out"
case "$ROOT" in
  */OneDrive*) RATEGATE_OUT_DEFAULT="$(cygpath -u "$LOCALAPPDATA")/rategate" ;;
esac
RATEGATE_OUT="${RATEGATE_OUT:-$RATEGATE_OUT_DEFAULT}"
mkdir -p "$RATEGATE_OUT"

# The throwaway PostgreSQL cluster for local tests (scripts/pg_local.sh): its own port and
# data folder, user "rategate", no password, local connections only.
RATEGATE_PG_PORT="${RATEGATE_PG_PORT:-54332}"
RATEGATE_PG_DATA="$RATEGATE_OUT/pgdata"
RATEGATE_LOCAL_DB="postgres://rategate@localhost:$RATEGATE_PG_PORT/postgres?sslmode=disable"
