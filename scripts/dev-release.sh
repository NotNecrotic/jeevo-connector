#!/usr/bin/env bash
# Usage: scripts/dev-release.sh [RELEASES_DIR]  (dev build for a CONNECTOR_DEV_MODE server, throwaway key kept in RELEASES_DIR)
set -euo pipefail

ROOT="${1:-${CONNECTOR_RELEASES_DIR:-}}"
[[ -n "$ROOT" ]] || { echo "usage: dev-release.sh RELEASES_DIR (the server's CONNECTOR_RELEASES_DIR)" >&2; exit 1; }
HERE="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$ROOT"
ROOT="$(cd "$ROOT" && pwd)"
KEY="$ROOT/.dev-signing-key"

cd "$HERE"
if [[ ! -s "$KEY" ]]; then
  echo "==> creating a dev signing key in $ROOT"
  out="$(go run ./cmd/jeevo-release keygen)"
  (umask 077 && awk '/^private:/{print $2}' <<<"$out" >"$KEY")
  awk '/^public:/{print $2}' <<<"$out" >"$KEY.pub"
fi

VERSION="0.0.0-dev.$(date -u +%Y%m%d%H%M%S)"
JEEVO_RELEASE_KEY="$(cat "$KEY")" JEEVO_RELEASE_SKIP_TESTS=1 ./scripts/release.sh "$VERSION" stable "$ROOT"

# Keep the three newest dev builds.
find "$ROOT" -maxdepth 1 -type d -name '0.0.0-dev.*' -printf '%f\n' | sort -r | tail -n +4 \
  | while read -r old; do rm -rf "${ROOT:?}/$old"; done

# The installer the server hands out (what the release sync does for GitHub releases).
install -m 644 "$ROOT/$VERSION/install.sh" "$ROOT/install.sh"

cat <<MSG

Dev release $VERSION is published in $ROOT.
The server picks it up within a minute; it must run with CONNECTOR_DEV_MODE=true
and without CONNECTOR_RELEASE_PUBLIC_KEYS (or with this key in it:
$(cat "$KEY.pub")).
MSG
