#!/usr/bin/env bash
# Usage: JEEVO_RELEASE_KEY=<base64 seed> scripts/release.sh VERSION [stable|beta] [releases-dir]
set -euo pipefail

VERSION="${1:?usage: release.sh VERSION [CHANNEL] [RELEASES_DIR]}"
CHANNEL="${2:-stable}"
OUT_ROOT="${3:-${CONNECTOR_RELEASES_DIR:-}}"
[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.-]+)?$ ]] || { echo "version must be semver, e.g. 1.2.0" >&2; exit 1; }
[[ "$CHANNEL" =~ ^(stable|beta)$ ]] || { echo "channel must be stable or beta" >&2; exit 1; }
[[ -n "$OUT_ROOT" ]] || { echo "set the releases dir (3rd argument or CONNECTOR_RELEASES_DIR)" >&2; exit 1; }
[[ -n "${JEEVO_RELEASE_KEY:-}" ]] || { echo "set JEEVO_RELEASE_KEY (base64 seed from jeevo-release keygen)" >&2; exit 1; }

HERE="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$OUT_ROOT/$VERSION"
[[ -e "$OUT" ]] && { echo "$OUT already exists: releases are immutable, bump the version" >&2; exit 1; }
STAGE="$(mktemp -d)"
DONE=0
mkdir -p "$OUT"
# Set only now, so a failed run removes its half-written version folder but never an existing release.
trap 'rm -rf "$STAGE"; [[ $DONE == 1 ]] || rm -rf "$OUT"' EXIT

cd "$HERE"
echo "==> checking the protocol is regenerated"
./scripts/check-generated.sh
if [[ "${JEEVO_RELEASE_SKIP_TESTS:-}" == 1 ]]; then
  echo "==> tests skipped (JEEVO_RELEASE_SKIP_TESTS=1)"
else
  echo "==> tests"
  go test ./...
  if command -v python3 >/dev/null && python3 -c 'import pytest, onnxruntime, PIL' 2>/dev/null; then
    python3 -m pytest -q ai/tests
  else
    echo "  (skipping AI worker tests: python3 with requirements-dev.txt not found)"
  fi
fi

COMMIT="$(git -C "$HERE" rev-parse --short HEAD 2>/dev/null || echo unknown)"
for ARCH in amd64 arm64; do
  echo "==> building linux/$ARCH"
  D="$STAGE/$ARCH"
  mkdir -p "$D"
  CGO_ENABLED=0 GOOS=linux GOARCH=$ARCH go build -trimpath \
    -ldflags "-s -w -X github.com/NotNecrotic/jeevo-connector/internal/version.Version=$VERSION -X github.com/NotNecrotic/jeevo-connector/internal/version.Commit=$COMMIT" \
    -o "$D/jeevo-connector" ./cmd/jeevo-connector
  echo "$VERSION" >"$D/VERSION"
  cp -r ai "$D/ai"
  rm -rf "$D/ai/tests" "$D/ai/requirements-dev.txt"
  find "$D/ai" -name '__pycache__' -prune -exec rm -rf {} +
  tar -C "$D" --owner=0 --group=0 --numeric-owner -czf "$OUT/jeevo-connector-$VERSION-linux-$ARCH.tar.gz" .
done

# The installer ships with the release; its hash is signed in the manifest.
install -m 644 scripts/install.sh "$OUT/install.sh"

echo "==> manifest and signature"
go run ./cmd/jeevo-release manifest --version "$VERSION" --channel "$CHANNEL" --dir "$OUT"
go run ./cmd/jeevo-release sign --manifest "$OUT/manifest.json"

mkdir -p "$OUT_ROOT/channels"
printf '{"version":"%s"}\n' "$VERSION" >"$OUT_ROOT/channels/$CHANNEL.json.tmp"
mv "$OUT_ROOT/channels/$CHANNEL.json.tmp" "$OUT_ROOT/channels/$CHANNEL.json"
DONE=1
echo "==> published $VERSION on $CHANNEL ($OUT)"
