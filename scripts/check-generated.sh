#!/usr/bin/env bash
# Fails when internal/protocol is out of date with protocol/openapi.yaml.
set -euo pipefail
cd "$(dirname "$0")/.."
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
cp internal/protocol/*.gen.go "$tmp/"
go generate ./internal/protocol
for f in internal/protocol/*.gen.go; do
  if ! cmp -s "$f" "$tmp/$(basename "$f")"; then
    cp "$tmp/$(basename "$f")" "$f"
    echo "stale: $f (run: go generate ./internal/protocol)" >&2
    exit 1
  fi
done
echo "connector protocol (Go): generated files are up to date"
