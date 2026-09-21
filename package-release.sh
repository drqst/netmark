#!/bin/sh
# Build a self-contained Linux release directory for copying to another host.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
RELEASE_DIR=${RELEASE_DIR:-$ROOT/release}

cd "$ROOT"
cargo build --release

rm -rf "$RELEASE_DIR"
mkdir -p "$RELEASE_DIR"

cp target/release/netmark "$RELEASE_DIR/netmark"
for file in README.md Cargo.toml Cargo.lock build.rs netmarkctl init.sh check.sh stop.sh test.sh \
  netmark.config postgres.env.example; do
  cp "$file" "$RELEASE_DIR/$file"
done
for directory in src doc examples k8s profiles; do
  cp -R "$directory" "$RELEASE_DIR/$directory"
done
chmod +x "$RELEASE_DIR/netmark" "$RELEASE_DIR/netmarkctl" \
  "$RELEASE_DIR/init.sh" "$RELEASE_DIR/check.sh" "$RELEASE_DIR/stop.sh" \
  "$RELEASE_DIR/test.sh"

printf 'Release bundle created at %s\n' "$RELEASE_DIR"