#!/usr/bin/env bash
# BLite.Server.Benchmarks — build + run helper (Linux / macOS)
# Copyright (C) 2026 Luca Fabbri — AGPL-3.0
#
# Prerequisites:
#   - Docker (with compose plugin) running
#   - Both BLite/ and BLite.Server/ cloned into the same parent directory
#   - .NET 10 SDK installed
#
# Usage:
#   ./run-benchmarks.sh                         — run CrudBenchmarks (default)
#   ./run-benchmarks.sh --filter '*'            — run all benchmark classes
#   ./run-benchmarks.sh --skip-build            — reuse cached Docker image
#   ./run-benchmarks.sh --skip-docker           — skip Docker; use already-running services
#
# Extra args beyond the flags are forwarded to BenchmarkDotNet.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
PARENT_DIR="$(cd "$REPO_DIR/.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.benchmark.yml"
BENCH_PROJ="$REPO_DIR/tests/BLite.Server.Benchmarks/BLite.Server.Benchmarks.csproj"

SKIP_BUILD=false
SKIP_DOCKER=false
BDN_ARGS=("--filter" "*CrudBenchmarks*")

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-build)  SKIP_BUILD=true  ; shift ;;
    --skip-docker) SKIP_DOCKER=true ; shift ;;
    --filter)      BDN_ARGS=("--filter" "$2") ; shift 2 ;;
    *)             BDN_ARGS+=("$1")            ; shift ;;
  esac
done

# ── Docker lifecycle ──────────────────────────────────────────────────────────
if [[ "$SKIP_DOCKER" == false ]]; then
  if [[ "$SKIP_BUILD" == false ]]; then
    echo "▶  Building blite-server-bench image (context: $PARENT_DIR)..."
    docker build -f "$REPO_DIR/Dockerfile" -t blite-server-bench:latest "$PARENT_DIR"
  fi

  echo "▶  Starting benchmark containers..."
  docker compose -f "$COMPOSE_FILE" up -d

  echo "⏳  Waiting for services to be ready (gRPC :2626, Mongo :27017, up to 90s)..."
  deadline=$(( $(date +%s) + 90 ))
  blite_ok=false ; mongo_ok=false
  while [[ $(date +%s) -lt $deadline ]]; do
    if [[ "$blite_ok" == false ]] && nc -z localhost 2626 2>/dev/null; then blite_ok=true; fi
    if [[ "$mongo_ok" == false ]] && nc -z localhost 27017 2>/dev/null; then mongo_ok=true; fi
    [[ "$blite_ok" == true && "$mongo_ok" == true ]] && break
    sleep 3
  done
  if [[ "$blite_ok" == false || "$mongo_ok" == false ]]; then
    echo "WARNING: Services did not open ports in time; running benchmarks anyway."
  else
    # Extra pause: port open != server ready (gRPC handshake needs a moment).
    sleep 5
  fi
fi

cleanup() {
  if [[ "$SKIP_DOCKER" == false ]]; then
    echo "⏹  Stopping containers..."
    docker compose -f "$COMPOSE_FILE" down
  fi
}
trap cleanup EXIT

# ── Run ───────────────────────────────────────────────────────────────────────
# BenchmarkDotNet searches for the .csproj from Environment.CurrentDirectory.
# cd to the project directory so it finds BLite.Server.Benchmarks.csproj.
BENCH_DIR="$(dirname "$BENCH_PROJ")"
cd "$BENCH_DIR"
echo "🚀  Running benchmarks..."
dotnet run --project "$BENCH_PROJ" -c Release -- "${BDN_ARGS[@]}"

echo "✅  Done. Results are in BenchmarkDotNet.Artifacts/"
