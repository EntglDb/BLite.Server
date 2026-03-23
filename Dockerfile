# BLite.Server — Docker multi-stage build
# Copyright (C) 2026 Luca Fabbri — AGPL-3.0
#
# Build context must be the *parent* directory that contains both:
#   BLite.Server/   — this repository
#   BLite/          — sibling engine repository (EntglDb/BLite)
#
# The CI workflow (release.yml) checks out both repos side-by-side and sets
# the build context to the workspace root before invoking this file.

# ── Stage 1: build ─────────────────────────────────────────────────────────────
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /workspace

# Copy the sibling engine repo and the server source
COPY BLite/ ./BLite/
COPY BLite.Server/ ./BLite.Server/

# Restore + publish self-contained for linux-x64
RUN dotnet publish BLite.Server/src/BLite.Server/BLite.Server.csproj \
        --configuration Release \
        --runtime linux-x64 \
        --self-contained true \
        --output /app/publish \
        -p:UseAppHost=true

# ── Stage 2: runtime ───────────────────────────────────────────────────────────
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app

# Update CA certificates so Let's Encrypt intermediate certs are trusted.
# Without this, outbound HTTPS calls (e.g. LicenseHub heartbeat) fail with
# PartialChain on minimal Debian images.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the self-contained publish output
COPY --from=build /app/publish ./

# ── Default environment ────────────────────────────────────────────────────────
# Use plain HTTP inside the container; TLS termination is handled by the
# reverse proxy / Kubernetes ingress layer.  Override these variables to
# re-enable HTTPS or change ports.

# gRPC (HTTP/2 cleartext)
ENV KESTREL__ENDPOINTS__GRPC__URL=http://*:2626
ENV KESTREL__ENDPOINTS__GRPC__PROTOCOLS=Http2

# REST API
ENV KESTREL__ENDPOINTS__REST__URL=http://*:2627
ENV KESTREL__ENDPOINTS__REST__PROTOCOLS=Http1AndHttp2

# Blazor Studio (disabled by default — enable with Studio__Enabled=true)
ENV KESTREL__ENDPOINTS__STUDIO__URL=http://*:2628
ENV KESTREL__ENDPOINTS__STUDIO__PROTOCOLS=Http1AndHttp2
ENV STUDIO__ENABLED=false
# When Studio is accessed via a reverse proxy (e.g. Plesk / nginx), set this to
# the public hostname so RequireHost() matches the proxied Host header.
# Example: -e STUDIO__HOST=studio.example.com
# Leave unset for direct port access (http://server:2628).
ENV STUDIO__HOST=

# Data paths — mount a volume at /data to persist the database
ENV BLITESERVER__DATABASEPATH=/data/blite.db
ENV BLITESERVER__DATABASESDIRECTORY=/data/tenants
# Page size in bytes for the storage engine (default: 16384). Must be a power of 2.
ENV BLITESERVER__MAXPAGESIZEBYTES=16384

# License management
# AGPLv3 §13 source disclosure URL (override if you self-host a fork)
ENV LICENSE__SOURCEURL=https://github.com/EntglDb/BLite.Server
# URL of the BLite License Hub API (for commercial license validation)
ENV LICENSE__HUBURL=https://api.licensehub.blitedb.com
# Path to a license file on disk (leave empty to use Hub-based validation)
ENV LICENSE__FILEPATH=
# PEM-encoded public key used to verify offline license files (leave empty for Hub validation)
ENV LICENSE__PUBLICKEYPEM=

# Root API key — set Auth__RootKey to provision the root user on first start.
# After the first start the value is no longer required (setup is persisted).
# Example: -e Auth__RootKey=my-secret-key

# Telemetry / OpenTelemetry
# Set Telemetry__Otlp__Endpoint to your OTLP collector URL to enable export.
# Leave empty (default) to disable all OTLP export.
ENV TELEMETRY__ENABLED=true
ENV TELEMETRY__SERVICENAME=blite-server
ENV TELEMETRY__CONSOLE=false
ENV TELEMETRY__OTLP__ENDPOINT=

# Transaction timeout (seconds). A transaction not committed/rolled back within
# this window is automatically aborted.
ENV TRANSACTIONS__TIMEOUTSECONDS=60

# Query result cache
ENV QUERYCACHE__ENABLED=true
# Maximum in-memory size of the query cache in bytes (default: 64 MB)
ENV QUERYCACHE__MAXSIZEBYTES=67108864

# Log level: Trace | Debug | Information | Warning | Error | Critical
ENV LOGGING__LOGLEVEL__DEFAULT=Information

ENV ASPNETCORE_ENVIRONMENT=Production

# ── Volumes & ports ────────────────────────────────────────────────────────────
VOLUME ["/data"]

EXPOSE 2626
EXPOSE 2627
EXPOSE 2628

# ── Entrypoint ─────────────────────────────────────────────────────────────────
ENTRYPOINT ["./BLite.Server"]
