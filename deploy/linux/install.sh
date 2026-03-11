#!/usr/bin/env bash
# BLite Server — Linux Installer
# Copyright (C) 2026 Luca Fabbri — AGPL-3.0
#
# Usage:
#   sudo ./install.sh [--grpc-port N] [--rest-port N] [--studio-port N]
#                     [--root-key KEY] [--source-url URL] [--data-dir DIR]
#                     [--cert-path PATH] [--cert-password PASS]
#                     [--no-studio] [--non-interactive]
#
# Without arguments the script runs interactively and prompts for each setting.

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
INSTALL_DIR="/opt/blite-server"
DATA_DIR="/var/lib/blite-server"
CONFIG_DIR="/etc/blite-server"
SERVICE_USER="blite"
SERVICE_FILE="/etc/systemd/system/blite-server.service"

GRPC_PORT=2626
REST_PORT=2627
STUDIO_PORT=2628
ROOT_KEY=""
SOURCE_URL="https://github.com/EntglDb/BLite.Server"
STUDIO_ENABLED="true"
CERT_PATH=""
CERT_PASSWORD=""
NON_INTERACTIVE=false

# ── Colour helpers ────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ── Argument parsing ─────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --grpc-port)       GRPC_PORT="$2";    shift 2 ;;
        --rest-port)       REST_PORT="$2";    shift 2 ;;
        --studio-port)     STUDIO_PORT="$2";  shift 2 ;;
        --root-key)        ROOT_KEY="$2";     shift 2 ;;
        --source-url)      SOURCE_URL="$2";   shift 2 ;;
        --data-dir)        DATA_DIR="$2";     shift 2 ;;
        --no-studio)       STUDIO_ENABLED="false"; shift ;;
        --cert-path)       CERT_PATH="$2";         shift 2 ;;
        --cert-password)   CERT_PASSWORD="$2";     shift 2 ;;
        --non-interactive) NON_INTERACTIVE=true;    shift ;;
        *) error "Unknown option: $1"; exit 1 ;;
    esac
done

# ── Root check ────────────────────────────────────────────────────────────────
if [[ "$EUID" -ne 0 ]]; then
    error "This installer must be run as root (use sudo)."
    exit 1
fi

# ── Locate the bundle ─────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$SCRIPT_DIR/bin"

if [[ ! -f "$BIN_DIR/BLite.Server" ]]; then
    error "Binary not found at $BIN_DIR/BLite.Server — ensure the archive is fully extracted."
    exit 1
fi

# ── Interactive prompts ───────────────────────────────────────────────────────
prompt() {
    local var_name="$1" prompt_text="$2" default="$3"
    if $NON_INTERACTIVE; then
        # If the variable is already set use it, otherwise use default
        [[ -z "${!var_name}" ]] && printf -v "$var_name" '%s' "$default"
        return
    fi
    read -rp "$prompt_text [$default]: " input
    printf -v "$var_name" '%s' "${input:-$default}"
}

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║       BLite Server — Linux Installer     ║"
echo "╚══════════════════════════════════════════╝"
echo ""

prompt GRPC_PORT   "gRPC port"          "$GRPC_PORT"
prompt REST_PORT   "REST API port"      "$REST_PORT"
prompt STUDIO_PORT "Studio (Blazor) port" "$STUDIO_PORT"

if [[ -z "$ROOT_KEY" ]]; then
    if $NON_INTERACTIVE; then
        error "--root-key is required in non-interactive mode."
        exit 1
    fi
    while [[ -z "$ROOT_KEY" ]]; do
        read -rsp "Root API key (will not be echoed, min 16 chars): " ROOT_KEY
        echo ""
        if [[ ${#ROOT_KEY} -lt 16 ]]; then
            warn "Key must be at least 16 characters. Try again."
            ROOT_KEY=""
        fi
    done
fi

prompt SOURCE_URL     "Source URL (AGPLv3 §13 compliance)" "$SOURCE_URL"
prompt STUDIO_ENABLED "Enable Studio UI (true/false)"      "$STUDIO_ENABLED"
prompt DATA_DIR       "Data directory"                     "$DATA_DIR"

# Optional TLS certificate (leave empty to use plain HTTP)
if ! $NON_INTERACTIVE && [[ -z "$CERT_PATH" ]]; then
    read -rp "Certificate (.pfx/.pem) path [leave empty for plain HTTP]: " _cert_input
    CERT_PATH="${_cert_input:-}"
    if [[ -n "$CERT_PATH" ]]; then
        read -rsp "Certificate password [leave empty if none]: " _pass_input
        echo ""
        CERT_PASSWORD="${_pass_input:-}"
    fi
fi

echo ""
info "Installing BLite Server with the following settings:"
echo "  Install dir   : $INSTALL_DIR"
echo "  Data dir      : $DATA_DIR"
echo "  Config dir    : $CONFIG_DIR"
echo "  gRPC port     : $GRPC_PORT"
echo "  REST port     : $REST_PORT"
echo "  Studio port   : $STUDIO_PORT"
echo "  Studio enabled: $STUDIO_ENABLED"
if [[ -n "$CERT_PATH" ]]; then
echo "  Certificate   : $CERT_PATH (HTTPS enabled)"
else
echo "  TLS           : disabled (plain HTTP)"
fi
echo "  Source URL    : $SOURCE_URL"
echo ""

# ── Create service user ───────────────────────────────────────────────────────
if ! id -u "$SERVICE_USER" &>/dev/null; then
    info "Creating service user '$SERVICE_USER'..."
    useradd --system --no-create-home --shell /sbin/nologin "$SERVICE_USER"
fi

# ── Stop existing service (if running) ───────────────────────────────────────
if systemctl is-active --quiet blite-server 2>/dev/null; then
    info "Stopping existing BLite Server service..."
    systemctl stop blite-server
fi

# ── Install binaries ──────────────────────────────────────────────────────────
info "Installing binaries to $INSTALL_DIR..."
mkdir -p "$INSTALL_DIR"
cp -r "$BIN_DIR"/. "$INSTALL_DIR/"
chmod +x "$INSTALL_DIR/BLite.Server"
chown -R root:root "$INSTALL_DIR"
chmod -R 755 "$INSTALL_DIR"

# ── Create data directory ─────────────────────────────────────────────────────
info "Creating data directory $DATA_DIR..."
mkdir -p "$DATA_DIR/tenants"
chown -R "$SERVICE_USER:$SERVICE_USER" "$DATA_DIR"
chmod 750 "$DATA_DIR"

# ── Write configuration ───────────────────────────────────────────────────────
info "Writing configuration to $CONFIG_DIR..."
mkdir -p "$CONFIG_DIR"

# Determine protocol scheme and install certificate when provided
if [[ -n "$CERT_PATH" ]]; then
    _SCHEME="https"
    _DEST_CERT="$CONFIG_DIR/server.pfx"
    cp "$CERT_PATH" "$_DEST_CERT"
    chown root:"$SERVICE_USER" "$_DEST_CERT"
    chmod 640 "$_DEST_CERT"
    info "Certificate installed at $_DEST_CERT"
    CERT_PATH="$_DEST_CERT"
else
    _SCHEME="http"
fi

# Environment file read by the systemd unit (EnvironmentFile=-/etc/blite-server/environment)
cat > "$CONFIG_DIR/environment" <<EOF
# BLite Server — site configuration
# Generated by install.sh on $(date -u +"%Y-%m-%dT%H:%M:%SZ")
# Edit this file and run: systemctl restart blite-server

# Root user provisioning (used only on first start; cleared after setup completes)
Auth__RootKey=${ROOT_KEY}

# Kestrel endpoint URLs
KESTREL__ENDPOINTS__GRPC__URL=${_SCHEME}://*:${GRPC_PORT}
KESTREL__ENDPOINTS__GRPC__PROTOCOLS=Http2
KESTREL__ENDPOINTS__REST__URL=${_SCHEME}://*:${REST_PORT}
KESTREL__ENDPOINTS__REST__PROTOCOLS=Http1AndHttp2
KESTREL__ENDPOINTS__STUDIO__URL=${_SCHEME}://*:${STUDIO_PORT}
KESTREL__ENDPOINTS__STUDIO__PROTOCOLS=Http1AndHttp2

# Data paths
BLITESERVER__DATABASEPATH=${DATA_DIR}/blite.db
BLITESERVER__DATABASESDIRECTORY=${DATA_DIR}/tenants

# Studio
STUDIO__ENABLED=${STUDIO_ENABLED}

# AGPLv3 §13 source URL
LICENSE__SOURCEURL=${SOURCE_URL}

ASPNETCORE_ENVIRONMENT=Production
EOF

# Append TLS certificate settings when HTTPS is enabled
if [[ -n "$CERT_PATH" ]]; then
    cat >> "$CONFIG_DIR/environment" <<EOFSSL

# TLS certificate
KESTREL__CERTIFICATES__DEFAULT__PATH=${CERT_PATH}
KESTREL__CERTIFICATES__DEFAULT__PASSWORD=${CERT_PASSWORD}
EOFSSL
fi

chmod 640 "$CONFIG_DIR/environment"
chown root:"$SERVICE_USER" "$CONFIG_DIR/environment"

# ── Install systemd unit ──────────────────────────────────────────────────────
info "Installing systemd service..."

cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=BLite Server — self-hosted database (gRPC + REST + Studio)
Documentation=https://github.com/EntglDb/BLite.Server
After=network-online.target
Wants=network-online.target

[Service]
Type=notify
User=${SERVICE_USER}
Group=${SERVICE_USER}

WorkingDirectory=${INSTALL_DIR}
ExecStart=${INSTALL_DIR}/BLite.Server

EnvironmentFile=-${CONFIG_DIR}/environment

Restart=on-failure
RestartSec=5s

NoNewPrivileges=true
ProtectSystem=full
ProtectHome=true
ReadWritePaths=${DATA_DIR}

TimeoutStartSec=120
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
EOF

# ── Enable and start the service ──────────────────────────────────────────────
info "Enabling and starting BLite Server..."
systemctl daemon-reload
systemctl enable blite-server
systemctl start blite-server

echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║  BLite Server installed and running successfully ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════╝${NC}"
echo ""
echo "  gRPC   → ${_SCHEME}://localhost:${GRPC_PORT}"
echo "  REST   → ${_SCHEME}://localhost:${REST_PORT}"
if [[ "$STUDIO_ENABLED" == "true" ]]; then
echo "  Studio → ${_SCHEME}://localhost:${STUDIO_PORT}"
fi
echo ""
echo "  Service status : systemctl status blite-server"
echo "  Logs           : journalctl -u blite-server -f"
echo "  Configuration  : $CONFIG_DIR/environment"
echo "  Data directory : $DATA_DIR"
echo ""
warn "After the first successful start the Auth__RootKey in $CONFIG_DIR/environment"
warn "is no longer required (setup is persisted in $DATA_DIR/server-setup.json)."
warn "You may remove that line from the environment file for security."
