#!/usr/bin/env bash
# Jeevo connector installer; everything but the server URL comes from that server's discovery document.
set -euo pipefail

JEEVO_URL="${JEEVO_URL:-__JEEVO_URL__}"
SERVICE=jeevo-connector
SERVICE_USER=jeevo-connector
CONF_DIR=/etc/jeevo-connector
CONF="$CONF_DIR/connector.json"
BIN_LINK=/usr/local/bin/jeevo-connector
PORT=28443
SYSTEMD="${JEEVO_SYSTEMD:-1}"
ASSUME_YES="${JEEVO_ASSUME_YES:-0}"
CHANNEL="${JEEVO_CHANNEL:-stable}"

# ---- output ---------------------------------------------------------------------------
c_bold=; c_ok=; c_off=; e_warn=; e_err=; e_off=
if [[ -z "${NO_COLOR:-}" && "${TERM:-dumb}" != dumb ]]; then
  if [[ -t 1 ]]; then c_bold=$'\033[1m'; c_ok=$'\033[32m'; c_off=$'\033[0m'; fi
  if [[ -t 2 ]]; then e_warn=$'\033[33m'; e_err=$'\033[31m'; e_off=$'\033[0m'; fi
fi
STEP=0
TOTAL=8; [[ "$SYSTEMD" != 1 ]] || TOTAL=9
DIED=0
step() { STEP=$((STEP + 1)); printf '\n%s[%d/%d] %s%s\n' "$c_bold" "$STEP" "$TOTAL" "$*" "$c_off"; }
ok()   { printf '  %sok%s     %s\n' "$c_ok" "$c_off" "$*"; }
note() { printf '         %s\n' "$*"; }
warn() { printf '  %swarn%s   %s\n' "$e_warn" "$e_off" "$*" >&2; }
die()  { DIED=1; printf '  %serror%s  %s\n' "$e_err" "$e_off" "$*" >&2; exit 1; }
kv()   { printf '  %-10s %s\n' "$1" "$2"; }

WORK=""
LOG=/dev/null
cleanup() {
  local rc=$?
  if [[ $rc -ne 0 && $DIED -eq 0 ]]; then
    printf '  %serror%s  The installer stopped unexpectedly (exit %d). It is safe to run it again.\n' "$e_err" "$e_off" "$rc" >&2
  fi
  [[ -z "$WORK" ]] || rm -rf "$WORK"
}
trap cleanup EXIT

# Output of quiet commands goes to the log; it is shown only when a command fails.
quiet() { "$@" >>"$LOG" 2>&1; }
must() { # must "failure message" command...
  local msg="$1"; shift
  quiet "$@" || { tail -n 20 "$LOG" >&2; die "$msg"; }
}

# The script usually arrives on stdin (curl | bash), so questions read the terminal directly.
HAVE_TTY=0
if ( : </dev/tty ) 2>/dev/null; then HAVE_TTY=1; fi
interactive() { [[ "$ASSUME_YES" != 1 && $HAVE_TTY -eq 1 ]]; }

ask() { # ask VAR "Question" default
  local var="$1" q="$2" def="$3" reply=""
  if [[ -z "${!var:-}" ]]; then
    if interactive; then
      read -r -p "  $q [$def]: " reply </dev/tty || true
    fi
    printf -v "$var" '%s' "${reply:-$def}"
  fi
  kv "$q" "${!var}"
}

confirm() { # confirm "Question" y|n
  local def="${2:-y}" reply=""
  interactive || { [[ "$def" == y ]]; return; }
  read -r -p "  $1 [$([[ $def == y ]] && echo Y/n || echo y/N)]: " reply </dev/tty || true
  [[ "${reply:-$def}" =~ ^[Yy] ]]
}

# fetch URL PROTO MAX_SECONDS OUTFILE [progress]
fetch() {
  local quietflag=-s
  [[ "${5:-}" == progress && -t 2 ]] && quietflag=--progress-bar
  curl -fSL "$quietflag" --retry 2 --proto "$2" --proto-redir "$2" --max-time "$3" "$1" -o "$4"
}

json() { python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(eval(sys.argv[2], {}, {'d': d}))" "$@"; }

# proto_for URL VAR: sets VAR to the curl protocols to allow (also for redirects) for that URL.
proto_for() {
  if [[ "$1" == https://* ]]; then printf -v "$2" '%s' '=https'
  elif [[ "$1" =~ ^http://(localhost|127\.0\.0\.1|\[::1\])([:/]|$) ]]; then printf -v "$2" '%s' '=https,http'
  else die "$1 is not an https:// URL. The installer only trusts a Jeevo server over https (http works for localhost only)."
  fi
}

# ---- 1. system check ---------------------------------------------------------------------
step "Checking this machine"
[[ $EUID -eq 0 ]] || die "Run as root: curl -fsSL <url> | sudo bash"
[[ "$(uname -s)" == Linux ]] || die "The connector runs on Linux only."
case "$(uname -m)" in
  x86_64|amd64) ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) die "Unsupported CPU architecture $(uname -m) (need x86_64 or arm64)." ;;
esac
for bin in mktemp runuser setsid id; do
  command -v "$bin" >/dev/null || die "$bin is required but not installed (util-linux and coreutils)."
done
if [[ "$SYSTEMD" == 1 ]]; then
  if ! command -v systemctl >/dev/null || [[ ! -d /run/systemd/system ]]; then die "systemd is required (set JEEVO_SYSTEMD=0 to install without a service)."; fi
fi
# Built from two halves so the server's substitution cannot rewrite the check.
UNSET="__JEEVO""_URL__"
if [[ -z "$JEEVO_URL" || "$JEEVO_URL" == "$UNSET" ]]; then
  die "This copy of the installer has no Jeevo server set. Use the command shown in Jeevo > Settings > Connectors."
fi
JEEVO_URL="${JEEVO_URL%/}"
proto_for "$JEEVO_URL" PROTO
# shellcheck disable=SC1091
DISTRO="$(. /etc/os-release 2>/dev/null; printf "%s" "${PRETTY_NAME:-}")"
ok "Linux $ARCH${DISTRO:+, $DISTRO}"

if [[ -f "$CONF" ]]; then
  warn "A connector is already installed here ($CONF)."
  note "Update it:     sudo jeevo-connector update"
  note "Check it:      sudo jeevo-connector status"
  note "To reinstall, remove $CONF first (media in the data directory is kept)."
  DIED=1
  exit 1
fi

WORK="$(mktemp -d)"
LOG="$WORK/install.log"

# ---- 2. dependencies ---------------------------------------------------------------------
step "Installing dependencies"
if [[ "${JEEVO_SKIP_PACKAGES:-0}" == 1 ]]; then
  warn "skipped (JEEVO_SKIP_PACKAGES=1)"
elif command -v apt-get >/dev/null; then
  export DEBIAN_FRONTEND=noninteractive
  must "apt-get update failed." apt-get update -qq
  must "Installing packages failed." apt-get install -y -qq curl ca-certificates tar gzip openssl ffmpeg python3 python3-venv python3-pip
  ok "ffmpeg, python3, openssl and tools"
elif command -v dnf >/dev/null; then
  must "Installing packages failed." dnf install -y -q curl ca-certificates tar gzip openssl python3 python3-pip
  quiet dnf install -y -q ffmpeg || quiet dnf install -y -q ffmpeg-free || warn "ffmpeg was not installed (on RHEL/Fedora it comes from RPM Fusion)"
  ok "python3, openssl and tools"
elif command -v pacman >/dev/null; then
  must "Installing packages failed." pacman -Sy --noconfirm --needed curl ca-certificates tar gzip openssl ffmpeg python python-pip
  ok "ffmpeg, python, openssl and tools"
elif command -v zypper >/dev/null; then
  must "Installing packages failed." zypper -q -n install curl ca-certificates tar gzip openssl ffmpeg python3 python3-pip
  ok "ffmpeg, python3, openssl and tools"
elif command -v apk >/dev/null; then
  must "Installing packages failed." apk add -q curl ca-certificates tar gzip openssl ffmpeg python3 py3-pip
  ok "ffmpeg, python3, openssl and tools"
else
  die "No supported package manager found. Install ffmpeg, python3 (with venv) and openssl, then re-run with JEEVO_SKIP_PACKAGES=1."
fi
for bin in curl tar sha256sum base64 install openssl python3 ffmpeg ffprobe awk df getent useradd usermod; do
  command -v "$bin" >/dev/null || die "$bin is required but not installed."
done
python3 -c 'import venv, ensurepip' 2>/dev/null || die "python3 venv support is missing (Debian/Ubuntu: apt install python3-venv)."

# ---- 3. discovery ------------------------------------------------------------------------
step "Contacting $JEEVO_URL"
fetch "$JEEVO_URL/.well-known/jeevo-connector.json" "$PROTO" 30 "$WORK/discovery.json" || die "Could not reach $JEEVO_URL. Check the server's internet connection."
RELEASES_URL="$(json "$WORK/discovery.json" "d['releasesUrl']")"
KEYS="$(json "$WORK/discovery.json" "' '.join(d['releaseSigningKeys'])")"
[[ -n "$KEYS" ]] || die "This Jeevo server publishes no release signing key, so the download cannot be verified."
proto_for "$RELEASES_URL" RELEASES_PROTO
# Optional pin: trust only the key whose sha256 you were given out of band.
PIN="$(printf '%s' "${JEEVO_RELEASE_KEY_FINGERPRINT:-}" | tr -d ':[:space:]' | tr '[:upper:]' '[:lower:]')"
if [[ -n "$PIN" ]]; then
  pinned=""
  for key in $KEYS; do
    [[ "$(printf '%s' "$key" | base64 -d | sha256sum | cut -d' ' -f1)" == "$PIN" ]] && pinned+="$key "
  done
  [[ -n "$pinned" ]] || die "None of this Jeevo server's release signing keys matches JEEVO_RELEASE_KEY_FINGERPRINT."
  KEYS="$pinned"
  ok "signing key pinned by fingerprint"
fi
ok "Jeevo server found"

# ---- 4. locations ------------------------------------------------------------------------
step "Choosing locations"
note "The install directory holds the program (small). The data directory holds"
note "every uploaded file and its processed copies: put it on your big disk."
ask INSTALL_DIR "Install directory" "${JEEVO_INSTALL_DIR:-/opt/jeevo-connector}"
ask DATA_DIR "Data directory" "${JEEVO_DATA_DIR:-/var/lib/jeevo-connector}"
for d in "$INSTALL_DIR" "$DATA_DIR"; do
  [[ "$d" =~ ^/[A-Za-z0-9._/-]*$ ]] || die "$d must be an absolute path using only letters, digits and . _ - /"
done
INSTALL_DIR="${INSTALL_DIR%/}"; DATA_DIR="${DATA_DIR%/}"
[[ -n "$INSTALL_DIR" && -n "$DATA_DIR" && "$INSTALL_DIR" != "$DATA_DIR" ]] || die "Use two different directories for the program and the data (not /)."
mkdir -p "$DATA_DIR"
FREE_GB="$(df -Pk "$DATA_DIR" | awk 'NR==2 {printf "%d", $4/1024/1024}')"
ok "data directory has $FREE_GB GB free"
if (( FREE_GB < 10 )); then warn "less than 10 GB free in $DATA_DIR: media will fill it quickly"; fi

GPU="${JEEVO_GPU:-}"
if [[ -z "$GPU" ]]; then
  GPU=0
  if command -v nvidia-smi >/dev/null && nvidia-smi -L >/dev/null 2>&1; then
    if confirm "NVIDIA GPU found. Use it for NSFW scanning (installs the CUDA runtime)?" y; then GPU=1; fi
  fi
fi
[[ "$GPU" == 0 || "$GPU" == 1 ]] || die "JEEVO_GPU must be 0 or 1."
VARIANT=cpu; ACCEL=CPU
if [[ "$GPU" == 1 ]]; then VARIANT=gpu; ACCEL=CUDA; fi

MEMORY_MAX="${JEEVO_MEMORY_MAX:-}"
[[ -z "$MEMORY_MAX" || "$MEMORY_MAX" =~ ^[0-9]+([KMGT]|%)?$ ]] || die "JEEVO_MEMORY_MAX must look like 8G, 4096M or 50%."

# ---- 5. signed release -------------------------------------------------------------------
step "Downloading the connector ($CHANNEL channel)"
fetch "$RELEASES_URL/$CHANNEL" "$RELEASES_PROTO" 60 "$WORK/release.json" || die "No release available on the $CHANNEL channel."
# print() adds a newline the signed bytes do not have.
python3 -c "import json,sys; sys.stdout.write(json.load(open(sys.argv[1]))['manifest'])" "$WORK/release.json" >"$WORK/manifest.json"
python3 -c "import json,sys,base64; sys.stdout.buffer.write(base64.b64decode(json.load(open(sys.argv[1]))['signature']))" "$WORK/release.json" >"$WORK/manifest.sig"

# ed25519_verify BASE64_KEY MESSAGE_FILE SIGNATURE_FILE
ed25519_verify() {
  # Raw 32-byte Ed25519 key -> SubjectPublicKeyInfo PEM.
  { echo "-----BEGIN PUBLIC KEY-----"; { printf '\x30\x2a\x30\x05\x06\x03\x2b\x65\x70\x03\x21\x00'; printf '%s' "$1" | base64 -d; } | base64 -w0; echo; echo "-----END PUBLIC KEY-----"; } >"$WORK/key.pem"
  openssl pkeyutl -verify -pubin -inkey "$WORK/key.pem" -rawin -in "$2" -sigfile "$3" >/dev/null 2>&1
}

verified=0
for key in $KEYS; do
  if ed25519_verify "$key" "$WORK/manifest.json" "$WORK/manifest.sig"; then verified=1; break; fi
done
if [[ $verified != 1 ]]; then
  # Tell a too-old OpenSSL from a key mismatch: RFC 8032 test vector 2 must verify if Ed25519 works at all.
  printf 'r' >"$WORK/vector.msg"
  printf '%s' 'kqAJqfDUyrhyDoILX2QlQKKye1QWUD+Ps3YiI+vbadoIWsHkPhWZbkWPNhPQ8R2MOHsurrQwKu6wDSkWErsMAA==' | base64 -d >"$WORK/vector.sig"
  if ! ed25519_verify 'PUAXw+hDiVqStwqnTRt+vJyYLM8uxJaMwM1V8Sr0Zgw=' "$WORK/vector.msg" "$WORK/vector.sig"; then
    die "This system's OpenSSL ($(openssl version 2>/dev/null || echo unknown)) cannot check Ed25519 signatures. Install OpenSSL 1.1.1 or newer and run the installer again."
  fi
  REL_VERSION="$(json "$WORK/manifest.json" "d.get('version', '?')" 2>/dev/null || echo '?')"
  warn "The $CHANNEL release ($REL_VERSION) is not signed by any key this Jeevo server trusts:"
  for key in $KEYS; do note "${key:0:12}..."; done
  die "Not installing. The Jeevo server's operator needs CONNECTOR_RELEASE_PUBLIC_KEYS to include the key that release was signed with (a dev release needs CONNECTOR_DEV_MODE=true)."
fi
ok "release signature verified"

VERSION="$(json "$WORK/manifest.json" "d['version']")"
read -r FILE SHA SIZE < <(python3 - "$WORK/manifest.json" "$ARCH" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))
for a in m["artifacts"]:
    if a["os"] == "linux" and a["arch"] == sys.argv[2]:
        print(a["file"], a["sha256"], a["size"]); break
else:
    sys.exit(1)
PY
) || die "Release $VERSION has no build for linux/$ARCH."
[[ "$FILE" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ && "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ && "$SHA" =~ ^[0-9a-f]{64}$ && "$SIZE" =~ ^[0-9]+$ ]] \
  || die "Release manifest contains an unsafe or malformed entry."
fetch "$RELEASES_URL/files/$VERSION/$FILE" "$RELEASES_PROTO" 1800 "$WORK/$FILE" progress || die "Download failed."
[[ "$(stat -c%s "$WORK/$FILE")" == "$SIZE" ]] || die "Size mismatch: the download is incomplete or was tampered with."
echo "$SHA  $WORK/$FILE" | sha256sum -c --quiet - >/dev/null 2>&1 || die "Checksum mismatch: the download was corrupted or tampered with."
ok "jeevo-connector $VERSION downloaded and verified"

# ---- 6. service user and files -----------------------------------------------------------
step "Installing to $INSTALL_DIR"
if ! id "$SERVICE_USER" >/dev/null 2>&1; then
  must "Could not create the $SERVICE_USER user." useradd --system --home-dir "$DATA_DIR" --shell /usr/sbin/nologin "$SERVICE_USER"
  ok "created system user $SERVICE_USER"
fi
# video/render give access to /dev/dri (Intel Quick Sync) and the NVIDIA devices for hardware encoding.
for g in video render; do
  if getent group "$g" >/dev/null; then quiet usermod -aG "$g" "$SERVICE_USER" || warn "could not add $SERVICE_USER to group $g"; fi
done
REL="$INSTALL_DIR/releases/$VERSION"
mkdir -p "$REL"
must "Unpacking the release failed." tar -xzf "$WORK/$FILE" -C "$REL" --no-same-owner --no-same-permissions
[[ -x "$REL/jeevo-connector" ]] || die "The release archive has no jeevo-connector binary."
ln -sfn "releases/$VERSION" "$INSTALL_DIR/current"
chown -R "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR" "$DATA_DIR"
chmod 750 "$DATA_DIR"
mkdir -p "$CONF_DIR"
chown "$SERVICE_USER:$SERVICE_USER" "$CONF_DIR"
chmod 700 "$CONF_DIR"
for d in "$INSTALL_DIR" "$DATA_DIR"; do
  runuser -u "$SERVICE_USER" -- test -x "$d" -a -w "$d" \
    || die "$SERVICE_USER cannot reach $d (a parent directory is private, e.g. under /root). Choose another location."
done
ok "installed $VERSION"

# Root-owned admin wrapper: root never executes the service-user-owned tree, and runs it in a new session.
rm -f "$BIN_LINK"
cat >"$WORK/wrapper" <<WRAPPER
#!/bin/sh
# Written by the Jeevo installer. Root-owned; do not replace with a symlink.
BIN="$INSTALL_DIR/current/jeevo-connector"
[ "\$(id -u)" -eq 0 ] || exec "\$BIN" "\$@"
exec setsid --wait runuser -u $SERVICE_USER -- "\$BIN" "\$@"
WRAPPER
install -m 0755 -o root -g root "$WORK/wrapper" "$BIN_LINK"
ok "installed $BIN_LINK"

# ---- 7. AI worker ------------------------------------------------------------------------
step "Setting up the AI worker (NSFW scanning, $ACCEL)"
REQ="$REL/ai/requirements-$VARIANT.txt"
[[ -f "$REQ" ]] || die "The release has no AI worker requirements for $ACCEL."
must "Creating the Python environment failed." python3 -m venv "$INSTALL_DIR/venv"
PIP=("$INSTALL_DIR/venv/bin/pip" install --disable-pip-version-check -q)
must "Upgrading pip failed." "${PIP[@]}" --upgrade pip
must "Installing the AI worker's Python packages failed." "${PIP[@]}" -r "$REQ"
echo "$VARIANT" >"$INSTALL_DIR/venv/.jeevo-requirements"
sha256sum "$REQ" | cut -d' ' -f1 >"$INSTALL_DIR/venv/.jeevo-requirements.sha256"
chown -R "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR/venv"
ok "AI worker ready; the model downloads on first start"

# ---- 8. license key ----------------------------------------------------------------------
step "Connecting to your Jeevo account"
note "In Jeevo: Settings > Connectors > Add connector. Paste the license key here."
attempt=0
while :; do
  attempt=$((attempt + 1))
  KEY="${JEEVO_LICENSE_KEY:-}"
  if [[ -z "$KEY" ]]; then
    [[ $HAVE_TTY -eq 1 ]] || die "No terminal to ask for the license key: set JEEVO_LICENSE_KEY."
    read -r -s -p "  License key: " KEY </dev/tty || die "No license key entered."
    echo
  fi
  KEY="$(printf '%s' "$KEY" | tr -d '[:space:]' | tr '[:lower:]' '[:upper:]')"
  # The key travels in the environment, not on the command line (visible in ps).
  if [[ -n "$KEY" ]] && JEEVO_LICENSE_KEY="$KEY" runuser -u "$SERVICE_USER" -- "$REL/jeevo-connector" enroll --url "$JEEVO_URL" \
       --install-dir "$INSTALL_DIR" --data-dir "$DATA_DIR" --config "$CONF"; then
    break
  fi
  if [[ -n "${JEEVO_LICENSE_KEY:-}" || $attempt -ge 3 ]]; then
    die "Enrolment failed. Generate a new license key in Jeevo and run the installer again."
  fi
  warn "That did not work; try again."
done
chmod 600 "$CONF"
ok "connector enrolled"

# ---- 9. service --------------------------------------------------------------------------
if [[ "$SYSTEMD" == 1 ]]; then
  step "Starting the service"
  # Not capped by default: transcoding and the AI worker need a lot of memory.
  MEMORY_LINE="# MemoryMax=8G  (uncapped; reinstall with JEEVO_MEMORY_MAX=8G or edit this line to cap it)"
  [[ -z "$MEMORY_MAX" ]] || MEMORY_LINE="MemoryMax=$MEMORY_MAX"
  cat >/etc/systemd/system/$SERVICE.service <<UNIT
[Unit]
Description=Jeevo connector (media storage and streaming)
Documentation=https://github.com/NotNecrotic/jeevo-connector
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
Group=$SERVICE_USER
Environment=HOME=$DATA_DIR
Environment=JEEVO_CONNECTOR_CONFIG=$CONF
ExecStart=$INSTALL_DIR/current/jeevo-connector run --config $CONF
# Exits 0 after installing an update; systemd starts the new version.
Restart=always
RestartSec=3
# Lets an owner choose a listen port below 1024 without running as root (the default, $PORT, needs nothing).
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=$INSTALL_DIR $DATA_DIR $CONF_DIR
ProtectHome=true
PrivateTmp=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectKernelLogs=true
ProtectControlGroups=true
RestrictSUIDSGID=true
RestrictNamespaces=true
RestrictRealtime=true
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX AF_NETLINK
SystemCallArchitectures=native
LockPersonality=true
LimitNOFILE=65536
TasksMax=4096
$MEMORY_LINE
# No DevicePolicy/PrivateDevices on purpose: hardware encoding needs /dev/dri
# (Intel/AMD, group render/video) and the NVIDIA device nodes.

[Install]
WantedBy=multi-user.target
UNIT
  must "systemctl daemon-reload failed." systemctl daemon-reload
  must "Could not enable and start $SERVICE." systemctl enable --now "$SERVICE"
  for _ in $(seq 1 30); do systemctl is-active --quiet "$SERVICE" && break; sleep 1; done
  if ! systemctl is-active --quiet "$SERVICE"; then
    journalctl -u "$SERVICE" -n 15 --no-pager >&2 || true
    die "The service did not start: journalctl -u $SERVICE -n 50"
  fi
  ok "$SERVICE is running"

  if command -v ufw >/dev/null && ufw status 2>/dev/null | grep -q "Status: active"; then
    if confirm "ufw is active. Allow the connector port (TCP $PORT) so viewers can reach it directly?" y; then
      must "ufw allow failed." ufw allow "$PORT/tcp"
      ok "opened $PORT/tcp in ufw"
    fi
  elif command -v firewall-cmd >/dev/null && firewall-cmd --state >/dev/null 2>&1; then
    if confirm "firewalld is active. Allow the connector port (TCP $PORT)?" y; then
      must "firewall-cmd failed." firewall-cmd -q --permanent --add-port="$PORT/tcp"
      must "firewall-cmd reload failed." firewall-cmd -q --reload
      ok "opened $PORT/tcp in firewalld"
    fi
  fi
fi

# ---- summary -----------------------------------------------------------------------------
DOMAIN="$(json "$CONF" "d['domain']")"
if [[ "$SYSTEMD" == 1 ]]; then SERVICE_STATE="$(systemctl is-active "$SERVICE" || true)"; else SERVICE_STATE="not installed (JEEVO_SYSTEMD=0)"; fi
printf '\n%sInstalled jeevo-connector %s%s\n\n' "$c_ok" "$VERSION" "$c_off"
kv Domain "https://$DOMAIN (DNS and the TLS certificate are set up automatically)"
kv Program "$INSTALL_DIR"
kv Media "$DATA_DIR"
kv Service "$SERVICE_STATE"
printf '\nNext\n'
kv Status "sudo jeevo-connector status"
if [[ "$SYSTEMD" == 1 ]]; then kv Logs "sudo journalctl -u $SERVICE -f"; fi
kv Update "sudo jeevo-connector update"
printf '\nFor viewers to stream straight from this server, TCP %s must be reachable\n' "$PORT"
printf 'from the internet (forward it on your router if you are behind one). If it\n'
printf 'is not, Jeevo relays your media instead, at reduced quality.\n'
printf 'Everything else is configured in Jeevo > Settings > Connectors.\n'
