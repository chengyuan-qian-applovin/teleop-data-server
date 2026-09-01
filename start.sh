#!/usr/bin/env bash
# Start the teleop data server. All settings live in fleet.env next to this script.
#
#   ./start.sh            run in the foreground (dev / quick check)
#   ./start.sh service    install + (re)start the duo-fleet systemd service (uses sudo)
#
# The installed service reads fleet.env directly, so later settings changes
# are just: edit fleet.env, then `sudo systemctl restart duo-fleet`.
set -euo pipefail
cd "$(dirname "$(readlink -f "$0")")"

[ -f fleet.env ] || { echo "fleet.env not found — copy fleet.env.example to fleet.env and edit it" >&2; exit 1; }
set -a
source fleet.env
set +a

if [ ! -d .venv ]; then
  echo "Creating .venv and installing dependencies..."
  python3 -m venv .venv
  .venv/bin/pip install -q -r requirements.txt
fi

if [ "${1:-}" = "service" ]; then
  unit="$(mktemp)"
  cat > "$unit" <<EOF
[Unit]
Description=Teleop Data Server
After=network.target

[Service]
User=$USER
WorkingDirectory=$PWD
EnvironmentFile=$PWD/fleet.env
ExecStart=$PWD/.venv/bin/uvicorn fleet_server.app:app --host \${FLEET_HOST} --port \${FLEET_PORT} --workers 1
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
  sudo cp "$unit" /etc/systemd/system/duo-fleet.service
  rm -f "$unit"
  sudo systemctl daemon-reload
  sudo systemctl enable duo-fleet
  sudo systemctl restart duo-fleet
  sleep 1
  if curl -sf "http://127.0.0.1:${FLEET_PORT}/api/health" > /dev/null; then
    echo "duo-fleet is running: http://$(hostname -I | awk '{print $1}'):${FLEET_PORT}/"
  else
    echo "duo-fleet did not come up — check: journalctl -u duo-fleet -n 50" >&2
    exit 1
  fi
else
  exec .venv/bin/uvicorn fleet_server.app:app --host "$FLEET_HOST" --port "$FLEET_PORT" --workers 1
fi
