#!/usr/bin/env bash
# Stop the teleop data server.
#
#   ./stop.sh             stop the duo-fleet service and any foreground runs
#   ./stop.sh --disable   also stop the service from starting at boot
#
# Start again with ./start.sh (foreground) or ./start.sh service.
set -euo pipefail

# The systemd service, if installed (needs sudo).
if [ -f /etc/systemd/system/duo-fleet.service ]; then
  if systemctl is-active --quiet duo-fleet; then
    sudo systemctl stop duo-fleet
    echo "duo-fleet service stopped."
  else
    echo "duo-fleet service is not running."
  fi
  if [ "${1:-}" = "--disable" ]; then
    sudo systemctl disable duo-fleet
    echo "duo-fleet will no longer start at boot."
  fi
fi

# Any foreground / manual runs of the server by this user. pgrep -f also
# matches wrapper shells whose command line mentions uvicorn, so keep only
# real python/uvicorn processes.
server_pids() {
  for p in $(pgrep -u "$USER" -f 'uvicorn fleet_server.app:app' || true); do
    case "$(ps -o comm= -p "$p" 2>/dev/null)" in python*|uvicorn) echo "$p" ;; esac
  done
}
pids="$(server_pids)"
if [ -n "$pids" ]; then
  kill $pids
  echo "Stopped foreground server process(es): $(echo $pids | tr '\n' ' ')"
fi

if [ -n "$(server_pids)" ]; then
  echo "Warning: some server processes are still shutting down." >&2
else
  echo "No teleop data server is running."
fi
