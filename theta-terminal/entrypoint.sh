#!/bin/bash
# ThetaTerminal launcher — starts the Java terminal in the background and
# fronts it with a socat TCP proxy so the local-only REST API is reachable
# from the cluster network.
set -euo pipefail

if [ -z "${THETA_USERNAME:-}" ] || [ -z "${THETA_PASSWORD:-}" ]; then
  echo "FATAL: THETA_USERNAME and THETA_PASSWORD env vars are required" >&2
  exit 1
fi

cleanup() {
  echo "Shutting down ThetaTerminal sidecar"
  jobs -p | xargs -r kill 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "Starting ThetaTerminal as user ${THETA_USERNAME}"
java -jar /opt/theta/ThetaTerminal.jar "$THETA_USERNAME" "$THETA_PASSWORD" &

echo "Starting socat proxy 0.0.0.0:25511 → 127.0.0.1:25510"
socat TCP-LISTEN:25511,fork,reuseaddr TCP:127.0.0.1:25510 &

# Exit when either process dies so k8s restarts the pod
wait -n
exit $?
