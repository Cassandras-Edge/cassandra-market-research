#!/bin/bash
# ThetaTerminal v3 launcher — writes a creds.txt file from env vars, starts the
# Java terminal in the background, and fronts it with a socat TCP proxy so the
# local-only REST API is reachable from the cluster network.
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

# v3 expects a creds.txt file (line 1 = email, line 2 = password) in the
# same directory as the jar. Positional credentials are no longer accepted.
printf '%s\n%s\n' "$THETA_USERNAME" "$THETA_PASSWORD" > /opt/theta/creds.txt
chmod 600 /opt/theta/creds.txt

echo "Starting ThetaTerminal v3 as user ${THETA_USERNAME}"
# ThetaTerminal uses LMAX Disruptor ring buffers that allocate large
# fixed-size arrays at startup; the JVM default heap (~256MB) is too
# small. THETA_JVM_HEAP can override; default leaves headroom under
# the k8s memory limit.
JVM_HEAP="${THETA_JVM_HEAP:--Xmx1500m}"
cd /opt/theta
java $JVM_HEAP -jar /opt/theta/ThetaTerminal.jar &

echo "Starting socat proxy 0.0.0.0:25511 → 127.0.0.1:25503"
socat TCP-LISTEN:25511,fork,reuseaddr TCP:127.0.0.1:25503 &

# Exit when either process dies so k8s restarts the pod
wait -n
exit $?
