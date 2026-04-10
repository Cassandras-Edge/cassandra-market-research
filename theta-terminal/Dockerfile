# ThetaTerminal v3 sidecar — Java app distributed by ThetaData that authenticates
# against an account on startup and exposes the REST API on :25503 by default.
#
# v3 is a separate jar from v2 (breaking API change). We pull it from
# download-unstable.thetadata.us since v3 is still in beta as of 2026.
#
# IMPORTANT: ThetaTerminal binds the HTTP REST server to 127.0.0.1 only and
# has no documented option to listen on all interfaces. We run socat as a
# TCP proxy from 0.0.0.0:25511 → 127.0.0.1:25503 so a k8s ClusterIP service
# can reach it.
#
# Credentials (THETA_USERNAME / THETA_PASSWORD) are injected at runtime via
# k8s Secret env vars and written into a creds.txt file at startup
# (email\npassword) — v3 does not accept positional-arg credentials anymore.

FROM eclipse-temurin:21-jre

ARG DEBIAN_FRONTEND=noninteractive
ARG THETA_TERMINAL_URL=https://download-unstable.thetadata.us/ThetaTerminalv3.jar

RUN apt-get update && apt-get install -y --no-install-recommends \
      curl ca-certificates socat tini \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -g 1001 theta && useradd -u 1001 -g theta -s /bin/bash -m theta

WORKDIR /opt/theta

RUN curl -fsSL -o ThetaTerminal.jar "$THETA_TERMINAL_URL" \
    && test -s ThetaTerminal.jar \
    && chown -R theta:theta /opt/theta

COPY --chown=theta:theta entrypoint.sh /opt/theta/entrypoint.sh
RUN chmod +x /opt/theta/entrypoint.sh

USER theta

# Internal: ThetaTerminal v3 binds 127.0.0.1:25503 by design.
# External: socat exposes 0.0.0.0:25511 → 127.0.0.1:25503 for k8s.
EXPOSE 25511

HEALTHCHECK --interval=30s --timeout=5s --start-period=120s --retries=3 \
  CMD curl -fsS "http://127.0.0.1:25511/v3/option/list/expirations?symbol=AAPL&format=json" >/dev/null || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/theta/entrypoint.sh"]
