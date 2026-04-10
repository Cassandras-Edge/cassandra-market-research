# ThetaTerminal sidecar — Java app distributed by ThetaData that authenticates
# against an account on startup and exposes the REST API on :25510.
#
# The jar lives at https://download-stable.thetadata.us (the URL itself is
# the binary). We download it at build time; rebuilds happen via Woodpecker.
#
# IMPORTANT: ThetaTerminal binds the HTTP REST server to 127.0.0.1 only and
# has no documented option to listen on all interfaces. We run socat as a
# TCP proxy from 0.0.0.0:25511 → 127.0.0.1:25510 so a k8s ClusterIP service
# can reach it. The same trick is used by every published ThetaData docker
# setup.
#
# Credentials (THETA_USERNAME / THETA_PASSWORD) are injected at runtime via
# k8s Secret env vars and passed to ThetaTerminal.jar as positional args.

FROM eclipse-temurin:21-jre

ARG DEBIAN_FRONTEND=noninteractive
ARG THETA_TERMINAL_URL=https://download-stable.thetadata.us

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

# Internal: ThetaTerminal binds 127.0.0.1:25510 by design.
# External: socat exposes 0.0.0.0:25511 → 127.0.0.1:25510 for k8s.
EXPOSE 25511

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD curl -fsS "http://127.0.0.1:25511/v2/list/roots/option" >/dev/null || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/theta/entrypoint.sh"]
