#!/usr/bin/env bash
set -euo pipefail

# This check is intentionally local-only: it must not contact Vercel or expose
# any credential material. The activation operator requires an explicit token,
# so a CLI login or linked-project file is not sufficient readiness evidence.
if [[ -n "${VERCEL_TOKEN:-}" ]]; then
  printf '%s\n' 'VERCEL_AUTH_AVAILABLE'
  exit 0
fi

printf '%s\n' 'VERCEL_AUTH_UNAVAILABLE' >&2
exit 2
