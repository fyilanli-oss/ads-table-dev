#!/usr/bin/env bash
set -euo pipefail

[[ "${E10_T6C_REDEPLOY_CONFIRMATION:-}" == "E10-T6C-REDEPLOY-WITHOUT-ACTIVATION" ]] || {
  printf '%s\n' 'E10_T6C_REDEPLOY_CONFIRMATION_INVALID' >&2
  exit 2
}
[[ -n "${VERCEL_TOKEN:-}" ]] || {
  printf '%s\n' 'VERCEL_AUTH_UNAVAILABLE' >&2
  exit 2
}

npx --yes vercel@59.16.0 link --yes --project ads-table-dev --scope firats-projects-d22284db --token "${VERCEL_TOKEN}"
npx --yes vercel@59.16.0 deploy --prod --yes --token "${VERCEL_TOKEN}"
