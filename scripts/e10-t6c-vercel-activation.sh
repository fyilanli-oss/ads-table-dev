#!/usr/bin/env bash
set -euo pipefail

[[ "${E10_T6C_CONFIRMATION:-}" == "E10-T6C-ACTIVATE-DEVELOPMENT-OAUTH" ]] || {
  printf '%s\n' 'E10_T6C_CONFIRMATION_INVALID' >&2
  exit 2
}
[[ "${E10_T6C_PRODUCTION_DEPLOY_APPROVAL:-}" == "E10-T6C-APPROVE-PRODUCTION-DEPLOY" ]] || {
  printf '%s\n' 'E10_T6C_PRODUCTION_DEPLOY_APPROVAL_REQUIRED' >&2
  exit 2
}
[[ -n "${VERCEL_TOKEN:-}" ]] || {
  printf '%s\n' 'VERCEL_AUTH_UNAVAILABLE' >&2
  exit 2
}

cleanup() { rm -f /tmp/e10-t6c-production.env; }
trap cleanup EXIT

npx --yes vercel@59.16.0 link --yes --project ads-table-dev --scope firats-projects-d22284db --token "${VERCEL_TOKEN}"
npx --yes vercel@59.16.0 env pull /tmp/e10-t6c-production.env --yes --environment production --token "${VERCEL_TOKEN}"
node --env-file=/tmp/e10-t6c-production.env scripts/e10-t6c-activation-preflight.js
printf 'true\n' | npx --yes vercel@59.16.0 env add SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED production --force --token "${VERCEL_TOKEN}"
npx --yes vercel@59.16.0 deploy --prod --yes --token "${VERCEL_TOKEN}"
