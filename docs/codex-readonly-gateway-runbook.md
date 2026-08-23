# Codex read-only HTTPS gateway runbook

## Boundary and endpoint contract

This gateway is infrastructure for later Dataset V2 acceptance; it does **not** complete E2. It exposes authenticated `GET` operations only:

| Path | Safe output |
|---|---|
| `/api/internal/codex-readonly/health` | Service/read-only status, contract version, timestamp, connectivity boolean |
| `/api/internal/codex-readonly/dataset-v2-contract` | Repository contract, expected names, reachability, explicit catalog limitation |
| `/api/internal/codex-readonly/dataset-v2-access-boundary` | Safe boolean access-boundary assertions |
| `/api/internal/codex-readonly/dataset-v2-safe-counts` | Dataset V2 global row count only |
| `/api/internal/codex-readonly/migration-inventory` | Repository migration inventory and `liveLedgerAvailable=false` |

Every request requires `Authorization: Bearer <ADSTABLE_CODEX_READONLY_TOKEN>`. The token must be generated outside the repository with at least 32 bytes of cryptographic entropy. Never paste it into source, a command argument, logs, evidence, or a persistent file. Routes do not accept query parameters, table/column names, filters, SQL, request bodies, writes, or RPC calls. Responses use `Cache-Control: no-store`.

## Merge/deploy provisioning — exactly two manual entries

Use the same independently generated gateway token in exactly these two locations:

1. Add `ADSTABLE_CODEX_READONLY_TOKEN` as a **Vercel Production Environment Secret**, then deploy Production. Existing backend-only `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` remain unchanged.
2. Add `ADSTABLE_CODEX_READONLY_TOKEN` to the **Codex Environment** used by tasks.

There is no general Codex setup command, GitHub Environment, PostgreSQL/pooler credential, migration, database role, or RPC to provision.

The gateway must never use a Supabase personal access token, the Management API database-query endpoint, a Supabase CLI/npm login session, or files under local CLI session directories. These are broad/session-local client capabilities rather than a durable authorization-level read-only boundary. No Management API fallback is permitted.

## Production smoke acceptance

Run from a fresh Codex task. These commands expand the environment variable only inside the HTTPS request; they do not print its value. Disable shell tracing before use and do not use verbose curl modes.

```bash
set +x
test -n "${ADSTABLE_CODEX_READONLY_TOKEN:-}" || { echo 'gateway credential unavailable'; exit 1; }
BASE_URL=https://YOUR_PRODUCTION_HOST/api/internal/codex-readonly

curl --silent --show-error --output /tmp/adstable-health.json --write-out 'health=%{http_code}\n' \
  --header "Authorization: Bearer ${ADSTABLE_CODEX_READONLY_TOKEN}" "${BASE_URL}/health"
curl --silent --show-error --output /tmp/adstable-boundary.json --write-out 'access-boundary=%{http_code}\n' \
  --header "Authorization: Bearer ${ADSTABLE_CODEX_READONLY_TOKEN}" "${BASE_URL}/dataset-v2-access-boundary"
curl --silent --show-error --output /dev/null --write-out 'missing=%{http_code}\n' "${BASE_URL}/health"
curl --silent --show-error --output /dev/null --write-out 'wrong=%{http_code}\n' \
  --header 'Authorization: Bearer intentionally-invalid' "${BASE_URL}/health"
curl --silent --show-error --output /dev/null --write-out 'unknown=%{http_code}\n' \
  --header "Authorization: Bearer ${ADSTABLE_CODEX_READONLY_TOKEN}" "${BASE_URL}/unknown"
```

Expected codes are `200`, `200`, `401`, `401`, and `404`. Inspect the two JSON files only for their documented allowlisted contracts, then delete them. Acceptance of persistent access requires repeating the authenticated check successfully in a different, newly created Codex task after deployment.

## Rotation, revocation, and rollback

Rotation changes the same value in the two entries above and redeploys. For rollback/revocation:

1. Remove `ADSTABLE_CODEX_READONLY_TOKEN` from Vercel Production.
2. Remove it from the Codex Environment.
3. Redeploy or restore the prior deployment. With no valid server token, the registered gateway fails closed with `503`.
4. If necessary, revert the gateway route registration and deploy again.

No database rollback is needed: this package changes no schema or data and leaves the existing application and E1 security path intact.

## Safe observability

One JSON audit event is emitted per routed request with only request ID, operation, status, duration, contract version, and rate-limit result. Never add authorization headers, token material or derivatives, Supabase configuration/errors, IP addresses, row data, or user/account/entity/provider fields to these events.
