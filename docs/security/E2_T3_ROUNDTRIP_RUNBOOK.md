# E2-T3 Canonical Round-Trip Runbook

## Status and boundary

This package is **preparation only**. No production SQL has been executed. E2-T3 remains `Verification` until a separately approved operation and postcheck pass.

The controlled operation is limited to one namespaced Meta paid Dataset V2 insert, one read-back, semantic assertions, and mandatory transaction rollback. It must never commit. It must not write V1, snapshots, auth, OAuth, token, ledger, schema, grants, RLS, policies, functions, or migrations.

## Immutable sources

- `artifacts/dataset-v2-acceptance/e2-t3-roundtrip/expected-canonical.json`
- `artifacts/dataset-v2-acceptance/e2-t3-roundtrip/expected-physical.json`
- `docs/security/sql/E2_T3_ROUNDTRIP_PREFLIGHT.sql`
- `docs/security/sql/E2_T3_ROUNDTRIP_TRANSACTION.sql`
- `docs/security/sql/E2_T3_ROUNDTRIP_POSTCHECK.sql`
- `scripts/e2-t3-roundtrip-evidence.js`

Before approval, record SHA-256 for every source and verify the repository commit. Never edit SQL during the operation.

## Stop gates

1. Run the exact preflight as one read-only statement.
2. Every equality/minimum check must pass. Capture the aggregate Dataset V2, V1, and snapshot counts; never capture an identity.
3. Replace only the documented aggregate placeholders in the postcheck copy kept as operator-local evidence. Do not commit production counts.
4. Stop if no eligible auth/public user exists, the fixture namespace exists, Dataset V2 is non-empty, ledger is not 37, or any security/schema postcondition differs.
5. Obtain separate human approval. A failed or ambiguous operation must not be retried.

## Controlled operation

Send `E2_T3_ROUNDTRIP_TRANSACTION.sql` once as one intact payload. The eligible user is selected internally and is never returned. The only mutation is the single Dataset V2 `INSERT`; the returned projection excludes the user identifier. The final statement is always `ROLLBACK`, and `COMMIT` is forbidden.

The operator must require: inserted count `1`, read-back count `1`, contract-match count `1`, all parity booleans `true`, and overall `passed=true`. Feed only that allowlisted response to the evidence converter. The converter rejects UUIDs, unknown fields, extra/missing canonical fields, `null`/zero drift, and semantic mismatch.

## Postcheck and completion

After the transaction response, run the exact read-only postcheck once with approved aggregate baselines. Require Dataset V2 and namespaced fixture counts to return to zero/baseline, V1/snapshot parity, unchanged OAuth/token counts, ledger `37`, and unchanged schema state.

E2-T3 can move beyond `Verification` only after the operation evidence and postcheck are reviewed. E2-T4 through E2-T8 are not completed by this runbook.

## Rollback

Rollback is the mandatory normal outcome, not an exception path. Any SQL error leaves the transaction unsuccessful and the intact payload ends with `ROLLBACK`. No cleanup write is authorized. If postcheck finds a fixture, stop and escalate; do not run ad hoc cleanup.

## Observability and privacy

Evidence may contain only run ID, operation status, counts, booleans, canonical field names, redacted expected/actual values, and PASS/FAIL. It must never contain a user/account/connection/entity identifier from production, UUID, credential, raw production row, URI, or authorization material.
