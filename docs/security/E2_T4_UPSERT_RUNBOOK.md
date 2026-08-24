# E2-T4 Same-Key PostgreSQL Upsert Acceptance Runbook

## Status and scope

This is a **preparation-only** repository package. This PR executes no live SQL and does not replace live acceptance. E2-T4 remains `Verification`. E2-T3 live acceptance remains incomplete; E2-T5, E2-T6, and E2-T7 are outside this package.

The proposed operation proves that two writes sharing the exact migration-defined canonical key produce one row, with the second payload updating mutable fields through PostgreSQL `INSERT ... ON CONFLICT ... DO UPDATE`. It never uses client-side select-then-update emulation.

## Immutable sources

Before any operation, verify the exact GitHub main SHA and record SHA-256 for the three fixtures, preflight, transaction, postcheck, evidence converter, and this runbook. Do not edit an approved transaction payload. Absence of Management API credential is an immediate `STOP`.

## Preflight and stop gates

Run `E2_T4_UPSERT_PREFLIGHT.sql` once as one read-only payload. Require every equality/minimum check to pass, including ledger, table/RLS, absent namespace, eligible user, OAuth/token/plaintext postconditions, and the valid/ready canonical unique index with exact ordered columns.

Capture Dataset V2, V1, and `dashboard_snapshots` aggregate counts. Put them only into the documented `-1` placeholders in an operator-local postcheck copy; never commit production counts. Obtain separate human approval after preflight.

## Controlled transaction

Send `E2_T4_UPSERT_TRANSACTION.sql` once, intact. Automatic retry is forbidden. It uses minimum explicit table locking, selects an eligible user internally, returns no user identity, writes initial fixture A, then writes fixture B through the exact canonical conflict target.

`COMMIT` is forbidden. The unconditional final statement is `ROLLBACK` and must never be removed. Require initial/upsert counts `1`, final fixture rows `1`, duplicate groups/excess rows `0`, updated match `1`, identity/hierarchy and null/zero booleans true, and V1/snapshot/OAuth/token parity.

## Postcheck and residue handling

After an unambiguous transaction response, run the operator-local read-only postcheck once. Require Dataset V2, V1, snapshots, ledger, security postconditions, and schema/RLS/index state to match preflight, with fixture and duplicate counts zero.

If any fixture residue appears, stop and escalate. This runbook authorizes no ad hoc cleanup. A failed or ambiguous request must not be retried.

## Privacy and impact boundary

Only namespaced fixture values, counts, booleans, safe expected/actual fixture metrics, and PASS/FAIL may be retained. Credentials, authorization material, connection strings, UUIDs, emails, production identities, and raw production rows are forbidden.

The prepared operation cannot mutate V1, snapshots, OAuth, token, ledger, schema, RLS, policy, or privileges. Repository preparation does not complete E2-T4 live acceptance.
