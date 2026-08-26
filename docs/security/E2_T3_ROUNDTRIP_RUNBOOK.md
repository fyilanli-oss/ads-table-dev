# E2-T3 Canonical Round-Trip Runbook — ordered v2

**Status: `Verification`.** This corrective repository package performs no live SQL or Management API call. Static tests do not replace live PostgreSQL acceptance.

## Safely recorded v1 outcome

The `e2_t3_static_v1` operation was executed live exactly once. Management API transport returned HTTP 201; insert and canonical contract assertions passed, but read-back and overall assertions failed because a target-table scan in the same statement cannot observe a sibling data-modifying CTE under PostgreSQL statement-snapshot semantics. The mandatory final `ROLLBACK` remained intact. The v1 transaction was not retried.

The v1 postcheck returned HTTP 400 because it projected an aggregate with a cross-joined expected value without a valid aggregate grouping. A separately human-approved, read-only recovery query returned HTTP 201 and 13/13 checks passed. It proved zero fixture residue, Dataset V2 zero, and unchanged V1, snapshot, OAuth, provider security, ledger, and schema state. Actual production counts and identities were not shared.

## v2 corrective design

Version 2 uses namespace `e2_t3_static_v2`, operation code `E2_T3_TRANSACTION_V2`, evidence version `e2-t3-roundtrip-v2`, and adapter version `e2-t3-meta-v2`. It is a new operation, not a retry of v1.

Send `docs/security/sql/E2_T3_ROUNDTRIP_TRANSACTION.sql` only as one intact payload. Its ordered top-level statements are:

1. `BEGIN`;
2. deterministic locks, including `SHARE ROW EXCLUSIVE` on Dataset V2 and `SHARE` on parity relations;
3. `pg_temp.e2_t3_v2_baseline` created `ON COMMIT DROP` with aggregate-only gates;
4. one top-level Dataset V2 `INSERT` gated by that baseline;
5. a separate top-level read-back/evidence `SELECT` that scans the v2 fixture from Dataset V2;
6. unconditional final `ROLLBACK`.

Never split statements, detach rollback, add commit, retry an ambiguous outcome, or expose the eligible user. The redacted evidence row excludes runtime user and timestamp fields.

## Preflight, evidence, and postcheck

A new v2 read-only preflight and separate human approval are mandatory. Validate the exact approved main SHA and artifact checksums before any request. Capture the four operator-local baselines, then make exactly four operator-local replacements, in this order: V1, snapshot, connected, encrypted. The committed Dataset V2 zero is not a placeholder and must not be changed. Actual provider counts remain operator-local, are never shared, and are never committed.

The converter accepts only the exact v2 result allowlist, one redacted fixture row, passing write/read/contract assertions, and all parity booleans. The postcheck is one read-only scalar-query `WITH ... SELECT` with 13 exact checks and no aggregate cross-join projection.

E2-T3 remains `Verification` until the newly approved v2 preflight, intact transaction, converter, rollback postcheck, and human review all pass. E2-T4 through E2-T8 are not started or completed by this corrective package.
