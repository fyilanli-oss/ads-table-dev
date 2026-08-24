# E2-T5 Dataset V2 Rejection Matrix

## Status and immutable sources

This is a **preparation-only** package. E2-T5 is `Verification`, not `Done`; no live SQL or PostgreSQL acceptance has run. The exact preparation base is main `9011e237424aea351a6370632c03e091a07ce72a`. Source migration SHA-256 values are:

- `20260816101220_create_performance_dataset_rows_v2.sql`: `27cd4bf405b12a7e6928e5c9742f709440da987738fb181bf6dbf5e8da8085f2`
- `20260816101540_fix_v2_klaviyo_channel_constraint.sql`: `6402acdc9a6e426c138d797b7993462ee4bd0c5a0ca34fced7a73586dfb5d5f3`

The namespace is `e2_t5_rejection_v1`. The matrix contains exactly 35 statically derived cases: 32 CHECK rejections (`23514`) and three NOT NULL rejections (`23502`). CHECK `expected_constraints` values are case-specific, sorted, duplicate-free **closed allowlists** derived from migration expressions. `INVALID_SUPPORT_ENUM` makes both the metric-support keys enum and metric-value/support expressions false because `impression` is `invalid` while `impressions` is non-null. PostgreSQL constraint evaluation order is not a contract: any actual CHECK constraint must be non-empty and belong to that case's closed set. Operators must never learn or widen an allowlist from a live result, use wildcards, or substitute the full constraint inventory. NOT NULL cases require the exact `entity_id`, `platform_account_id`, or `business_date` column.

## Authorization and stop gates

The read-only preflight is a single `WITH ... SELECT` returning aggregate gates only. It verifies the 37-entry ledger baseline, Dataset V2 table and RLS state, all 19 named and validated CHECK constraints, the three required NOT NULL columns, the corrective paid-Klaviyo non-null-channel definition, zero namespace residue, at least one eligible auth/public user, Dataset V2/V1/dashboard snapshot count captures, and OAuth/connected/encrypted/missing/plaintext postconditions. Any failed gate is a STOP. Missing operator credential is also a STOP; credentials are never stored here.

Repository review and a **separate human approval** are mandatory before any future live preflight. E2-T3 and E2-T4 remain `Verification`. E2-T6 and E2-T7 are out of scope.

## Controlled transaction boundary

The transaction file is one intact payload: `BEGIN`, minimum relation locks, one `pg_temp` evidence table with `ON COMMIT DROP`, one static PL/pgSQL block, a single final redacted response, and mandatory final `ROLLBACK`. It creates no persistent object and changes no schema, ledger, RLS, policy, grant, or privilege.

Each of the 35 cases has a distinct namespaced entity key and its own nested exception subtransaction containing a static `INSERT`. There is no dynamic SQL, `EXECUTE`, `format()`, SQL concatenation, retry, `ON CONFLICT`, `UPDATE`, `DELETE`, `TRUNCATE`, or ad hoc cleanup. Expected rejection rolls back the nested subtransaction. `dataset_unchanged` means exact equality with the captured Dataset V2 baseline; an unexpectedly accepted insert makes it false, is recorded as failed, and remains only until the outer mandatory rollback. `COMMIT` is forbidden.

Only `RETURNED_SQLSTATE`, `CONSTRAINT_NAME`, and `COLUMN_NAME` are collected with `GET STACKED DIAGNOSTICS`. SQL error messages, detail, hint, context, raw SQL, production values, identities, UUIDs, tokens, credentials, and URIs are prohibited. If no eligible user exists or any namespace residue exists, no invalid insert runs and the final result fails closed.

## Evidence and operator procedure

1. Review the exact source checksums, matrix closed sets, SQL, converter, and static tests.
2. Obtain separate human approval and operator credential; otherwise STOP.
3. Run the preflight once. On any failed gate, STOP with no retry.
4. Capture the three count baselines only in the operator-local record; never commit production counts.
5. Submit the transaction as one intact payload. Do not split it or manually merge result sets.
6. Preserve only its single final response and validate it with `scripts/e2-t5-rejection-evidence.js`.
7. Confirm the final statement executed was `ROLLBACK`; a missing result or uncertain rollback is a STOP.
8. Replace only the three `-1` scalar placeholders in an operator-local postcheck copy and run that single read-only query.
9. Any residue or parity failure is a STOP; ad hoc cleanup and retry are not authorized.

The response proves exact case count/state/constraint/column matching, zero unexpected acceptance/residue, and Dataset V2, V1, dashboard snapshots, OAuth, encrypted-token, plaintext-token, and ledger no-change. It contains no production identity. Static repository tests validate structure and fail-closed conversion; they are **not live PostgreSQL acceptance**.

No live Supabase query, invalid insert, transaction, postcheck, application-data change, schema change, ledger change, privilege change, or deployment has been performed by this preparation task.
