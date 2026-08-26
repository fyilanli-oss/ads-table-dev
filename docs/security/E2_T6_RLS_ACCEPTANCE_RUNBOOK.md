# E2-T6 Dataset V2 RLS Acceptance

## Status and boundary

**E2-T6: `Verification`.** This is a repository-preparation package only. No live SQL, Supabase query, fixture write, RLS matrix, postcheck, Management API operation, deployment, or data/schema/policy/grant/ledger/environment change was performed. Static tests are not live PostgreSQL acceptance. E2-T3, E2-T4, and E2-T5 remain `Verification`; E2-T7 is out of scope and `Not started`.

## Immutable review inputs

Before any separately approved operation, verify the exact approved main/base SHA `c6c2b88ad4c19b2f01b77b63ca84c5fc359db558` and these source checksums:

- `supabase/migrations/20260816101220_create_performance_dataset_rows_v2.sql`: SHA-256 `27cd4bf405b12a7e6928e5c9742f709440da987738fb181bf6dbf5e8da8085f2`
- `artifacts/dataset-v2-acceptance/20260824-metadata-acceptance/rls-grants.json`: SHA-256 `95de1cab3ea95595a2c9eba2e9b0c5ed3bc0a16cc498df273ec8b6d98245ab51`

The migration and committed metadata establish the contract: RLS enabled and not forced; one permissive authenticated own-row SELECT policy; no anon table privilege; authenticated SELECT only with no INSERT/UPDATE/DELETE privilege; service-role backend table access. Metadata evidence confirms configuration, but never substitutes for runtime RLS acceptance.

## Human-controlled operation

A separate human approval and an operator-held database credential are mandatory. Missing credentials, checksum drift, a failed preflight gate, fewer than two distinct users present in both `auth.users` and `public.users`, namespace residue, or inability to preserve deterministic transaction-local role/JWT and temporary-evidence access means **STOP**. Never automatically retry.

1. Run `docs/security/sql/E2_T6_RLS_PREFLIGHT.sql` as one read-only statement. Record the three operator-local Dataset V2, V1, and snapshot baselines without recording identities.
2. Review every stop gate and obtain separate human approval.
3. Submit `docs/security/sql/E2_T6_RLS_TRANSACTION.sql` once as one intact payload. Do not split, edit, or manually combine result sets.
4. Preserve its single final redacted response for offline conversion with `scripts/e2-t6-rls-evidence.js`.
5. Confirm the final statement was the mandatory `ROLLBACK`, then replace the three `-1` placeholders in `docs/security/sql/E2_T6_RLS_POSTCHECK.sql` with the approved operator-local counts and run that one read-only statement.

## Transaction safety model

The auth/public identity intersection is selected once in operator/postgres context before any role switch and stored in a separate transaction-local internal `pg_temp` actor table as exactly `user_a` and `user_b`. The real identifiers are internal actor state, not evidence, and cannot leave the transaction. `service_role` reads only that temp actor store and never queries `auth.users` directly. The harness writes two `e2_t6_rls_v1` fixtures under transaction-local `service_role`, then uses `SET LOCAL ROLE` and transaction-local `set_config(..., true)` for authenticated JWT claim emulation. Every actor transition resets the previous role and both claim settings. No actor state can survive the transaction.

Evidence and internal actor state exist only in separate `pg_temp` tables declared `ON COMMIT DROP`. Transaction-local grants give authenticated, anon, and service-role only the required evidence access; service-role alone receives read access to the internal actor store. All temp objects and their grants are removed by the outer rollback. Their allowlist excludes identity, JWT/claim values, production rows, credentials, URIs, and raw error text/detail/hint/context. Each denied mutation runs in its own nested exception subtransaction; only the safe SQLSTATE and classified outcome are retained. Unexpected successful mutations are rolled back inside their nested block and fail evidence. No `ON CONFLICT`, ad hoc cleanup, persistent DDL, or mutation of V1, snapshots, OAuth, token, ledger, auth, subscription, or connection state is allowed.

The payload contains no `COMMIT`; its unconditional normal end is `ROLLBACK`. Residue or an interrupted/ambiguous operation is a stop condition: do not perform ad hoc cleanup and do not retry. Escalate for investigation and separately approved recovery. The postcheck must prove Dataset V2 returned to its baseline, namespace residue is zero, V1/snapshots/OAuth/token/ledger are unchanged, RLS/policy/grants remain exact, and no persistent evidence object exists.

## Acceptance matrix

The closed 16-case matrix covers User A and User B own/cross-user reads, anon reads, both authenticated users' INSERT/UPDATE/DELETE privilege denials, and service-role fixture inserts/reads. Cross-user SELECT is a zero-row `DENIED_BY_RLS`; anon and authenticated mutation denials are SQLSTATE-class-42 `DENIED_BY_PRIVILEGE`. Service and own reads must each produce one row. The converter rejects missing, extra, duplicate, reordered-contract, unknown-field, identity-bearing, leaking, unexpectedly allowed, residue-bearing, or parity-false evidence.

This package does not execute E2-T6. Completion requires later, separately authorized live preflight, intact rollback-only transaction, postcheck, redacted evidence review, and human acceptance. E2-T6 must not be marked `Done` before that review.

## E2-C1 deviation / decision (corrective preparation)

- İlk repository hazırlığı, E1 kapanış anındaki 7/7 provider bağlantı/token nüfusunu sabit kabul etmişti.
- Canlı read-only E2-T3 preflight, connection/token nüfusunun değişebildiğini kanıtladı; hardcoded kontroller bu nedenle başarısız oldu.
- Actual production sayıları evidence'a veya repository'ye alınmadı; yalnız operator-local baseline olarak tutulmalıdır.
- Missing encrypted ve plaintext güvenlik kontrolleri geçti; corrective sözleşme ayrıca orphan encrypted kontrolünü zorunlu kılar.
- Güvenlik contract'ı fixed population yerine captured connected/encrypted parity ile missing/orphan/plaintext zero olarak düzeltildi.
- Bu değişiklik production data correction değildir. Bu corrective PR kapsamında canlı transaction, INSERT veya postcheck çalıştırılmadı ve production değişmedi.
