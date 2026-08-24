# DB-LEDGER-T2 controlled reconciliation runbook

## Status and scope

This package is repository preparation only. Merging it does not contact Supabase, run a migration, reconcile the ledger, or change OAuth privileges. A separate, approved production operation is required.

The operation is limited to these exact versions and names:

| Version | Name |
|---|---|
| `20260818090000` | `create_oauth_transactions` |
| `20260818120000` | `create_platform_connection_tokens` |
| `20260819120000` | `harden_platform_connection_tokens_service_role_grants` |
| `20260824120000` | `harden_oauth_transactions_service_role_grants` |

It may change only:

1. `public.oauth_transactions` table privileges and enabled RLS state; and
2. the four exact rows in `supabase_migrations.schema_migrations`.

It must not recreate application objects, rerun historical migrations, invoke functions, mutate application data, touch token-table privileges/schema/data, or modify the 31 historical ledger rows.

## Accepted repository checksums

Verify from the approved commit before the production GO decision:

| File | SHA-256 |
|---|---|
| `20260818090000_create_oauth_transactions.sql` | `728f62d4af52eccd0fee195b0a3dedf68889e52b7434f2676bfb90a5b65ef00b` |
| `20260818120000_create_platform_connection_tokens.sql` | `afada06dec7b95aca69e3d01dc432554c2599dcc1b357dbb300b3695ae8cb156` |
| `20260819120000_harden_platform_connection_tokens_service_role_grants.sql` | `7cd9c0dcf6b4ec8ece5c12656b974a9c02587158771404ba6e7e3735e9ca9c76` |
| `20260824120000_harden_oauth_transactions_service_role_grants.sql` | `f73116b653c0ce77f5751e74c36ece39ac4e94aa5f745b5f80772cb6d34de32e` |

Do not approve if any checksum differs from the reviewed commit.

Accepted normalized evidence uses `db-ledger-norm-v1`. Function checksums:

- consume: `21ddf7f83cbdaeab420877381a768952605a2373516163029170a8dbb45b4e56`
- cleanup: `789f7053c8461cc09f0e760cb7bbcc1e639eb202df36f9751c460a485da37ac9`

Token envelope semantic checksums:

- access: `70e9e634688afe4eaeab1737e25a55ff8f911783616495b6887bef34d0485d7b`
- refresh: `3ee72ae856fa94a6d4ec319339a8d6e64f0487cbb2b7b7573325b2eb5d550050`

## Stop gates

1. Run `DB_LEDGER_T2_PREFLIGHT.sql` as a separate read-only operation.
2. Every row must have `passed=true`.
3. Capture total ledger count, OAuth row count, application relation aggregate counts, and the normalized checksum evidence without row data.
4. OAuth row count must be zero. A nonzero count requires a new explicit approval and a revised exact expected count; do not edit the production script ad hoc.
5. Connected/encrypted counts must be 7/7; missing-encrypted and plaintext counts must be zero.
6. Verify all four repository file checksums and the accepted normalized object checksums.
7. Obtain a human GO for one execution of `DB_LEDGER_T2_RECONCILIATION.sql`.
8. Stop if any target version exists, including with a different name.

## Controlled operation

Execute the reconciliation file exactly once in a SQL surface that preserves its single transaction. Never pass it to the normal migration runner or `db push`.

The transaction locks the ledger and three inspected relations, repeats critical preconditions, applies OAuth least privilege, inserts exactly four ledger rows, checks `ROW_COUNT=4`, validates postconditions, and commits. Any exception rolls the whole transaction back.

Do not retry after an unknown failure. First rerun only the read-only preflight and acceptance, preserve redacted PASS/FAIL/count evidence, and obtain a new decision.

## Acceptance

Run `DB_LEDGER_T2_ACCEPTANCE.sql` separately. Every result must pass. Additionally compare the captured before/after evidence:

- total ledger count increases by exactly four;
- the 31 historical version/name pairs are unchanged;
- OAuth function and schema/constraint/index normalized checksums are unchanged;
- token table/check/privilege normalized checksums are unchanged;
- OAuth row count and all application-table aggregate row counts are unchanged;
- connected/encrypted/missing/plaintext counts are unchanged;
- OAuth service-role privileges are exactly SELECT, INSERT, DELETE;
- PUBLIC, anon, and authenticated have no OAuth table privilege.

Evidence may contain only version/name, checksum, boolean, aggregate count, privilege name, and PASS/FAIL code.

## Rollback

### Transaction-time rollback

Any failed assertion raises an exception. PostgreSQL rolls back the grants, RLS statement, and four ledger inserts together. Do not issue a partial commit.

### Controlled post-operation rollback

Post-operation rollback requires a new approved transaction. It may remove only the four exact ledger rows and only after all four version/name pairs match. It must assert exactly four affected rows and leave every application object unchanged.

The rollback must **not** restore UPDATE, TRUNCATE, REFERENCES, or TRIGGER to the OAuth service role. Keep the safe least-privilege SELECT/INSERT/DELETE state. Do not rerun or reverse any application migration.

A rollback operator must use the same target table lock, exact values allowlist, before/after ledger counts, object checksum parity checks, and fail-closed exception pattern as the forward operator. It must not reference any of the 31 historical versions in a mutation predicate.

## Forbidden actions

- Recreating live tables, functions, constraints, indexes, policies, or triggers
- Running the three historical migrations
- Running `db push` or the normal migration runner for reconciliation
- Modifying application rows, token rows, OAuth rows, or historical ledger rows
- Expanding OAuth privileges during rollback
- Including credentials, identities, token material, envelopes, or raw rows in evidence
