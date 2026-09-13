# E2-T8 fresh-project restore readiness runbook

## 1. Status

**Preparation only.** This document and the companion `restore-scope.json` are the first E2-T8 restore-readiness deliverables. They authorize no capture, database connection, target provisioning, restore, migration, ledger bootstrap, teardown, or production operation. Fresh-project restore remains unverified.

## 2. Verified current state

- Repository `main` is fixed to `e3aca83d6a5ba65b601190b37b7547d97ea7a475`; that commit contains the PR #27 merge.
- `docs/security/evidence/DB_LEDGER_T2_BASELINE_MANIFEST.json` records 31 historical ledger entries with `historical_sql_available=false`, an application-owned `public` metadata boundary, and no row data, auth-managed schema, secrets, or PII.
- The reconciled live ledger total is 37. The four corrective/reconciled migrations are present in that ledger.
- The repository contains exactly six migration files. Their relationship to the future baseline is not yet classified.
- The metadata acceptance evidence is normalized evidence, not a schema dump and not proof of a successful fresh-project restore.
- Production is not a restore target.

## 3. Why historical replay is impossible

The SQL bodies for the 31 historical production-ledger entries are absent from the repository. Migration names and current metadata cannot establish the original statement order, intermediate states, role context, or side effects. Consequently, generating SQL from names or asserting that reconstructed statements are historical migrations would invent provenance. E2-T8 preserves `historical_sql_available=false` and does not attempt historical replay.

## 4. Selected strategy

- Never infer the missing 31 migration bodies and never restore into production.
- Never dump production row data. Do not produce a new application baseline in this preparation stage; review this scope contract first.
- In a later stage of this same PR, prepare a sanitized schema-only capture operator, validator, and acceptance artifacts. Do not perform actual capture without credentials and explicit authorization.
- Perform the eventual fresh-project restore only on a unique, empty, disposable target after separate human approval. The target platform continues to manage Supabase-managed schemas.
- Treat the application-owned current state as the source for a versioned immutable squashed baseline with provenance `CURRENT_STATE_BASELINE`. It does not claim to replace the missing SQL as historical evidence or prove that any historical migration ran.
- Compare the eventual result with the repository migration order and approved normalized live evidence.

## 5. Restore scope

The only initially included schema is `public`, and only objects positively classified as application-owned are included. Included classes are application-owned tables, columns and defaults, constraints, indexes, functions, triggers, RLS state, policies, explicit application grants, and required application-owned extensions on an explicit allowlist.

Schema membership alone does not establish ownership. Extension-owned or Supabase-managed objects found in `public` must not be captured automatically: unknown ownership is a stop gate until evidence assigns the object to the application or managed exclusion set.

Table rows, identities, auth users, storage objects, provider tokens, encrypted token envelopes, OAuth transactions, secrets, credentials, connection URIs, PII, ledger rows treated as application data, production-role ownership values, database settings, project secrets, and backup/PITR content are excluded.

## 6. Managed-schema exclusions

The excluded schemas are `auth`, `storage`, `realtime`, `extensions`, `graphql`, `graphql_public`, `net`, `pgsodium`, `pgsodium_masks`, `supabase_functions`, `supabase_migrations`, and `vault`. These are excluded because they are Supabase/platform/extension-managed boundaries rather than application-owned restore input. This evidence-backed list is not expanded by guesswork. Any additional schema discovered later remains unclassified and blocks capture until its ownership is evidenced; managed target bootstrap must remain platform-managed.

## 7. Current-state squashed baseline contract

The future baseline must be a sanitized, schema-only, immutable and checksummed representation of positively classified application-owned current state. Its provenance must be exactly `CURRENT_STATE_BASELINE`. It must contain neither data nor managed-schema DDL and must not claim that the 31 unavailable historical SQL bodies were recovered, replayed, or executed.

The baseline is not created by this task stage. Historical ledger rows must not be inserted automatically after restore. Ledger bootstrap requires a separate explicit design decision, human acceptance, and unambiguous provenance.

## 8. Baseline cutoff and forward-migration classification requirement

`baseline_cutoff_status = pending_capture_classification`.

No cutoff is guessed. During a later implementation stage, an object-by-object manifest must classify each of the six repository migrations as `absorbed_by_baseline`, `applied_after_baseline`, `reconciliation_metadata_only`, or `excluded_with_reason`, and record the exact cutoff represented by the baseline. Only deterministic post-cutoff migrations may run. A migration or object represented both in the baseline and forward replay is a fail-closed blocker; normal `db push` is forbidden before contract validation.

## 9. Disposable target contract

The only permitted future target is a uniquely identified, isolated, empty, disposable Supabase project, or the official full local Supabase stack after exact managed primitives are verified. A plain or generic PostgreSQL-compatible database is forbidden, as are production, shared development, and data-bearing targets. Preflight must prove the `auth` schema, the exact `auth.uid()` signature, and the `anon`, `authenticated`, and `service_role` roles exist; `public` must be empty or contain only explicitly allowlisted managed objects; and operator-supplied safe source/target project-ref fingerprints must differ. Any missing primitive or uncertain identity stops before restore. Provisioning and restore require separate human approval; teardown requires another separate approval.

## 10. Stop gates

Stop without applying statements if any source checksum differs; schema ownership is unknown; the target is production, shared, or non-empty; row data is present; managed-schema DDL is present; a secret, credential, URI, or PII pattern appears; any object is unclassified; baseline and migration ownership overlaps; historical provenance disclosure is missing; an extension is unsupported or not allowlisted; a restore statement is outside the allowlist; target objects drift; acceptance differs; or ledger bootstrap remains ambiguous.

## 11. Planned capture phase

After scope review, prepare—but do not automatically execute—a least-privilege sanitized schema-only capture operator and validator. The operator must pin source identity, checksums, classification inputs, output version, and cutoff status. It must reject data-bearing statements, ownership tied to managed production roles, managed-schema content, unsupported extensions, and sensitive patterns. Actual capture requires credentials and explicit authorization; this task neither requests nor uses them.

## 12. Planned restore phase

After separate approval: verify immutable inputs and checksums; provision and verify the empty disposable target; leave platform-managed bootstrap untouched; apply the sanitized application baseline; apply only classified post-cutoff migrations; then apply explicit grants, RLS, and policies in controlled order. No step may continue past a stop gate.

## 13. Planned acceptance phase

Run read-only acceptance and compare a normalized target contract with the approved source manifest. Require parity for inventory; columns, types, nullability, and defaults; constraints and indexes; function signatures and body fingerprints; triggers; and RLS, policies, and grants. Require zero production rows, secrets, PII, and auth/storage mutations; complete migration classification; reviewed rollback/teardown; an actually executed future fresh-project restore; and human-reviewed evidence.

## 14. Evidence and redaction

Evidence must pin source and target identity without exposing connection material, include immutable checksums and classification results, and record every gate and acceptance result. Outputs must be redacted and scanned for secrets, credentials, connection URIs, token material, private keys, identities, UUIDs, and email addresses before review or commit. Raw dumps and sensitive query results must not enter the repository.

## 15. Rollback and teardown

Before restore, reviewers must approve a target-specific rollback and teardown procedure. Failure stops execution and preserves redacted diagnostic evidence; it never redirects work to production or a shared project. Because the target is disposable, recovery means abandoning it pending review. Destruction is not automatic and occurs only after separate teardown approval and evidence retention review.

## 16. Explicitly forbidden actions

In this stage, do not connect to Supabase; call its Management API; run or install its CLI; run `pg_dump`; request a database URI; capture a production schema dump; export rows; create a target; restore; run migrations; run `db push`; generate historical SQL or baseline SQL; alter existing migrations, the execution plan, packages, or tests; deploy; change environment or secrets; create another task or branch; or reuse an old PR.

## 17. Remaining work before E2-T8 Done

Review and approve this contract; prepare the later same-PR capture operator, validator, object classification manifest, and acceptance artifacts; authorize and perform sanitized capture; set an evidenced cutoff; classify all six migrations object by object; approve a disposable target; execute restore and read-only acceptance; resolve every mismatch; review redacted evidence; and approve teardown. Until all are complete, E2-T8 is not Done.

### Capture operator and fixed arguments

`scripts/e2-t8-schema-capture.js` is plan-only by default. Future execution requires both `--execute` and `--confirm E2_T8_SCHEMA_ONLY_CAPTURE`, plus an environment-only source credential. It exposes no URI, password, arbitrary argument, table filter, or data mode. Its fixed plan selects only `public`, schema-only content, no rows, no ownership restoration, and a fixed argument plan; output remains non-deterministic until tool-version and normalization gates pass. Tool absence returns `CAPTURE_TOOL_UNAVAILABLE`. Import is side-effect free and this PR does not execute capture.

### Raw artifact handling and validator order

Raw capture output is quarantined outside the repository and is never directly committed. First verify the fixed capture plan and source inventory allowlist; then managed-schema exclusions and object classification; then manifest/checksums and statement/sensitive deny scans; then human review; finally target preflight and acceptance. Regex alone never establishes safety. The validator reports artifact-contract PASS/FAIL only, never “safe to restore.”

### Classification, cutoff, target, and evidence gates

All six migrations remain `pending_capture_checksum` and non-replayable until captured-object overlap analysis completes. The cutoff remains pending. Provisioning is outside this PR and limited to the target contract above. `E2_T8_TARGET_PREFLIGHT.sql` must pass before any future restore. There is no restore operator in this PR: it cannot be prepared until an accepted baseline exists. Actual capture and restore each require credentials where applicable and separate human approval. The offline evidence converter accepts exact allowlisted normalized inputs and emits only redacted status, counts, and checksums. E2-T8 remains `Verification` until capture, classification, cutoff, disposable restore, acceptance, evidence review, and teardown review are complete.

### Standalone capture, grant parity, and captured-SQL validation revision

The standalone execute path has real synchronous tool discovery, a fixed `public` schema-only plan, and a strict quarantine sink outside the repository. The source URI is accepted only through `E2_T8_SOURCE_DATABASE_URL`, mapped to child-only `PGDATABASE`, and never placed in arguments or results; the child environment allowlists only required libpq/OS keys. The operator never prints SQL. Quarantine directories/files use restrictive permissions; nonzero tool exit, repository-local output, missing validation inputs, or validator failure cannot create an accepted artifact. This PR does not execute that path.

Application table privileges are captured; `--no-owner` remains fixed and `--no-privileges` is not used. The captured-SQL validator accepts only source-inventory table grants to `anon`, `authenticated`, `service_role`, and to `PUBLIC` only when inventory evidence explicitly authorizes it. Grant option, unknown privilege/grantee, schema/global/database grants, role DDL, and ownership restoration fail closed.

The stateful captured-SQL lexer respects comments, quoted strings/identifiers, and dollar-quoted bodies while splitting statements. Its contract combines fixed arguments, inventory/object classification, managed exclusions, manifest gates, deny checks, SHA-256 normalization, and human review; it reports only artifact-contract PASS/FAIL, never restore safety. Capture determinism remains false until the capture tool version is pinned and normalized output is accepted.

Target preflight reads the operator-local `e2_t8.target_kind` and permits only `disposable_supabase` or `official_full_local_supabase`; no kind is hardcoded. Target inventory has exactly the same four-field identity/class/ownership/SHA-256 shape as source inventory. Separate final gates preserve results even for an empty inventory and compute exact aggregate `count(*)` values using only an identifier-quoted table allowlist generated from the checksum-bound approved inventory; catalog estimates are forbidden. Actual capture, target provisioning, and restore still require separate approval and remain unperformed.

### Final identity, ownership, allowlist, and shell-status decisions

Captured identities must exactly match normalized source keys, including function identity arguments, trigger relation/name, policy schema/relation/name, and quoted identifiers; overload or relation association drift stops validation. Inventory classifies every object as `application_owned`, `managed_extension_owned`, or `excluded_managed`. Only application-owned objects are required in the baseline. Managed extension objects such as an extension-owned public relation are forbidden in captured SQL, omitted from application row counts, and checked separately through managed parity.

The final-gate application table list is generated from the accepted source inventory, passed with its SHA-256, identifier-quoted for exact aggregate `count(*)` queries, compared in both directions with the target application relation set, and checksum-bound again by the evidence converter. A missing, extra, duplicate, managed-extension, or nonzero table fails. The capture CLI returns shell success only for `PLAN_ONLY` and `CAPTURE_QUARANTINED_CONTRACT_PASS`; validation-required, contract-fail, and every operational failure return exit 1.

### Provenance ownership and index identity final gate

Database owner roles, including `postgres` and `supabase_admin`, are not application provenance. Public objects require explicit repository-reviewed application or managed classification; extension dependency takes precedence as `managed_extension_owned`, approved application membership yields `application_owned`, explicit managed membership yields `excluded_managed`, and everything else is `unclassified`. Unclassified ownership is a capture stop gate, while relation child metadata inherits its parent classification. Both regular and unique index statements, including quoted and schema-qualified forms, must match the exact `index:<index_name>` source identity.
