# E1-T6D controlled encrypted write runbook

## Stop gates

This manual workflow performs a **real encrypted write** and is physically separate from the dry-run workflow. Do not dispatch it until all of these gates are reviewed:

1. Configure a required reviewer on the `production-token-backfill` GitHub Environment and retain its main-only deployment branch rule. If only one operator exists, do not enable the setting that prevents self-review, because that would make approval impossible. The workflow must not run without a working manual approval gate.
2. Provision `PROVIDER_TOKEN_ACTIVE_KEY_ID` and `PROVIDER_TOKEN_ENCRYPTION_KEYS` securely in Vercel production first. Their values must exactly match the GitHub Environment keyring; never put their real values in evidence or logs.
3. Keep `PROVIDER_TOKEN_ENCRYPTION_ENABLED` off and do not change `PROVIDER_TOKEN_LEGACY_READ_ENABLED`.
4. Confirm that accepted dry-run run `32245732566` remains the governing evidence.

## One approved dispatch

From `main`, an administrator may approve one dispatch of **Provider token production encrypted write** with only:

- `expected_total`: `9`
- `confirmation`: `E1-T6D-PRODUCTION-WRITE`
- `dry_run_evidence_id`: `32245732566`

Do not add a cursor, batch size, or mode. The protected environment approval is the authoritative human gate; the confirmation string is only an accidental-execution guard.

## Review safe output

A success is one JSON object with `ok=true`, `mode=encrypted-backfill-write`, `contractVersion=v1`, and `expectedTotal=9`. Review each allowlisted counter group:

- `preflight`: 9 scanned, zero failed/empty/written, and eligible plus already-encrypted equals 9.
- `write`: zero failed/empty, and written plus already-encrypted equals 9.
- `verification`: 9 scanned, 0 eligible/written/rotation candidates/empty/failed, and 9 already encrypted. Verification exercises encrypted resolution/decryption, not merely a row count.

If output reports `BACKFILL_WRITE_PARTIAL_FAILURE` or any other failure, **do not retry**. Preserve plaintext and encrypted rows, keep flags unchanged, share only the redacted JSON with the administrator, and obtain a new GO decision for an idempotent retry using the same verified keyring. Never delete rows as rollback.

Even after success, do not enable encryption, remove plaintext tokens, rotate/delete the keyring, or change legacy reads. Submit the redacted workflow result for administrator review. Never share raw logs containing secrets, tokens, user identifiers, URLs, database errors, or stack traces.

## Scope status

Preparing this artefact does not dispatch the workflow, contact Vercel or Supabase, write tokens, activate a flag, or retire plaintext. E1-T6D remains: `dry-run accepted / write artefact ready / Vercel key provisioning and controlled write pending`.
