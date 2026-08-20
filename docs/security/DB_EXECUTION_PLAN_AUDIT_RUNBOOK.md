# DB–Execution Plan production audit runbook

This runbook provisions and operates a **manual, metadata-only, read-only** audit. It never applies a correction, migration, DDL, DML, RPC, deployment, or data cleanup.

## One-time provisioning after merge

1. In repository **Settings → Environments**, create `production-db-audit`.
2. Limit deployment branches to the protected `main` branch and add the required production reviewer(s). Do not allow self-review where the organization supports that control.
3. Add exactly one environment secret: `SUPABASE_AUDIT_DATABASE_URL`. Its URI must authenticate as the dedicated `codex_auditor` role. Never use an owner, `postgres`, service-role, or application credential.
4. Do not paste the secret into a task, issue, pull request, chat, log, repository variable, or workflow input.

The environment and secret do not exist as part of this PR; an administrator creates them only after merge.

## Manual operation

1. Open **Actions → Supabase production schema audit → Run workflow** and select `main`.
2. The secret-free `validation` job rejects every non-`main` ref, installs locked dependencies without lifecycle scripts, and runs the audit contracts and full test suite.
3. After validation and environment reviewer approval, `production-audit` verifies the role, transaction read-only setting, database connection, Dataset V2 read-only boundary, sensitive token/auth/OAuth denials, and migration-ledger access. Any mismatch stops before introspection.
4. The job reads only catalog metadata and ledger version/name evidence, converts raw temporary output through an allowlist, validates redaction, writes a short job summary, and uploads the two redacted reports for 7 days.

The summary reports PASS/FAIL, acceptance state, relation and ledger counts, classification counts, artifact name, run ID, and commit only. Download `db-execution-plan-drift-report.json` and `.md` from the run artifact and share only those files with the designated management reviewers. Never share the temporary raw metadata file.

## Failure and rollback

An audit failure is evidence, not authorization to change a grant or run a migration. Do not prepare corrective migrations, grants, foreign keys, or destructive work until the drift report is reviewed. The workflow has no production write authority and performs no automatic remediation.

To suspend the facility, disable the workflow and remove the environment secret. A database administrator may rotate/reset, revoke login from, or drop `codex_auditor` using the separately controlled database administration process; never place those privileged commands or credentials in this workflow. Removing the workflow/secret or auditor role does not affect live schema or rows.
