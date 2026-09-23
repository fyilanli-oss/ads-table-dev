-- R4-C rollback requires a separate explicit approval.
-- Failed jobs are deliberately not re-queued; a new operator decision is required.
drop trigger if exists r4c_freeze_standalone_oauth_insert on public.oauth_transactions;
drop trigger if exists r4c_freeze_platform_connections_write on public.platform_connections;
drop trigger if exists r4c_freeze_platform_connection_tokens_write on public.platform_connection_tokens;
drop trigger if exists r4c_freeze_platform_account_ownerships_write on public.platform_account_ownerships;
drop trigger if exists r4c_freeze_snapshot_schedules_write on public.snapshot_schedules;
drop trigger if exists r4c_freeze_snapshot_jobs_write on public.snapshot_jobs;

drop function if exists public.r4c_reject_standalone_oauth_transaction();
drop function if exists public.r4c_reject_legacy_provider_write();

update public.snapshot_schedules
set
  active = true,
  stopped_at = null,
  stop_reason = null,
  next_run_at = nullif(metadata ->> 'r4cPreviousNextRunAt', '')::timestamptz,
  metadata = metadata - 'r4cFrozenAt' - 'r4cPreviousNextRunAt' - 'r4cFreezeReason',
  updated_at = now()
where stop_reason = 'r4c_legacy_provider_runtime_frozen'
  and metadata ->> 'r4cFreezeReason' = 'legacy_provider_runtime_frozen';
