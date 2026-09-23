-- R4-C freezes standalone provider authority before R5 consolidation (live version 20260923093756).
-- Historical connections, encrypted tokens, snapshots and analytics remain intact.

update public.snapshot_schedules
set
  active = false,
  stopped_at = now(),
  stop_reason = 'r4c_legacy_provider_runtime_frozen',
  lifecycle_version = 'r4c-workspace-authority-freeze-v1',
  metadata = coalesce(metadata, '{}'::jsonb) || jsonb_build_object(
    'r4cFrozenAt', now(),
    'r4cPreviousNextRunAt', next_run_at,
    'r4cFreezeReason', 'legacy_provider_runtime_frozen'
  ),
  updated_at = now()
where active = true
  and platform in ('meta', 'google', 'klaviyo', 'tiktok', 'pinterest');

update public.snapshot_jobs
set
  status = 'failed',
  error_message = 'Stopped by R4-C legacy provider runtime freeze',
  finished_at = now(),
  lifecycle_version = 'r4c-workspace-authority-freeze-v1',
  metadata = coalesce(metadata, '{}'::jsonb) || jsonb_build_object(
    'r4cFrozenAt', now(),
    'r4cPreviousStatus', status,
    'r4cFreezeReason', 'legacy_provider_runtime_frozen'
  ),
  updated_at = now()
where status in ('queued', 'running')
  and platform in ('meta', 'google', 'klaviyo', 'tiktok', 'pinterest');

create function public.r4c_reject_standalone_oauth_transaction()
returns trigger
language plpgsql
security invoker
set search_path = pg_catalog, public
as $$
begin
  if new.surface is distinct from 'shopify_embedded'
    and new.provider in ('meta', 'google_ads', 'klaviyo', 'tiktok', 'pinterest') then
    raise exception using
      errcode = '55000',
      message = 'R4C_STANDALONE_OAUTH_FROZEN';
  end if;
  return new;
end;
$$;

create function public.r4c_reject_legacy_provider_write()
returns trigger
language plpgsql
security invoker
set search_path = pg_catalog, public
as $$
declare
  target_platform text;
begin
  if tg_op = 'DELETE' then
    target_platform := lower(old.platform);
  else
    target_platform := lower(new.platform);
  end if;

  if target_platform in ('meta', 'google', 'klaviyo', 'tiktok', 'pinterest') then
    raise exception using
      errcode = '55000',
      message = 'R4C_LEGACY_PROVIDER_WRITE_FROZEN';
  end if;

  if tg_op = 'DELETE' then return old; end if;
  return new;
end;
$$;

revoke all on function public.r4c_reject_standalone_oauth_transaction() from public, anon, authenticated, service_role;
revoke all on function public.r4c_reject_legacy_provider_write() from public, anon, authenticated, service_role;

create trigger r4c_freeze_standalone_oauth_insert
before insert on public.oauth_transactions
for each row execute function public.r4c_reject_standalone_oauth_transaction();

create trigger r4c_freeze_platform_connections_write
before insert or update or delete on public.platform_connections
for each row execute function public.r4c_reject_legacy_provider_write();

create trigger r4c_freeze_platform_connection_tokens_write
before insert or update or delete on public.platform_connection_tokens
for each row execute function public.r4c_reject_legacy_provider_write();

create trigger r4c_freeze_platform_account_ownerships_write
before insert or update or delete on public.platform_account_ownerships
for each row execute function public.r4c_reject_legacy_provider_write();

create trigger r4c_freeze_snapshot_schedules_write
before insert or update or delete on public.snapshot_schedules
for each row execute function public.r4c_reject_legacy_provider_write();

create trigger r4c_freeze_snapshot_jobs_write
before insert or update or delete on public.snapshot_jobs
for each row execute function public.r4c_reject_legacy_provider_write();

comment on function public.r4c_reject_standalone_oauth_transaction() is
  'R4-C guard: blocks standalone provider OAuth while preserving Shopify embedded workspace OAuth.';
comment on function public.r4c_reject_legacy_provider_write() is
  'R4-C guard: freezes legacy provider connection, token, ownership, schedule and job writes before R5.';
