-- R4-C read-only production postcheck. Expected gate: PASS.
with state as (
  select
    (select count(*) from pg_trigger where tgname like 'r4c_freeze_%' and not tgisinternal) = 6 as exact_freeze_triggers,
    exists (select 1 from pg_proc where proname = 'r4c_reject_standalone_oauth_transaction' and prosecdef = false) as oauth_guard_security_invoker,
    exists (select 1 from pg_proc where proname = 'r4c_reject_legacy_provider_write' and prosecdef = false) as write_guard_security_invoker,
    (select count(*) from public.snapshot_schedules where active and platform in ('meta','google','klaviyo','tiktok','pinterest')) = 0 as frozen_provider_schedules_stopped,
    (select count(*) from public.snapshot_schedules where active and platform = 'klaviyo') = 0 as klaviyo_schedule_stopped,
    (select count(*) from public.snapshot_jobs where status in ('queued','running') and platform in ('meta','google','klaviyo','tiktok','pinterest')) = 0 as frozen_provider_jobs_closed,
    (select count(*) from public.snapshot_jobs where status in ('queued','running') and platform = 'klaviyo') = 0 as klaviyo_jobs_closed,
    (select count(*) from public.workspace_provider_connections) = 0 as canonical_table_still_empty,
    (select count(*) from public.platform_connections) as legacy_connection_rows,
    (select count(*) from public.platform_connection_tokens) as legacy_token_rows,
    (select count(*) from public.platform_connections where access_token is not null or refresh_token is not null) as legacy_plaintext_token_rows
)
select
  case when exact_freeze_triggers
    and oauth_guard_security_invoker
    and write_guard_security_invoker
    and frozen_provider_schedules_stopped
    and klaviyo_schedule_stopped
    and frozen_provider_jobs_closed
    and klaviyo_jobs_closed
    and canonical_table_still_empty
    and legacy_plaintext_token_rows = 0
  then 'PASS' else 'FAIL' end as r4c_postcheck_gate,
  *
from state;
