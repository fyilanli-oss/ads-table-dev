-- R4-C read-only production preflight. Expected gate: PASS.
with state as (
  select
    to_regclass('public.workspace_provider_connections') is not null as canonical_table_exists,
    (select count(*) from public.workspace_provider_connections) = 0 as canonical_table_empty,
    not exists (
      select 1 from pg_trigger
      where tgname like 'r4c_freeze_%' and not tgisinternal
    ) as r4c_triggers_absent,
    not exists (
      select 1 from public.oauth_transactions
      where surface is distinct from 'shopify_embedded'
        and provider in ('meta','google_ads','klaviyo','tiktok','pinterest')
    ) as standalone_oauth_transactions_absent,
    (select count(*) from public.platform_connections) as legacy_connection_rows,
    (select count(*) from public.platform_connection_tokens) as legacy_token_rows,
    (select count(*) from public.platform_connections where access_token is not null or refresh_token is not null) as legacy_plaintext_token_rows,
    (select count(*) from public.snapshot_schedules where active and platform in ('meta','google','klaviyo','tiktok','pinterest')) as active_frozen_provider_schedules,
    (select count(*) from public.snapshot_schedules where active and platform = 'klaviyo') as active_klaviyo_schedules,
    (select count(*) from public.snapshot_jobs where status in ('queued','running') and platform in ('meta','google','klaviyo','tiktok','pinterest')) as open_frozen_provider_jobs,
    (select count(*) from public.snapshot_jobs where status in ('queued','running') and platform = 'klaviyo') as open_klaviyo_jobs
)
select
  case when canonical_table_exists
    and canonical_table_empty
    and r4c_triggers_absent
    and standalone_oauth_transactions_absent
    and legacy_plaintext_token_rows = 0
  then 'PASS' else 'BLOCK_R4C_APPLY' end as r4c_preflight_gate,
  *
from state;
