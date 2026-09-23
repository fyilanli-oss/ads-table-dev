-- R2 read-only production preflight. This script performs no mutation.
-- Review every result before applying 20260920090105_create_workspace_currency_foundation.sql.

select 'shopify_installations_total' as check_name, count(*)::bigint as observed
from public.shopify_installations
union all
select 'shopify_workspace_nulls', count(*)::bigint
from public.shopify_installations where workspace_id is null
union all
select 'shopify_distinct_workspaces', count(distinct workspace_id)::bigint
from public.shopify_installations
union all
select 'dataset_v2_rows', count(*)::bigint
from public.performance_dataset_rows_v2
union all
select 'backfill_checkpoints_exists', case when to_regclass('public.backfill_checkpoints') is null then 0 else 1 end
union all
select 'embedded_provider_connections', count(*)::bigint
from public.shopify_workspace_provider_connections
union all
select 'embedded_klaviyo_connections', count(*)::bigint
from public.shopify_workspace_provider_connections where provider = 'klaviyo'
union all
select 'legacy_klaviyo_connections', count(*)::bigint
from public.platform_connections where platform = 'klaviyo'
union all
select 'backfill_base_migration_recorded', count(*)::bigint
from supabase_migrations.schema_migrations where version = '20260908074500'
union all
select 'embedded_oauth_migration_recorded', count(*)::bigint
from supabase_migrations.schema_migrations where version = '20260911130000'
union all
select 'embedded_provider_migration_recorded', count(*)::bigint
from supabase_migrations.schema_migrations where version = '20260911150000'
union all
select 'repository_klaviyo_migration_recorded', count(*)::bigint
from supabase_migrations.schema_migrations where version = '20260919194248';

select workspace_id, count(*)::bigint as installation_count
from public.shopify_installations
group by workspace_id
having count(*) > 1;

select workspace_id, provider, count(*)::bigint as connection_count
from public.shopify_workspace_provider_connections
group by workspace_id, provider
having count(*) > 1;

with ledger as (
  select
    to_regclass('public.backfill_checkpoints') is not null as backfill_exists,
    exists (
      select 1 from information_schema.columns
      where table_schema = 'public' and table_name = 'oauth_transactions' and column_name = 'workspace_id'
    ) as embedded_oauth_schema_exists,
    to_regclass('public.shopify_workspace_provider_connections') is not null as embedded_provider_schema_exists,
    exists (select 1 from supabase_migrations.schema_migrations where version = '20260908074500') as backfill_migration_recorded,
    exists (select 1 from supabase_migrations.schema_migrations where version = '20260911130000') as embedded_oauth_migration_recorded,
    exists (select 1 from supabase_migrations.schema_migrations where version = '20260911150000') as embedded_provider_migration_recorded,
    exists (select 1 from supabase_migrations.schema_migrations where version = '20260919194248') as repository_klaviyo_migration_recorded
)
select case
  when to_regclass('public.workspaces') is not null then 'BLOCK_ALREADY_EXISTS'
  when to_regclass('public.workspace_settings') is not null then 'BLOCK_ALREADY_EXISTS'
  when exists (select 1 from public.shopify_installations where workspace_id is null) then 'BLOCK_NULL_WORKSPACE'
  when ledger.embedded_oauth_schema_exists <> ledger.embedded_oauth_migration_recorded
    or ledger.embedded_provider_schema_exists <> ledger.embedded_provider_migration_recorded
    or not ledger.repository_klaviyo_migration_recorded
    then 'BLOCK_EMBEDDED_MIGRATION_LEDGER_DRIFT'
  else 'PASS'
end as r2_preflight_gate,
ledger.*
from ledger;
