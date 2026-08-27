-- E2-T5 read-only preflight. Aggregate-only stop gates; no identity or row data is returned.
with expected_checks(name) as (
  select unnest(array[
    'performance_dataset_rows_v2_platform_chk','performance_dataset_rows_v2_traffic_type_chk',
    'performance_dataset_rows_v2_source_system_chk','performance_dataset_rows_v2_channel_chk',
    'performance_dataset_rows_v2_campaign_type_chk','performance_dataset_rows_v2_root_type_chk',
    'performance_dataset_rows_v2_parent_type_chk','performance_dataset_rows_v2_entity_type_chk',
    'performance_dataset_rows_v2_source_confidence_chk','performance_dataset_rows_v2_source_currency_chk',
    'performance_dataset_rows_v2_target_currency_chk','performance_dataset_rows_v2_fx_rate_chk',
    'performance_dataset_rows_v2_metric_support_object_chk','performance_dataset_rows_v2_raw_object_chk',
    'performance_dataset_rows_v2_synthetic_chk','performance_dataset_rows_v2_source_semantics_chk',
    'performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_metric_support_keys_chk',
    'performance_dataset_rows_v2_metric_value_support_chk'
  ]::text[])
), required_columns(name) as (
  values ('entity_id'),('platform_account_id'),('business_date')
), checks(check_code,actual_count,expected_count,comparison) as (
  select 'LEDGER_TOTAL',count(*),37,'eq' from supabase_migrations.schema_migrations
  union all select 'DATASET_TABLE',count(*),1,'eq' from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='performance_dataset_rows_v2' and c.relkind='r'
  union all select 'DATASET_ROWS',count(*),count(*),'capture' from public.performance_dataset_rows_v2
  union all select 'V1_ROWS',count(*),count(*),'capture' from public.performance_dataset_rows
  union all select 'SNAPSHOT_ROWS',count(*),count(*),'capture' from public.dashboard_snapshots
  union all select 'E2_T5_RESIDUE',count(*),0,'eq' from public.performance_dataset_rows_v2 where entity_key like 'e2\_t5\_rejection\_v2:%' escape '\'
  union all select 'ELIGIBLE_USERS',count(*),1,'gte' from public.users u where exists(select 1 from auth.users a where a.id=u.id)
  union all select 'RLS_STATE',count(*),1,'eq' from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='performance_dataset_rows_v2' and c.relrowsecurity and not c.relforcerowsecurity
  union all select 'NAMED_VALIDATED_CHECKS',count(*),19,'eq' from pg_catalog.pg_constraint c join pg_catalog.pg_class t on t.oid=c.conrelid join pg_catalog.pg_namespace n on n.oid=t.relnamespace join expected_checks e on e.name=c.conname where n.nspname='public' and t.relname='performance_dataset_rows_v2' and c.contype='c' and c.convalidated
  union all select 'REQUIRED_NOT_NULL_COLUMNS',count(*),3,'eq' from pg_catalog.pg_attribute a join pg_catalog.pg_class t on t.oid=a.attrelid join pg_catalog.pg_namespace n on n.oid=t.relnamespace join required_columns r on r.name=a.attname where n.nspname='public' and t.relname='performance_dataset_rows_v2' and a.attnotnull and not a.attisdropped
  union all select 'KLAVIYO_CORRECTIVE_SEMANTICS',count(*),1,'eq' from pg_catalog.pg_constraint c join pg_catalog.pg_class t on t.oid=c.conrelid join pg_catalog.pg_namespace n on n.oid=t.relnamespace where n.nspname='public' and t.relname='performance_dataset_rows_v2' and c.conname='performance_dataset_rows_v2_source_semantics_chk' and c.convalidated and pg_get_constraintdef(c.oid) ilike '%platform = ''klaviyo''%' and pg_get_constraintdef(c.oid) ilike '%channel IS NOT NULL%'
  union all select 'OAUTH_ROWS',count(*),0,'eq' from public.oauth_transactions
  union all select 'CONNECTED_CONNECTIONS',count(*),count(*),'capture' from public.platform_connections where connected=true
  union all select 'ENCRYPTED_TOKEN_ROWS',count(*),count(*),'capture' from public.platform_connection_tokens
  union all select 'MISSING_ENCRYPTED',count(*),0,'eq' from public.platform_connections pc where pc.connected=true and not exists(select 1 from public.platform_connection_tokens pt where pt.user_id=pc.user_id and pt.platform=pc.platform)
  union all select 'ORPHAN_ENCRYPTED',count(*),0,'eq' from public.platform_connection_tokens pt where not exists(select 1 from public.platform_connections pc where pc.user_id=pt.user_id and pc.platform=pt.platform and pc.connected=true)
  union all select 'CONNECTION_TOKEN_PARITY',case when (select count(*) from public.platform_connections where connected=true)=(select count(*) from public.platform_connection_tokens) then 1 else 0 end,1,'eq'
  union all select 'PLAINTEXT_TOKENS',count(*),0,'eq' from public.platform_connections where access_token is not null or refresh_token is not null
)
select check_code,actual_count,expected_count,
  case comparison when 'eq' then actual_count=expected_count when 'gte' then actual_count>=expected_count else true end passed
from checks order by check_code;
