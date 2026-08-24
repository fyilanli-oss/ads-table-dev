-- E2-T4 read-only postcheck. Replace only -1 baseline placeholders in an operator-local copy.
with expected(dataset_rows,v1_rows,snapshot_rows) as (
  values ((-1)::bigint,(-1)::bigint,(-1)::bigint)
), constants as (
  select 'meta:e2_t4_same_key_v1_account:paid:none:campaign:e2_t4_same_key_v1_campaign:ad:e2_t4_same_key_v1_ad'::text entity_key
), fixture_groups as (
  select user_id,platform,platform_account_id,business_date,traffic_type,entity_key,count(*) row_count
  from public.performance_dataset_rows_v2 d cross join constants k where d.entity_key=k.entity_key
  group by user_id,platform,platform_account_id,business_date,traffic_type,entity_key
), checks(check_code,actual_count,expected_count) as (
  select 'DATASET_ROWS',(select count(*) from public.performance_dataset_rows_v2),(select dataset_rows from expected)
  union all select 'FIXTURE_ROWS',coalesce(sum(row_count),0),0 from fixture_groups
  union all select 'DUPLICATE_GROUPS',count(*) filter(where row_count>1),0 from fixture_groups
  union all select 'V1_ROWS',(select count(*) from public.performance_dataset_rows),(select v1_rows from expected)
  union all select 'SNAPSHOT_ROWS',(select count(*) from public.dashboard_snapshots),(select snapshot_rows from expected)
  union all select 'OAUTH_ROWS',count(*),0 from public.oauth_transactions
  union all select 'CONNECTED_CONNECTIONS',count(*),7 from public.platform_connections where connected=true
  union all select 'ENCRYPTED_TOKEN_ROWS',count(*),7 from public.platform_connection_tokens
  union all select 'MISSING_ENCRYPTED',count(*),0 from public.platform_connections pc where pc.connected=true and not exists(select 1 from public.platform_connection_tokens pt where pt.user_id=pc.user_id and pt.platform=pc.platform)
  union all select 'PLAINTEXT_TOKENS',count(*),0 from public.platform_connections where access_token is not null or refresh_token is not null
  union all select 'LEDGER_TOTAL',count(*),37 from supabase_migrations.schema_migrations
  union all select 'SCHEMA_RLS_INDEX_STATE',count(*),1 from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='performance_dataset_rows_v2' and c.relrowsecurity and not c.relforcerowsecurity and (select count(*) from pg_catalog.pg_index i where i.indrelid=c.oid and i.indisvalid and i.indisready)=5
)
select check_code,actual_count,expected_count,actual_count=expected_count passed from checks order by check_code;
