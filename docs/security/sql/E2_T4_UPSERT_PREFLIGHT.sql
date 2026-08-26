-- E2-T4 read-only preflight. Aggregate-only; no identity or row data is returned.
with constants as (
  select 'meta:e2_t4_same_key_v2_account:paid:none:campaign:e2_t4_same_key_v2_campaign:ad:e2_t4_same_key_v2_ad'::text entity_key
), index_state as (
  select i.indisvalid, i.indisready,
    array_agg(a.attname order by key.ordinality) as columns
  from pg_catalog.pg_index i
  join pg_catalog.pg_class c on c.oid=i.indrelid
  join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  cross join lateral unnest(i.indkey::smallint[]) with ordinality key(attnum, ordinality)
  join pg_catalog.pg_attribute a on a.attrelid=c.oid and a.attnum=key.attnum
  where n.nspname='public' and c.relname='performance_dataset_rows_v2' and i.indisunique and not i.indisprimary
  group by i.indexrelid,i.indisvalid,i.indisready
), checks(check_code,actual_count,expected_count,comparison) as (
  select 'LEDGER_TOTAL',count(*),37,'eq' from supabase_migrations.schema_migrations
  union all select 'DATASET_TABLE',count(*),1,'eq' from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='performance_dataset_rows_v2' and c.relkind='r'
  union all select 'DATASET_ROWS',count(*),count(*),'capture' from public.performance_dataset_rows_v2
  union all select 'FIXTURE_ROWS',count(*),0,'eq' from public.performance_dataset_rows_v2 d cross join constants k where d.entity_key=k.entity_key
  union all select 'ELIGIBLE_USERS',count(*),1,'gte' from public.users u where exists(select 1 from auth.users a where a.id=u.id)
  union all select 'V1_ROWS',count(*),count(*),'capture' from public.performance_dataset_rows
  union all select 'SNAPSHOT_ROWS',count(*),count(*),'capture' from public.dashboard_snapshots
  union all select 'OAUTH_ROWS',count(*),0,'eq' from public.oauth_transactions
  union all select 'CONNECTED_CONNECTIONS',count(*),count(*),'capture' from public.platform_connections where connected=true
  union all select 'ENCRYPTED_TOKEN_ROWS',count(*),count(*),'capture' from public.platform_connection_tokens
  union all select 'MISSING_ENCRYPTED',count(*),0,'eq' from public.platform_connections pc where pc.connected=true and not exists(select 1 from public.platform_connection_tokens pt where pt.user_id=pc.user_id and pt.platform=pc.platform)
  union all select 'ORPHAN_ENCRYPTED',count(*),0,'eq' from public.platform_connection_tokens pt where not exists(select 1 from public.platform_connections pc where pc.user_id=pt.user_id and pc.platform=pt.platform and pc.connected=true)
  union all select 'PLAINTEXT_TOKENS',count(*),0,'eq' from public.platform_connections where access_token is not null or refresh_token is not null
  union all select 'RLS_STATE',count(*),1,'eq' from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='performance_dataset_rows_v2' and c.relrowsecurity and not c.relforcerowsecurity
  union all select 'CONNECTION_TOKEN_PARITY',case when (select count(*) from public.platform_connections where connected=true)=(select count(*) from public.platform_connection_tokens) then 1 else 0 end,1,'eq'
  union all select 'CANONICAL_UNIQUE_INDEX',count(*),1,'eq' from index_state where indisvalid and indisready and columns=array['user_id','platform','platform_account_id','business_date','traffic_type','entity_key']::name[]
)
select check_code,actual_count,expected_count,
 case comparison when 'eq' then actual_count=expected_count when 'gte' then actual_count>=expected_count else true end passed
from checks order by check_code;
