-- E2-T4 V2 read-only scalar postcheck. Replace only the five -1 placeholders in an operator-local copy.
with expected(dataset_rows,v1_rows,snapshot_rows,connected_rows,encrypted_rows) as (
  values ((-1)::bigint,(-1)::bigint,(-1)::bigint,(-1)::bigint,(-1)::bigint)
), constants as (
  select 'meta:e2_t4_same_key_v2_account:paid:none:campaign:e2_t4_same_key_v2_campaign:ad:e2_t4_same_key_v2_ad'::text entity_key
), checks(check_code,actual_count,expected_count) as (
  select 'DATASET_ROWS',
    (select count(*)::bigint from public.performance_dataset_rows_v2),
    (select dataset_rows::bigint from expected)
  union all select 'FIXTURE_ROWS',
    (select count(*)::bigint from public.performance_dataset_rows_v2 d cross join constants k where d.entity_key=k.entity_key),
    0::bigint
  union all select 'DUPLICATE_GROUPS',
    (select count(*)::bigint from (select d.user_id,d.platform,d.platform_account_id,d.business_date,d.traffic_type,d.entity_key from public.performance_dataset_rows_v2 d cross join constants k where d.entity_key=k.entity_key group by d.user_id,d.platform,d.platform_account_id,d.business_date,d.traffic_type,d.entity_key having count(*)>1) duplicate_groups),
    0::bigint
  union all select 'V1_ROWS',
    (select count(*)::bigint from public.performance_dataset_rows),
    (select v1_rows::bigint from expected)
  union all select 'SNAPSHOT_ROWS',
    (select count(*)::bigint from public.dashboard_snapshots),
    (select snapshot_rows::bigint from expected)
  union all select 'OAUTH_ROWS',
    (select count(*)::bigint from public.oauth_transactions),
    0::bigint
  union all select 'CONNECTED_CONNECTIONS',
    (select count(*)::bigint from public.platform_connections where connected=true),
    (select connected_rows::bigint from expected)
  union all select 'ENCRYPTED_TOKEN_ROWS',
    (select count(*)::bigint from public.platform_connection_tokens),
    (select encrypted_rows::bigint from expected)
  union all select 'MISSING_ENCRYPTED',
    (select count(*)::bigint from public.platform_connections pc where pc.connected=true and not exists(select 1 from public.platform_connection_tokens pt where pt.user_id=pc.user_id and pt.platform=pc.platform)),
    0::bigint
  union all select 'ORPHAN_ENCRYPTED',
    (select count(*)::bigint from public.platform_connection_tokens pt where not exists(select 1 from public.platform_connections pc where pc.user_id=pt.user_id and pc.platform=pt.platform and pc.connected=true)),
    0::bigint
  union all select 'CONNECTION_TOKEN_PARITY',
    (select (case when (select count(*) from public.platform_connections where connected=true)=(select count(*) from public.platform_connection_tokens) then 1 else 0 end)::bigint),
    1::bigint
  union all select 'PLAINTEXT_TOKENS',
    (select count(*)::bigint from public.platform_connections where access_token is not null or refresh_token is not null),
    0::bigint
  union all select 'LEDGER_TOTAL',
    (select count(*)::bigint from supabase_migrations.schema_migrations),
    37::bigint
  union all select 'SCHEMA_RLS_INDEX_STATE',
    (select count(*)::bigint from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='performance_dataset_rows_v2' and c.relrowsecurity and not c.relforcerowsecurity and (select count(*) from pg_catalog.pg_index i where i.indrelid=c.oid and i.indisvalid and i.indisready)=5),
    1::bigint
)
select check_code,actual_count::bigint as actual_count,expected_count::bigint as expected_count,(actual_count=expected_count)::boolean as passed
from checks order by check_code;
