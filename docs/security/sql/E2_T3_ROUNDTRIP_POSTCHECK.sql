-- E2-T3 read-only postcheck. Replace the three captured-count literals only from approved preflight evidence.
with expected(dataset_rows, v1_rows, snapshot_rows, connected_rows, encrypted_rows) as (
  values (0::bigint, (-1)::bigint, (-1)::bigint, (-1)::bigint, (-1)::bigint)
), constants as (
  select 'meta:e2_t3_static_v1_account:paid:none:campaign:e2_t3_static_v1_campaign:ad:e2_t3_static_v1_ad'::text entity_key
), checks(check_code, actual_count, expected_count) as (
  select 'DATASET_ROWS', count(*), e.dataset_rows from public.performance_dataset_rows_v2 cross join expected e
  union all select 'FIXTURE_ROWS', count(*), 0 from public.performance_dataset_rows_v2 d cross join constants k where d.entity_key=k.entity_key
  union all select 'V1_ROWS', count(*), e.v1_rows from public.performance_dataset_rows cross join expected e
  union all select 'SNAPSHOT_ROWS', count(*), e.snapshot_rows from public.dashboard_snapshots cross join expected e
  union all select 'OAUTH_ROWS', count(*), 0 from public.oauth_transactions
  union all select 'CONNECTED_CONNECTIONS',count(*),(select connected_rows from expected) from public.platform_connections where connected=true
  union all select 'ENCRYPTED_TOKEN_ROWS',count(*),(select encrypted_rows from expected) from public.platform_connection_tokens
  union all select 'MISSING_ENCRYPTED', count(*), 0 from public.platform_connections pc where pc.connected=true and not exists (select 1 from public.platform_connection_tokens pt where pt.user_id=pc.user_id and pt.platform=pc.platform)
  union all select 'ORPHAN_ENCRYPTED',(select count(*) from public.platform_connection_tokens pt where not exists(select 1 from public.platform_connections pc where pc.user_id=pt.user_id and pc.platform=pt.platform and pc.connected=true)),0
  union all select 'CONNECTION_TOKEN_PARITY',case when (select count(*) from public.platform_connections where connected=true)=(select count(*) from public.platform_connection_tokens) then 1 else 0 end,1
  union all select 'PLAINTEXT_TOKENS', count(*), 0 from public.platform_connections where access_token is not null or refresh_token is not null
  union all select 'LEDGER_TOTAL', count(*), 37 from supabase_migrations.schema_migrations
  union all select 'SCHEMA_STATE', count(*), 1 from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='performance_dataset_rows_v2' and c.relrowsecurity and not c.relforcerowsecurity
)
select check_code, actual_count, expected_count, actual_count=expected_count passed
from checks order by check_code;
