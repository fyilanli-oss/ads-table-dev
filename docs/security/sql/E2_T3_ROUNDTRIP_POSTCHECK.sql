-- E2-T3 v2 read-only postcheck. Keep dataset_rows=0; replace exactly four operator-local placeholders in V1/snapshot/connected/encrypted order.
with expected(dataset_rows, v1_rows, snapshot_rows, connected_rows, encrypted_rows) as (
  values (0::bigint, (-1)::bigint, (-1)::bigint, (-1)::bigint, (-1)::bigint)
), constants as (
  select 'meta:e2_t3_static_v2_account:paid:none:campaign:e2_t3_static_v2_campaign:ad:e2_t3_static_v2_ad'::text entity_key
), checks(check_code, actual_count, expected_count) as (
  select 'DATASET_ROWS', (select count(*) from public.performance_dataset_rows_v2), (select dataset_rows from expected)
  union all select 'FIXTURE_ROWS', (select count(*) from public.performance_dataset_rows_v2 d cross join constants k where d.entity_key=k.entity_key), 0
  union all select 'V1_ROWS', (select count(*) from public.performance_dataset_rows), (select v1_rows from expected)
  union all select 'SNAPSHOT_ROWS', (select count(*) from public.dashboard_snapshots), (select snapshot_rows from expected)
  union all select 'OAUTH_ROWS', (select count(*) from public.oauth_transactions), 0
  union all select 'CONNECTED_CONNECTIONS', (select count(*) from public.platform_connections where connected=true), (select connected_rows from expected)
  union all select 'ENCRYPTED_TOKEN_ROWS', (select count(*) from public.platform_connection_tokens), (select encrypted_rows from expected)
  union all select 'MISSING_ENCRYPTED', (select count(*) from public.platform_connections pc where pc.connected=true and not exists (select 1 from public.platform_connection_tokens pt where pt.user_id=pc.user_id and pt.platform=pc.platform)), 0
  union all select 'ORPHAN_ENCRYPTED', (select count(*) from public.platform_connection_tokens pt where not exists(select 1 from public.platform_connections pc where pc.user_id=pt.user_id and pc.platform=pt.platform and pc.connected=true)), 0
  union all select 'CONNECTION_TOKEN_PARITY', case when (select count(*) from public.platform_connections where connected=true)=(select count(*) from public.platform_connection_tokens) then 1 else 0 end, 1
  union all select 'PLAINTEXT_TOKENS', (select count(*) from public.platform_connections where access_token is not null or refresh_token is not null), 0
  union all select 'LEDGER_TOTAL', (select count(*) from supabase_migrations.schema_migrations), 37
  union all select 'SCHEMA_STATE', (select count(*) from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='performance_dataset_rows_v2' and c.relrowsecurity and not c.relforcerowsecurity), 1
)
select check_code, actual_count, expected_count, actual_count=expected_count passed
from checks order by check_code;
