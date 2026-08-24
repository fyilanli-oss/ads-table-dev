-- E2-T5 read-only postcheck. Replace only -1 placeholders in an operator-local copy.
with expected(dataset_rows,v1_rows,snapshot_rows) as (
  values ((-1)::bigint,(-1)::bigint,(-1)::bigint)
), checks(check_code,actual_count,expected_count) as (
  select 'DATASET_ROWS',(select count(*) from public.performance_dataset_rows_v2),(select dataset_rows from expected)
  union all select 'E2_T5_RESIDUE',(select count(*) from public.performance_dataset_rows_v2 where entity_key like 'e2\_t5\_rejection\_v1:%' escape '\'),0
  union all select 'V1_ROWS',(select count(*) from public.performance_dataset_rows),(select v1_rows from expected)
  union all select 'SNAPSHOT_ROWS',(select count(*) from public.dashboard_snapshots),(select snapshot_rows from expected)
  union all select 'OAUTH_ROWS',(select count(*) from public.oauth_transactions),0
  union all select 'CONNECTED_CONNECTIONS',(select count(*) from public.platform_connections where connected=true),7
  union all select 'ENCRYPTED_TOKEN_ROWS',(select count(*) from public.platform_connection_tokens),7
  union all select 'MISSING_ENCRYPTED',(select count(*) from public.platform_connections pc where pc.connected=true and not exists(select 1 from public.platform_connection_tokens pt where pt.user_id=pc.user_id and pt.platform=pc.platform)),0
  union all select 'PLAINTEXT_TOKENS',(select count(*) from public.platform_connections where access_token is not null or refresh_token is not null),0
  union all select 'LEDGER_TOTAL',(select count(*) from supabase_migrations.schema_migrations),37
  union all select 'VALIDATED_CHECKS',(select count(*) from pg_catalog.pg_constraint c join pg_catalog.pg_class t on t.oid=c.conrelid join pg_catalog.pg_namespace n on n.oid=t.relnamespace where n.nspname='public' and t.relname='performance_dataset_rows_v2' and c.contype='c' and c.convalidated),19
  union all select 'RLS_STATE',(select count(*) from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='performance_dataset_rows_v2' and c.relrowsecurity and not c.relforcerowsecurity),1
  union all select 'PERSISTENT_EVIDENCE_OBJECT',(select count(*) from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname not like 'pg_temp_%' and c.relname='e2_t5_rejection_evidence'),0
)
select check_code,actual_count,expected_count,actual_count=expected_count passed from checks order by check_code;
