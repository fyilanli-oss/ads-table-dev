-- Replace the five -1 operator-local placeholders with approved preflight Dataset V2/V1/snapshot/connected/encrypted counts.
with expected(dataset_v2_rows,dataset_v1_rows,snapshot_rows,connected_rows,encrypted_rows) as (values ((-1)::bigint,(-1)::bigint,(-1)::bigint,(-1)::bigint,(-1)::bigint)),
policy_state as (
  select count(*) filter (where p.polname='performance_dataset_rows_v2_select_own' and p.polcmd='r' and p.polpermissive
    and p.polroles=array[(select oid from pg_catalog.pg_roles where rolname='authenticated')]
    and regexp_replace(pg_catalog.pg_get_expr(p.polqual,p.polrelid),E'\\s+','','g') in ('((SELECTauth.uid()ASuid)=user_id)','(SELECTauth.uid()ASuid)=user_id','(auth.uid()=user_id)')) policy_count,
    count(*) policy_total
  from pg_catalog.pg_policy p join pg_catalog.pg_class c on c.oid=p.polrelid join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='performance_dataset_rows_v2'
), checks(check_code,actual_count,expected_count) as (
  select 'DATASET_V2_BASELINE',count(*),e.dataset_v2_rows from public.performance_dataset_rows_v2 cross join expected e group by e.dataset_v2_rows
  union all select 'DATASET_V1_BASELINE',count(*),e.dataset_v1_rows from public.performance_dataset_rows cross join expected e group by e.dataset_v1_rows
  union all select 'SNAPSHOT_BASELINE',count(*),e.snapshot_rows from public.dashboard_snapshots cross join expected e group by e.snapshot_rows
  union all select 'E2_T6_RESIDUE',count(*),0 from public.performance_dataset_rows_v2 where entity_key like 'e2\_t6\_rls\_v1:%' escape '\'
  union all select 'LEDGER_TOTAL',count(*),37 from supabase_migrations.schema_migrations
  union all select 'OAUTH_ROWS',count(*),0 from public.oauth_transactions
  union all select 'CONNECTED_CONNECTIONS',count(*),(select connected_rows from expected) from public.platform_connections where connected=true
  union all select 'ENCRYPTED_TOKEN_ROWS',count(*),(select encrypted_rows from expected) from public.platform_connection_tokens
  union all select 'MISSING_ENCRYPTED',(select count(*) from public.platform_connections pc where pc.connected=true and not exists(select 1 from public.platform_connection_tokens pt where pt.user_id=pc.user_id and pt.platform=pc.platform)),0
  union all select 'ORPHAN_ENCRYPTED',(select count(*) from public.platform_connection_tokens pt where not exists(select 1 from public.platform_connections pc where pc.user_id=pt.user_id and pc.platform=pt.platform and pc.connected=true)),0
  union all select 'CONNECTION_TOKEN_PARITY',case when (select count(*) from public.platform_connections where connected=true)=(select count(*) from public.platform_connection_tokens) then 1 else 0 end,1
  union all select 'PLAINTEXT_TOKENS',count(*),0 from public.platform_connections where access_token is not null or refresh_token is not null
  union all select 'RLS_ENABLED_NOT_FORCED',count(*),1 from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='performance_dataset_rows_v2' and c.relrowsecurity and not c.relforcerowsecurity
  union all select 'EXACT_OWN_SELECT_POLICY',policy_count,1 from policy_state where policy_total=1
  union all select 'ANON_NO_PRIVILEGE',case when has_table_privilege('anon','public.performance_dataset_rows_v2','SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') then 1 else 0 end,0
  union all select 'AUTHENTICATED_SELECT_ONLY',case when has_table_privilege('authenticated','public.performance_dataset_rows_v2','SELECT') and not has_table_privilege('authenticated','public.performance_dataset_rows_v2','INSERT,UPDATE,DELETE,TRUNCATE') then 1 else 0 end,1
  union all select 'SERVICE_ROLE_ACCESS',case when has_table_privilege('service_role','public.performance_dataset_rows_v2','SELECT') and has_table_privilege('service_role','public.performance_dataset_rows_v2','INSERT') and has_table_privilege('service_role','public.performance_dataset_rows_v2','UPDATE') and has_table_privilege('service_role','public.performance_dataset_rows_v2','DELETE') and has_table_privilege('service_role','public.performance_dataset_rows_v2','TRUNCATE') and has_table_privilege('service_role','public.performance_dataset_rows_v2','REFERENCES') and has_table_privilege('service_role','public.performance_dataset_rows_v2','TRIGGER') then 1 else 0 end,1
  union all select 'PERSISTENT_EVIDENCE_OBJECTS',count(*),0 from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname not like 'pg_temp_%' and c.relname like 'e2_t6_rls%'
  union all select 'CONSTRAINT_INDEX_STATE',(select count(*) from pg_catalog.pg_constraint x join pg_catalog.pg_class t on t.oid=x.conrelid join pg_catalog.pg_namespace n on n.oid=t.relnamespace where n.nspname='public' and t.relname='performance_dataset_rows_v2' and x.convalidated)+(select count(*) from pg_catalog.pg_index i join pg_catalog.pg_class t on t.oid=i.indrelid join pg_catalog.pg_namespace n on n.oid=t.relnamespace where n.nspname='public' and t.relname='performance_dataset_rows_v2' and i.indisvalid),26
)
select check_code,actual_count,expected_count,actual_count=expected_count passed from checks order by check_code;
