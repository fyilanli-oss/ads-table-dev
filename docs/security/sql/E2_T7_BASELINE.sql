-- E2-T7 preparation-only aggregate baseline. STOP if any required gate fails; retain capture values operator-locally only.
with policy_state as (
  select count(*) filter (where p.polname='performance_dataset_rows_v2_select_own' and p.polcmd='r' and p.polpermissive
    and p.polroles=array[(select oid from pg_catalog.pg_roles where rolname='authenticated')]
    and regexp_replace(pg_catalog.pg_get_expr(p.polqual,p.polrelid),E'\\s+','','g') in ('((SELECTauth.uid()ASuid)=user_id)','(SELECTauth.uid()ASuid)=user_id','(auth.uid()=user_id)')) exact_count,
    count(*) policy_total
  from pg_catalog.pg_policy p join pg_catalog.pg_class c on c.oid=p.polrelid join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relname='performance_dataset_rows_v2'
), privilege_state as (
  select
    case when has_table_privilege('anon','public.performance_dataset_rows_v2','SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') then 0 else 1 end anon_ok,
    case when has_table_privilege('authenticated','public.performance_dataset_rows_v2','SELECT') and not has_table_privilege('authenticated','public.performance_dataset_rows_v2','INSERT,UPDATE,DELETE,TRUNCATE') then 1 else 0 end authenticated_ok,
    case when
      has_table_privilege('service_role','public.performance_dataset_rows_v2','SELECT')
      and has_table_privilege('service_role','public.performance_dataset_rows_v2','INSERT')
      and has_table_privilege('service_role','public.performance_dataset_rows_v2','UPDATE')
      and has_table_privilege('service_role','public.performance_dataset_rows_v2','DELETE')
      and has_table_privilege('service_role','public.performance_dataset_rows_v2','TRUNCATE')
      and has_table_privilege('service_role','public.performance_dataset_rows_v2','REFERENCES')
      and has_table_privilege('service_role','public.performance_dataset_rows_v2','TRIGGER')
    then 1 else 0 end service_ok
), checks(check_code,actual_count,expected_count,comparison) as (
  select 'DATASET_V2_BASELINE',(select count(*) from public.performance_dataset_rows_v2),(select count(*) from public.performance_dataset_rows_v2),'capture'
  union all select 'E2_T3_RESIDUE',(select count(*) from public.performance_dataset_rows_v2 where entity_key='meta:e2_t3_static_v1_account:paid:none:campaign:e2_t3_static_v1_campaign:ad:e2_t3_static_v1_ad'),0,'eq'
  union all select 'E2_T4_RESIDUE',(select count(*) from public.performance_dataset_rows_v2 where entity_key='meta:e2_t4_same_key_v1_account:paid:none:campaign:e2_t4_same_key_v1_campaign:ad:e2_t4_same_key_v1_ad'),0,'eq'
  union all select 'E2_T5_RESIDUE',(select count(*) from public.performance_dataset_rows_v2 where entity_key like 'e2\_t5\_rejection\_v1:%' escape '\'),0,'eq'
  union all select 'E2_T6_RESIDUE',(select count(*) from public.performance_dataset_rows_v2 where entity_key like 'e2\_t6\_rls\_v1:%' escape '\'),0,'eq'
  union all select 'TOTAL_E2_RESIDUE',(select count(*) from public.performance_dataset_rows_v2 where entity_key in ('meta:e2_t3_static_v1_account:paid:none:campaign:e2_t3_static_v1_campaign:ad:e2_t3_static_v1_ad','meta:e2_t4_same_key_v1_account:paid:none:campaign:e2_t4_same_key_v1_campaign:ad:e2_t4_same_key_v1_ad') or entity_key like 'e2\_t5\_rejection\_v1:%' escape '\' or entity_key like 'e2\_t6\_rls\_v1:%' escape '\'),0,'eq'
  union all select 'DATASET_V1_BASELINE',(select count(*) from public.performance_dataset_rows),(select count(*) from public.performance_dataset_rows),'capture'
  union all select 'SNAPSHOT_BASELINE',(select count(*) from public.dashboard_snapshots),(select count(*) from public.dashboard_snapshots),'capture'
  union all select 'LEDGER_TOTAL',(select count(*) from supabase_migrations.schema_migrations),37,'eq'
  union all select 'OAUTH_ROWS',(select count(*) from public.oauth_transactions),0,'eq'
  union all select 'CONNECTED_CONNECTIONS',(select count(*) from public.platform_connections where connected=true),7,'eq'
  union all select 'ENCRYPTED_TOKEN_ROWS',(select count(*) from public.platform_connection_tokens),7,'eq'
  union all select 'MISSING_ENCRYPTED',(select count(*) from public.platform_connections pc where pc.connected=true and not exists(select 1 from public.platform_connection_tokens pt where pt.user_id=pc.user_id and pt.platform=pc.platform)),0,'eq'
  union all select 'PLAINTEXT_TOKENS',(select count(*) from public.platform_connections where access_token is not null or refresh_token is not null),0,'eq'
  union all select 'DATASET_V2_SCHEMA_STATE',(select count(*) from pg_catalog.pg_constraint x join pg_catalog.pg_class t on t.oid=x.conrelid join pg_catalog.pg_namespace n on n.oid=t.relnamespace where n.nspname='public' and t.relname='performance_dataset_rows_v2' and x.convalidated)+(select count(*) from pg_catalog.pg_index i join pg_catalog.pg_class t on t.oid=i.indrelid join pg_catalog.pg_namespace n on n.oid=t.relnamespace where n.nspname='public' and t.relname='performance_dataset_rows_v2' and i.indisvalid),26,'eq'
  union all select 'DATASET_V2_RLS_POLICY_GRANT_STATE',(select count(*) from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='performance_dataset_rows_v2' and c.relrowsecurity and not c.relforcerowsecurity)+(select exact_count from policy_state where policy_total=1)+(select anon_ok+authenticated_ok+service_ok from privilege_state),5,'eq'
  union all select 'PERSISTENT_EVIDENCE_OBJECTS',(select count(*) from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname not like 'pg_temp_%' and (c.relname like 'e2\_t3\_%' escape '\' or c.relname like 'e2\_t4\_%' escape '\' or c.relname like 'e2\_t5\_%' escape '\' or c.relname like 'e2\_t6\_%' escape '\' or c.relname like 'e2\_t7\_%' escape '\')),0,'eq'
)
select check_code,actual_count,expected_count,comparison,case comparison when 'eq' then actual_count=expected_count else true end passed from checks order by check_code;
