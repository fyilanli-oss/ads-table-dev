-- E2-T6 V3 controlled PostgreSQL-native harness. Execute only after explicit production approval; never remove ROLLBACK.
begin;
lock table public.performance_dataset_rows_v2 in row exclusive mode;
create temp table pg_temp.e2_t6_rls_evidence (
  case_code text primary key, actor_label text not null, operation text not null, expected_outcome text not null,
  actual_outcome text not null, actual_row_count bigint not null, actual_sqlstate text null, passed boolean not null
) on commit drop;
create temp table pg_temp.e2_t6_rls_actors (
  actor_label text primary key check (actor_label in ('user_a','user_b')), internal_user_id uuid not null unique
) on commit drop;
create temp table pg_temp.e2_t6_rls_baseline (
  dataset_v2_count bigint not null, dataset_v1_count bigint not null, snapshot_count bigint not null,
  oauth_count bigint not null, connected_count bigint not null, encrypted_count bigint not null, ledger_count bigint not null
) on commit drop;
grant insert, select on pg_temp.e2_t6_rls_evidence to authenticated, anon, service_role;
grant select on pg_temp.e2_t6_rls_actors to service_role;
insert into pg_temp.e2_t6_rls_actors (actor_label,internal_user_id)
select case row_number() over(order by u.id) when 1 then 'user_a' else 'user_b' end,u.id
from public.users u where exists(select 1 from auth.users a where a.id=u.id) order by u.id limit 2;
insert into pg_temp.e2_t6_rls_baseline
select (select count(*) from public.performance_dataset_rows_v2),(select count(*) from public.performance_dataset_rows),
  (select count(*) from public.dashboard_snapshots),(select count(*) from public.oauth_transactions),
  (select count(*) from public.platform_connections where connected),(select count(*) from public.platform_connection_tokens),(select count(*) from supabase_migrations.schema_migrations);
do $guard$
begin
  if (select count(*)=2 and count(distinct internal_user_id)=2 and array_agg(actor_label order by actor_label)=array['user_a','user_b'] from pg_temp.e2_t6_rls_actors) is not true then
    raise exception using errcode='P0001', message='E2-T6 requires exactly two selectable eligible users; STOP';
  end if;
  if (select connected_count<>encrypted_count from pg_temp.e2_t6_rls_baseline) or exists(select 1 from public.platform_connections pc where pc.connected and not exists(select 1 from public.platform_connection_tokens pt where pt.user_id=pc.user_id and pt.platform=pc.platform)) or exists(select 1 from public.platform_connection_tokens pt where not exists(select 1 from public.platform_connections pc where pc.user_id=pt.user_id and pc.platform=pt.platform and pc.connected)) or exists(select 1 from public.platform_connections where access_token is not null or refresh_token is not null) then
    raise exception using errcode='P0001', message='E2-T6 provider token invariant failed; STOP';
  end if;
  if exists(select 1 from public.performance_dataset_rows_v2 where raw->>'fixture_namespace'='e2_t6_rls_v3') then
    raise exception using errcode='P0001', message='E2-T6 namespace residue; STOP';
  end if;
end $guard$;
set local role service_role;
with inserted as (
  insert into public.performance_dataset_rows_v2 (
    user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,
    root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,
    entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,
    add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,
    fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,
    adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
  select internal_user_id,'meta','paid','meta_ads',null,'e2_t6_rls_v3_account_a',current_date,null,
    'campaign','e2_t6_rls_v3_campaign_a','E2 T6 Campaign','adset','e2_t6_rls_v3_adset_a','E2 T6 AdSet',
    'ad','e2_t6_rls_v3_ad_a','E2 T6 Ad','meta:e2_t6_rls_v3_account_a:paid:none:campaign:e2_t6_rls_v3_campaign_a:ad:e2_t6_rls_v3_ad_a',
    '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb,
    1,1,null,1,0,0,0,0,0,0,'USD','TRY',1,current_date,'e2_t6_fixed_fx','v1','UTC','v1','v1','e2-t6-meta-v3','real',false,null,null,
    '{"fixture_namespace":"e2_t6_rls_v3","actor_label":"user_a"}'::jsonb from pg_temp.e2_t6_rls_actors where actor_label='user_a' returning 1)
insert into pg_temp.e2_t6_rls_evidence
select 'SERVICE_ROLE_INSERT_A','service_role','INSERT','ALLOWED','ALLOWED',count(*),null,count(*)=1 from inserted;
with inserted as (
  insert into public.performance_dataset_rows_v2 (
    user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,
    root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,
    entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,
    add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,
    fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,
    adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
  select internal_user_id,'meta','paid','meta_ads',null,'e2_t6_rls_v3_account_b',current_date,null,
    'campaign','e2_t6_rls_v3_campaign_b','E2 T6 Campaign','adset','e2_t6_rls_v3_adset_b','E2 T6 AdSet',
    'ad','e2_t6_rls_v3_ad_b','E2 T6 Ad','meta:e2_t6_rls_v3_account_b:paid:none:campaign:e2_t6_rls_v3_campaign_b:ad:e2_t6_rls_v3_ad_b',
    '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb,
    1,1,null,1,0,0,0,0,0,0,'USD','TRY',1,current_date,'e2_t6_fixed_fx','v1','UTC','v1','v1','e2-t6-meta-v3','real',false,null,null,
    '{"fixture_namespace":"e2_t6_rls_v3","actor_label":"user_b"}'::jsonb from pg_temp.e2_t6_rls_actors where actor_label='user_b' returning 1)
insert into pg_temp.e2_t6_rls_evidence
select 'SERVICE_ROLE_INSERT_B','service_role','INSERT','ALLOWED','ALLOWED',count(*),null,count(*)=1 from inserted;
reset role;
do $claim_a$ begin perform set_config('request.jwt.claim.sub',(select internal_user_id::text from pg_temp.e2_t6_rls_actors where actor_label='user_a'),true); perform set_config('request.jwt.claims',json_build_object('sub',(select internal_user_id::text from pg_temp.e2_t6_rls_actors where actor_label='user_a'),'role','authenticated')::text,true); end $claim_a$;
set local role authenticated;
with observed as (select count(*) row_count from public.performance_dataset_rows_v2 where entity_key='meta:e2_t6_rls_v3_account_a:paid:none:campaign:e2_t6_rls_v3_campaign_a:ad:e2_t6_rls_v3_ad_a')
insert into pg_temp.e2_t6_rls_evidence select 'USER_A_READ_OWN','user_a','SELECT','ALLOWED','ALLOWED',row_count,null,row_count=1 from observed;
with observed as (select count(*) row_count from public.performance_dataset_rows_v2 where entity_key='meta:e2_t6_rls_v3_account_b:paid:none:campaign:e2_t6_rls_v3_campaign_b:ad:e2_t6_rls_v3_ad_b')
insert into pg_temp.e2_t6_rls_evidence select 'USER_A_READ_USER_B','user_a','SELECT','DENIED_BY_RLS','DENIED_BY_RLS',row_count,null,row_count=0 from observed;
do $case_user_a_insert$
declare v_outcome text := 'ALLOWED'; v_count bigint := 0; v_state text := null;
begin
  begin
    insert into public.performance_dataset_rows_v2 (user_id) values (null); get diagnostics v_count = row_count;
    raise exception using errcode='Z0001',message='local rollback sentinel';
  exception when sqlstate 'Z0001' then null;
    when insufficient_privilege then v_outcome := 'DENIED_BY_PRIVILEGE'; v_count := 0; v_state := sqlstate;
    when others then v_outcome := 'UNEXPECTED_SQLSTATE'; v_count := 0; v_state := sqlstate;
  end;
  insert into pg_temp.e2_t6_rls_evidence values ('USER_A_INSERT','user_a','INSERT','DENIED_BY_PRIVILEGE',v_outcome,v_count,v_state,v_outcome='DENIED_BY_PRIVILEGE' and v_state='42501');
end $case_user_a_insert$;
do $case_user_a_update_own$
declare v_outcome text := 'ALLOWED'; v_count bigint := 0; v_state text := null;
begin
  begin
    update public.performance_dataset_rows_v2 set entity_name=entity_name where entity_key='meta:e2_t6_rls_v3_account_a:paid:none:campaign:e2_t6_rls_v3_campaign_a:ad:e2_t6_rls_v3_ad_a'; get diagnostics v_count = row_count;
    raise exception using errcode='Z0001',message='local rollback sentinel';
  exception when sqlstate 'Z0001' then null;
    when insufficient_privilege then v_outcome := 'DENIED_BY_PRIVILEGE'; v_count := 0; v_state := sqlstate;
    when others then v_outcome := 'UNEXPECTED_SQLSTATE'; v_count := 0; v_state := sqlstate;
  end;
  insert into pg_temp.e2_t6_rls_evidence values ('USER_A_UPDATE_OWN','user_a','UPDATE','DENIED_BY_PRIVILEGE',v_outcome,v_count,v_state,v_outcome='DENIED_BY_PRIVILEGE' and v_state='42501');
end $case_user_a_update_own$;
do $case_user_a_delete_own$
declare v_outcome text := 'ALLOWED'; v_count bigint := 0; v_state text := null;
begin
  begin
    delete from public.performance_dataset_rows_v2 where entity_key='meta:e2_t6_rls_v3_account_a:paid:none:campaign:e2_t6_rls_v3_campaign_a:ad:e2_t6_rls_v3_ad_a'; get diagnostics v_count = row_count;
    raise exception using errcode='Z0001',message='local rollback sentinel';
  exception when sqlstate 'Z0001' then null;
    when insufficient_privilege then v_outcome := 'DENIED_BY_PRIVILEGE'; v_count := 0; v_state := sqlstate;
    when others then v_outcome := 'UNEXPECTED_SQLSTATE'; v_count := 0; v_state := sqlstate;
  end;
  insert into pg_temp.e2_t6_rls_evidence values ('USER_A_DELETE_OWN','user_a','DELETE','DENIED_BY_PRIVILEGE',v_outcome,v_count,v_state,v_outcome='DENIED_BY_PRIVILEGE' and v_state='42501');
end $case_user_a_delete_own$;
reset role;
do $claim_b$ begin perform set_config('request.jwt.claim.sub',(select internal_user_id::text from pg_temp.e2_t6_rls_actors where actor_label='user_b'),true); perform set_config('request.jwt.claims',json_build_object('sub',(select internal_user_id::text from pg_temp.e2_t6_rls_actors where actor_label='user_b'),'role','authenticated')::text,true); end $claim_b$;
set local role authenticated;
with observed as (select count(*) row_count from public.performance_dataset_rows_v2 where entity_key='meta:e2_t6_rls_v3_account_b:paid:none:campaign:e2_t6_rls_v3_campaign_b:ad:e2_t6_rls_v3_ad_b')
insert into pg_temp.e2_t6_rls_evidence select 'USER_B_READ_OWN','user_b','SELECT','ALLOWED','ALLOWED',row_count,null,row_count=1 from observed;
with observed as (select count(*) row_count from public.performance_dataset_rows_v2 where entity_key='meta:e2_t6_rls_v3_account_a:paid:none:campaign:e2_t6_rls_v3_campaign_a:ad:e2_t6_rls_v3_ad_a')
insert into pg_temp.e2_t6_rls_evidence select 'USER_B_READ_USER_A','user_b','SELECT','DENIED_BY_RLS','DENIED_BY_RLS',row_count,null,row_count=0 from observed;
do $case_user_b_insert$
declare v_outcome text := 'ALLOWED'; v_count bigint := 0; v_state text := null;
begin
  begin
    insert into public.performance_dataset_rows_v2 (user_id) values (null); get diagnostics v_count = row_count;
    raise exception using errcode='Z0001',message='local rollback sentinel';
  exception when sqlstate 'Z0001' then null;
    when insufficient_privilege then v_outcome := 'DENIED_BY_PRIVILEGE'; v_count := 0; v_state := sqlstate;
    when others then v_outcome := 'UNEXPECTED_SQLSTATE'; v_count := 0; v_state := sqlstate;
  end;
  insert into pg_temp.e2_t6_rls_evidence values ('USER_B_INSERT','user_b','INSERT','DENIED_BY_PRIVILEGE',v_outcome,v_count,v_state,v_outcome='DENIED_BY_PRIVILEGE' and v_state='42501');
end $case_user_b_insert$;
do $case_user_b_update_own$
declare v_outcome text := 'ALLOWED'; v_count bigint := 0; v_state text := null;
begin
  begin
    update public.performance_dataset_rows_v2 set entity_name=entity_name where entity_key='meta:e2_t6_rls_v3_account_b:paid:none:campaign:e2_t6_rls_v3_campaign_b:ad:e2_t6_rls_v3_ad_b'; get diagnostics v_count = row_count;
    raise exception using errcode='Z0001',message='local rollback sentinel';
  exception when sqlstate 'Z0001' then null;
    when insufficient_privilege then v_outcome := 'DENIED_BY_PRIVILEGE'; v_count := 0; v_state := sqlstate;
    when others then v_outcome := 'UNEXPECTED_SQLSTATE'; v_count := 0; v_state := sqlstate;
  end;
  insert into pg_temp.e2_t6_rls_evidence values ('USER_B_UPDATE_OWN','user_b','UPDATE','DENIED_BY_PRIVILEGE',v_outcome,v_count,v_state,v_outcome='DENIED_BY_PRIVILEGE' and v_state='42501');
end $case_user_b_update_own$;
do $case_user_b_delete_own$
declare v_outcome text := 'ALLOWED'; v_count bigint := 0; v_state text := null;
begin
  begin
    delete from public.performance_dataset_rows_v2 where entity_key='meta:e2_t6_rls_v3_account_b:paid:none:campaign:e2_t6_rls_v3_campaign_b:ad:e2_t6_rls_v3_ad_b'; get diagnostics v_count = row_count;
    raise exception using errcode='Z0001',message='local rollback sentinel';
  exception when sqlstate 'Z0001' then null;
    when insufficient_privilege then v_outcome := 'DENIED_BY_PRIVILEGE'; v_count := 0; v_state := sqlstate;
    when others then v_outcome := 'UNEXPECTED_SQLSTATE'; v_count := 0; v_state := sqlstate;
  end;
  insert into pg_temp.e2_t6_rls_evidence values ('USER_B_DELETE_OWN','user_b','DELETE','DENIED_BY_PRIVILEGE',v_outcome,v_count,v_state,v_outcome='DENIED_BY_PRIVILEGE' and v_state='42501');
end $case_user_b_delete_own$;
reset role;
do $reset_claims$ begin perform set_config('request.jwt.claim.sub','',true); perform set_config('request.jwt.claims','',true); end $reset_claims$;
set local role anon;
do $case_anon_read_user_a$
declare v_outcome text := 'ALLOWED'; v_count bigint := 0; v_state text := null;
begin
  begin
    select count(*) into v_count from public.performance_dataset_rows_v2 where entity_key='meta:e2_t6_rls_v3_account_a:paid:none:campaign:e2_t6_rls_v3_campaign_a:ad:e2_t6_rls_v3_ad_a';
  exception when insufficient_privilege then v_outcome := 'DENIED_BY_PRIVILEGE'; v_count := 0; v_state := sqlstate;
    when others then v_outcome := 'UNEXPECTED_SQLSTATE'; v_count := 0; v_state := sqlstate;
  end;
  insert into pg_temp.e2_t6_rls_evidence values ('ANON_READ_USER_A','anon','SELECT','DENIED_BY_PRIVILEGE',v_outcome,v_count,v_state,v_outcome='DENIED_BY_PRIVILEGE' and v_state='42501');
end $case_anon_read_user_a$;
do $case_anon_read_user_b$
declare v_outcome text := 'ALLOWED'; v_count bigint := 0; v_state text := null;
begin
  begin
    select count(*) into v_count from public.performance_dataset_rows_v2 where entity_key='meta:e2_t6_rls_v3_account_b:paid:none:campaign:e2_t6_rls_v3_campaign_b:ad:e2_t6_rls_v3_ad_b';
  exception when insufficient_privilege then v_outcome := 'DENIED_BY_PRIVILEGE'; v_count := 0; v_state := sqlstate;
    when others then v_outcome := 'UNEXPECTED_SQLSTATE'; v_count := 0; v_state := sqlstate;
  end;
  insert into pg_temp.e2_t6_rls_evidence values ('ANON_READ_USER_B','anon','SELECT','DENIED_BY_PRIVILEGE',v_outcome,v_count,v_state,v_outcome='DENIED_BY_PRIVILEGE' and v_state='42501');
end $case_anon_read_user_b$;
reset role;
do $reset_claims$ begin perform set_config('request.jwt.claim.sub','',true); perform set_config('request.jwt.claims','',true); end $reset_claims$;
set local role service_role;
with observed as (select count(*) row_count from public.performance_dataset_rows_v2 where entity_key='meta:e2_t6_rls_v3_account_a:paid:none:campaign:e2_t6_rls_v3_campaign_a:ad:e2_t6_rls_v3_ad_a')
insert into pg_temp.e2_t6_rls_evidence select 'SERVICE_ROLE_READ_A','service_role','SELECT','ALLOWED','ALLOWED',row_count,null,row_count=1 from observed;
with observed as (select count(*) row_count from public.performance_dataset_rows_v2 where entity_key='meta:e2_t6_rls_v3_account_b:paid:none:campaign:e2_t6_rls_v3_campaign_b:ad:e2_t6_rls_v3_ad_b')
insert into pg_temp.e2_t6_rls_evidence select 'SERVICE_ROLE_READ_B','service_role','SELECT','ALLOWED','ALLOWED',row_count,null,row_count=1 from observed;
reset role;
do $reset_claims$ begin perform set_config('request.jwt.claim.sub','',true); perform set_config('request.jwt.claims','',true); end $reset_claims$;
with summary as (
  select count(*) evaluated_case_count,count(*) filter(where passed) passed_case_count,count(*) filter(where not passed) failed_case_count,
    count(*) filter(where expected_outcome<>'ALLOWED' and actual_outcome='ALLOWED') unexpected_allow_count from pg_temp.e2_t6_rls_evidence
), residue as (select greatest(count(*)-2,0) residue_count,count(*) fixture_count from public.performance_dataset_rows_v2 where raw->>'fixture_namespace'='e2_t6_rls_v3'),
parity as (select
  (select count(*) from public.performance_dataset_rows_v2)=b.dataset_v2_count+2 dataset_baseline_preserved,
  (select count(*) from public.performance_dataset_rows)=b.dataset_v1_count v1_unchanged,
  (select count(*) from public.dashboard_snapshots)=b.snapshot_count snapshots_unchanged,
  (select count(*) from public.oauth_transactions)=b.oauth_count oauth_unchanged,
  (select count(*) from public.platform_connections where connected)=b.connected_count connected_unchanged,
  (select count(*) from public.platform_connection_tokens)=b.encrypted_count encrypted_unchanged,
  (select count(*) from public.platform_connections pc where pc.connected and not exists(select 1 from public.platform_connection_tokens pt where pt.user_id=pc.user_id and pt.platform=pc.platform))=0 missing_encrypted_unchanged,
  (select count(*) from public.platform_connection_tokens pt where not exists(select 1 from public.platform_connections pc where pc.user_id=pt.user_id and pc.platform=pt.platform and pc.connected))=0 orphan_encrypted_unchanged,
  (select count(*) from public.platform_connections where access_token is not null or refresh_token is not null)=0 plaintext_unchanged,
  (select count(*) from supabase_migrations.schema_migrations)=b.ledger_count ledger_unchanged
  from pg_temp.e2_t6_rls_baseline b),
payload as (select jsonb_build_object(
  'operation_code','e2_t6_rls_v3','expected_case_count',16,'evaluated_case_count',s.evaluated_case_count,
  'passed_case_count',s.passed_case_count,'failed_case_count',s.failed_case_count,'unexpected_allow_count',s.unexpected_allow_count,
  'fixture_count',r.fixture_count,'residue_count',r.residue_count,
  'dataset_baseline_preserved',p.dataset_baseline_preserved,
  'v1_unchanged',p.v1_unchanged,'snapshots_unchanged',p.snapshots_unchanged,'oauth_unchanged',p.oauth_unchanged,
  'connected_unchanged',p.connected_unchanged,'encrypted_unchanged',p.encrypted_unchanged,'missing_encrypted_unchanged',p.missing_encrypted_unchanged,'orphan_encrypted_unchanged',p.orphan_encrypted_unchanged,'plaintext_unchanged',p.plaintext_unchanged,'ledger_unchanged',p.ledger_unchanged,
  'overall_passed',s.evaluated_case_count=16 and s.passed_case_count=16 and s.failed_case_count=0 and s.unexpected_allow_count=0 and r.fixture_count=2 and r.residue_count=0 and p.dataset_baseline_preserved and p.v1_unchanged and p.snapshots_unchanged and p.oauth_unchanged and p.connected_unchanged and p.encrypted_unchanged and p.missing_encrypted_unchanged and p.orphan_encrypted_unchanged and p.plaintext_unchanged and p.ledger_unchanged,
  'cases',(select jsonb_agg(jsonb_build_object('case_code',case_code,'actor',actor_label,'operation',operation,'target',case
      when case_code like '%READ_OWN' or case_code like '%UPDATE_OWN' or case_code like '%DELETE_OWN' or case_code like '%INSERT' then 'own'
      when case_code like '%USER_A' and actor_label='user_b' then 'user_a' when case_code like '%USER_B' and actor_label='user_a' then 'user_b'
      when case_code like '%_A' then 'user_a' else 'user_b' end,
    'expected_outcome',expected_outcome,'actual_outcome',actual_outcome,'actual_row_count',actual_row_count,'actual_sqlstate',actual_sqlstate,'passed',passed)
    order by array_position(array['USER_A_READ_OWN','USER_A_READ_USER_B','USER_B_READ_OWN','USER_B_READ_USER_A','ANON_READ_USER_A','ANON_READ_USER_B','USER_A_INSERT','USER_A_UPDATE_OWN','USER_A_DELETE_OWN','USER_B_INSERT','USER_B_UPDATE_OWN','USER_B_DELETE_OWN','SERVICE_ROLE_INSERT_A','SERVICE_ROLE_INSERT_B','SERVICE_ROLE_READ_A','SERVICE_ROLE_READ_B'],case_code)) from pg_temp.e2_t6_rls_evidence
)) evidence from summary s cross join residue r cross join parity p)
select evidence from payload;
rollback;
