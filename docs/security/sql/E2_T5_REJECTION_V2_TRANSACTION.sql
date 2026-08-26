-- E2-T5 V2 CONTROLLED PAYLOAD: static invalid INSERTs, safe diagnostics, mandatory final ROLLBACK.
begin;
lock table public.performance_dataset_rows_v2 in share row exclusive mode;
lock table public.performance_dataset_rows, public.dashboard_snapshots, public.oauth_transactions, public.platform_connections, public.platform_connection_tokens in share mode;
create temp table pg_temp.e2_t5_security_baseline as select
  (select count(*) from public.platform_connections where connected) connected_count,
  (select count(*) from public.platform_connection_tokens) encrypted_count;
create temp table pg_temp.e2_t5_rejection_evidence (
  case_code text primary key, expected_sqlstate text not null, expected_constraints text[] not null,
  expected_column text null, actual_sqlstate text null, actual_constraint text null, actual_column text null,
  rejected boolean not null, passed boolean not null, dataset_before bigint not null, v1_before bigint not null,
  snapshots_before bigint not null
) on commit drop;
do $e2_t5$
declare
  v_user_id uuid; v_state text; v_constraint text; v_column text;
  v_dataset_before bigint; v_v1_before bigint; v_snapshots_before bigint; v_residue bigint;
begin
  select u.id into v_user_id from public.users u where exists(select 1 from auth.users a where a.id=u.id) order by u.id limit 1;
  select count(*) into v_dataset_before from public.performance_dataset_rows_v2;
  select count(*) into v_v1_before from public.performance_dataset_rows;
  select count(*) into v_snapshots_before from public.dashboard_snapshots;
  select count(*) into v_residue from public.performance_dataset_rows_v2 where entity_key like 'e2\_t5\_rejection\_v2:%' escape '\';
  if v_user_id is not null and v_residue=0
    and (select count(*) from supabase_migrations.schema_migrations)=37
    and (select count(*) from public.oauth_transactions)=0
    and (select connected_count=encrypted_count from pg_temp.e2_t5_security_baseline)
    and (select count(*) from public.platform_connections pc where pc.connected and not exists(select 1 from public.platform_connection_tokens pt where pt.user_id=pc.user_id and pt.platform=pc.platform))=0
    and (select count(*) from public.platform_connection_tokens pt where not exists(select 1 from public.platform_connections pc where pc.user_id=pt.user_id and pc.platform=pt.platform and pc.connected))=0
    and (select count(*) from public.platform_connections where access_token is not null or refresh_token is not null)=0 then
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'invalid', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_invalid_platform_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_invalid_platform_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_invalid_platform_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_invalid_platform_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:invalid_platform', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('INVALID_PLATFORM','23514',array['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_platform_chk','performance_dataset_rows_v2_source_semantics_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_PLATFORM','23514',array['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_platform_chk','performance_dataset_rows_v2_source_semantics_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_platform_chk','performance_dataset_rows_v2_source_semantics_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_PLATFORM','23514',array['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_platform_chk','performance_dataset_rows_v2_source_semantics_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'invalid', 'meta_ads', null, 'e2_t5_rejection_v2_invalid_traffic_type_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_invalid_traffic_type_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_invalid_traffic_type_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_invalid_traffic_type_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:invalid_traffic_type', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('INVALID_TRAFFIC_TYPE','23514',array['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_source_semantics_chk','performance_dataset_rows_v2_traffic_type_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_TRAFFIC_TYPE','23514',array['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_source_semantics_chk','performance_dataset_rows_v2_traffic_type_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_source_semantics_chk','performance_dataset_rows_v2_traffic_type_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_TRAFFIC_TYPE','23514',array['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_source_semantics_chk','performance_dataset_rows_v2_traffic_type_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'invalid', null, 'e2_t5_rejection_v2_invalid_source_system_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_invalid_source_system_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_invalid_source_system_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_invalid_source_system_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:invalid_source_system', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('INVALID_SOURCE_SYSTEM','23514',array['performance_dataset_rows_v2_source_semantics_chk','performance_dataset_rows_v2_source_system_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_SOURCE_SYSTEM','23514',array['performance_dataset_rows_v2_source_semantics_chk','performance_dataset_rows_v2_source_system_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_source_semantics_chk','performance_dataset_rows_v2_source_system_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_SOURCE_SYSTEM','23514',array['performance_dataset_rows_v2_source_semantics_chk','performance_dataset_rows_v2_source_system_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', 'push', 'e2_t5_rejection_v2_invalid_channel_enum_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_invalid_channel_enum_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_invalid_channel_enum_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_invalid_channel_enum_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:invalid_channel_enum', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('INVALID_CHANNEL_ENUM','23514',array['performance_dataset_rows_v2_channel_chk','performance_dataset_rows_v2_source_semantics_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_CHANNEL_ENUM','23514',array['performance_dataset_rows_v2_channel_chk','performance_dataset_rows_v2_source_semantics_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_channel_chk','performance_dataset_rows_v2_source_semantics_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_CHANNEL_ENUM','23514',array['performance_dataset_rows_v2_channel_chk','performance_dataset_rows_v2_source_semantics_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_invalid_campaign_type_account', '2026-08-26', 'invalid', 'campaign', 'e2_t5_rejection_v2_invalid_campaign_type_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_invalid_campaign_type_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_invalid_campaign_type_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:invalid_campaign_type', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('INVALID_CAMPAIGN_TYPE','23514',array['performance_dataset_rows_v2_campaign_type_chk','performance_dataset_rows_v2_hierarchy_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_CAMPAIGN_TYPE','23514',array['performance_dataset_rows_v2_campaign_type_chk','performance_dataset_rows_v2_hierarchy_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_campaign_type_chk','performance_dataset_rows_v2_hierarchy_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_CAMPAIGN_TYPE','23514',array['performance_dataset_rows_v2_campaign_type_chk','performance_dataset_rows_v2_hierarchy_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_invalid_root_entity_type_account', '2026-08-26', null, 'invalid', 'e2_t5_rejection_v2_invalid_root_entity_type_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_invalid_root_entity_type_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_invalid_root_entity_type_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:invalid_root_entity_type', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('INVALID_ROOT_ENTITY_TYPE','23514',array['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_root_type_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_ROOT_ENTITY_TYPE','23514',array['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_root_type_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_root_type_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_ROOT_ENTITY_TYPE','23514',array['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_root_type_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_invalid_parent_entity_type_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_invalid_parent_entity_type_root', 'E2 T5 Root', 'invalid', 'e2_t5_rejection_v2_invalid_parent_entity_type_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_invalid_parent_entity_type_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:invalid_parent_entity_type', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('INVALID_PARENT_ENTITY_TYPE','23514',array['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_parent_type_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_PARENT_ENTITY_TYPE','23514',array['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_parent_type_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_parent_type_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_PARENT_ENTITY_TYPE','23514',array['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_parent_type_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_invalid_entity_type_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_invalid_entity_type_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_invalid_entity_type_parent', 'E2 T5 Parent', 'invalid', 'e2_t5_rejection_v2_invalid_entity_type_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:invalid_entity_type', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('INVALID_ENTITY_TYPE','23514',array['performance_dataset_rows_v2_entity_type_chk','performance_dataset_rows_v2_hierarchy_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_ENTITY_TYPE','23514',array['performance_dataset_rows_v2_entity_type_chk','performance_dataset_rows_v2_hierarchy_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_entity_type_chk','performance_dataset_rows_v2_hierarchy_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_ENTITY_TYPE','23514',array['performance_dataset_rows_v2_entity_type_chk','performance_dataset_rows_v2_hierarchy_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_invalid_source_confidence_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_invalid_source_confidence_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_invalid_source_confidence_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_invalid_source_confidence_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:invalid_source_confidence', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'invalid', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('INVALID_SOURCE_CONFIDENCE','23514',array['performance_dataset_rows_v2_source_confidence_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_SOURCE_CONFIDENCE','23514',array['performance_dataset_rows_v2_source_confidence_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_source_confidence_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_SOURCE_CONFIDENCE','23514',array['performance_dataset_rows_v2_source_confidence_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_invalid_source_currency_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_invalid_source_currency_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_invalid_source_currency_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_invalid_source_currency_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:invalid_source_currency', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'usd', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('INVALID_SOURCE_CURRENCY','23514',array['performance_dataset_rows_v2_source_currency_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_SOURCE_CURRENCY','23514',array['performance_dataset_rows_v2_source_currency_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_source_currency_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_SOURCE_CURRENCY','23514',array['performance_dataset_rows_v2_source_currency_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_invalid_target_currency_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_invalid_target_currency_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_invalid_target_currency_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_invalid_target_currency_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:invalid_target_currency', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRYX', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('INVALID_TARGET_CURRENCY','23514',array['performance_dataset_rows_v2_target_currency_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_TARGET_CURRENCY','23514',array['performance_dataset_rows_v2_target_currency_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_target_currency_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_TARGET_CURRENCY','23514',array['performance_dataset_rows_v2_target_currency_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_non_positive_fx_rate_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_non_positive_fx_rate_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_non_positive_fx_rate_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_non_positive_fx_rate_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:non_positive_fx_rate', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 0, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('NON_POSITIVE_FX_RATE','23514',array['performance_dataset_rows_v2_fx_rate_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('NON_POSITIVE_FX_RATE','23514',array['performance_dataset_rows_v2_fx_rate_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_fx_rate_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('NON_POSITIVE_FX_RATE','23514',array['performance_dataset_rows_v2_fx_rate_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_metric_support_not_object_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_metric_support_not_object_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_metric_support_not_object_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_metric_support_not_object_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:metric_support_not_object', '[]'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('METRIC_SUPPORT_NOT_OBJECT','23514',array['performance_dataset_rows_v2_metric_support_keys_chk','performance_dataset_rows_v2_metric_support_object_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('METRIC_SUPPORT_NOT_OBJECT','23514',array['performance_dataset_rows_v2_metric_support_keys_chk','performance_dataset_rows_v2_metric_support_object_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_metric_support_keys_chk','performance_dataset_rows_v2_metric_support_object_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('METRIC_SUPPORT_NOT_OBJECT','23514',array['performance_dataset_rows_v2_metric_support_keys_chk','performance_dataset_rows_v2_metric_support_object_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_raw_not_object_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_raw_not_object_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_raw_not_object_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_raw_not_object_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:raw_not_object', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '[]'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('RAW_NOT_OBJECT','23514',array['performance_dataset_rows_v2_raw_object_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('RAW_NOT_OBJECT','23514',array['performance_dataset_rows_v2_raw_object_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_raw_object_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('RAW_NOT_OBJECT','23514',array['performance_dataset_rows_v2_raw_object_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_synthetic_true_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_synthetic_true_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_synthetic_true_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_synthetic_true_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:synthetic_true', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', true, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('SYNTHETIC_TRUE','23514',array['performance_dataset_rows_v2_synthetic_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('SYNTHETIC_TRUE','23514',array['performance_dataset_rows_v2_synthetic_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_synthetic_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('SYNTHETIC_TRUE','23514',array['performance_dataset_rows_v2_synthetic_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'google_ads', null, 'e2_t5_rejection_v2_meta_wrong_source_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_meta_wrong_source_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_meta_wrong_source_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_meta_wrong_source_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:meta_wrong_source', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('META_WRONG_SOURCE','23514',array['performance_dataset_rows_v2_source_semantics_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('META_WRONG_SOURCE','23514',array['performance_dataset_rows_v2_source_semantics_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_source_semantics_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('META_WRONG_SOURCE','23514',array['performance_dataset_rows_v2_source_semantics_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_paid_with_ga4_property_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_paid_with_ga4_property_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_paid_with_ga4_property_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_paid_with_ga4_property_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:paid_with_ga4_property', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, 'e2_t5_property', null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('PAID_WITH_GA4_PROPERTY','23514',array['performance_dataset_rows_v2_source_semantics_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('PAID_WITH_GA4_PROPERTY','23514',array['performance_dataset_rows_v2_source_semantics_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_source_semantics_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('PAID_WITH_GA4_PROPERTY','23514',array['performance_dataset_rows_v2_source_semantics_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'organic', 'ga4', null, 'e2_t5_rejection_v2_organic_without_ga4_property_account', '2026-08-26', null, 'organic', 'e2_t5_rejection_v2_organic_without_ga4_property_root', 'E2 T5 Root', null, null, null, 'organic', 'e2_t5_rejection_v2_organic_without_ga4_property_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:organic_without_ga4_property', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('ORGANIC_WITHOUT_GA4_PROPERTY','23514',array['performance_dataset_rows_v2_source_semantics_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('ORGANIC_WITHOUT_GA4_PROPERTY','23514',array['performance_dataset_rows_v2_source_semantics_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_source_semantics_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('ORGANIC_WITHOUT_GA4_PROPERTY','23514',array['performance_dataset_rows_v2_source_semantics_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'klaviyo', 'paid', 'klaviyo', null, 'e2_t5_rejection_v2_klaviyo_paid_with_null_channel_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_klaviyo_paid_with_null_channel_root', 'E2 T5 Root', null, null, null, 'campaign_message', 'e2_t5_rejection_v2_klaviyo_paid_with_null_channel_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:klaviyo_paid_with_null_channel', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('KLAVIYO_PAID_WITH_NULL_CHANNEL','23514',array['performance_dataset_rows_v2_source_semantics_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('KLAVIYO_PAID_WITH_NULL_CHANNEL','23514',array['performance_dataset_rows_v2_source_semantics_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_source_semantics_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('KLAVIYO_PAID_WITH_NULL_CHANNEL','23514',array['performance_dataset_rows_v2_source_semantics_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'klaviyo', 'paid', 'meta_ads', 'email', 'e2_t5_rejection_v2_klaviyo_paid_with_invalid_source_pair_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_klaviyo_paid_with_invalid_source_pair_root', 'E2 T5 Root', null, null, null, 'campaign_message', 'e2_t5_rejection_v2_klaviyo_paid_with_invalid_source_pair_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:klaviyo_paid_with_invalid_source_pair', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('KLAVIYO_PAID_WITH_INVALID_SOURCE_PAIR','23514',array['performance_dataset_rows_v2_source_semantics_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('KLAVIYO_PAID_WITH_INVALID_SOURCE_PAIR','23514',array['performance_dataset_rows_v2_source_semantics_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_source_semantics_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('KLAVIYO_PAID_WITH_INVALID_SOURCE_PAIR','23514',array['performance_dataset_rows_v2_source_semantics_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_meta_with_adgroup_parent_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_meta_with_adgroup_parent_root', 'E2 T5 Root', 'adgroup', 'e2_t5_rejection_v2_meta_with_adgroup_parent_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_meta_with_adgroup_parent_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:meta_with_adgroup_parent', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('META_WITH_ADGROUP_PARENT','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('META_WITH_ADGROUP_PARENT','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_hierarchy_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('META_WITH_ADGROUP_PARENT','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'google', 'paid', 'google_ads', null, 'e2_t5_rejection_v2_google_standard_without_adgroup_account', '2026-08-26', 'standard', 'campaign', 'e2_t5_rejection_v2_google_standard_without_adgroup_root', 'E2 T5 Root', null, null, null, 'ad', 'e2_t5_rejection_v2_google_standard_without_adgroup_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:google_standard_without_adgroup', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('GOOGLE_STANDARD_WITHOUT_ADGROUP','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('GOOGLE_STANDARD_WITHOUT_ADGROUP','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_hierarchy_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('GOOGLE_STANDARD_WITHOUT_ADGROUP','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'google', 'paid', 'google_ads', null, 'e2_t5_rejection_v2_google_pmax_with_fake_parent_account', '2026-08-26', 'performance_max', 'campaign', 'e2_t5_rejection_v2_google_pmax_with_fake_parent_root', 'E2 T5 Root', 'adgroup', 'e2_t5_rejection_v2_google_pmax_with_fake_parent_parent', 'E2 T5 Parent', 'asset_group', 'e2_t5_rejection_v2_google_pmax_with_fake_parent_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:google_pmax_with_fake_parent', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('GOOGLE_PMAX_WITH_FAKE_PARENT','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('GOOGLE_PMAX_WITH_FAKE_PARENT','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_hierarchy_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('GOOGLE_PMAX_WITH_FAKE_PARENT','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'tiktok', 'paid', 'tiktok_ads', null, 'e2_t5_rejection_v2_tiktok_with_adset_parent_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_tiktok_with_adset_parent_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_tiktok_with_adset_parent_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_tiktok_with_adset_parent_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:tiktok_with_adset_parent', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('TIKTOK_WITH_ADSET_PARENT','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('TIKTOK_WITH_ADSET_PARENT','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_hierarchy_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('TIKTOK_WITH_ADSET_PARENT','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'klaviyo', 'paid', 'klaviyo', 'email', 'e2_t5_rejection_v2_klaviyo_campaign_with_parent_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_klaviyo_campaign_with_parent_root', 'E2 T5 Root', 'campaign', 'e2_t5_rejection_v2_klaviyo_campaign_with_parent_parent', 'E2 T5 Parent', 'campaign_message', 'e2_t5_rejection_v2_klaviyo_campaign_with_parent_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:klaviyo_campaign_with_parent', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('KLAVIYO_CAMPAIGN_WITH_PARENT','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('KLAVIYO_CAMPAIGN_WITH_PARENT','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_hierarchy_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('KLAVIYO_CAMPAIGN_WITH_PARENT','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'klaviyo', 'paid', 'klaviyo', 'email', 'e2_t5_rejection_v2_klaviyo_flow_as_campaign_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_klaviyo_flow_as_campaign_root', 'E2 T5 Root', null, null, null, 'flow_message', 'e2_t5_rejection_v2_klaviyo_flow_as_campaign_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:klaviyo_flow_as_campaign', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('KLAVIYO_FLOW_AS_CAMPAIGN','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('KLAVIYO_FLOW_AS_CAMPAIGN','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_hierarchy_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('KLAVIYO_FLOW_AS_CAMPAIGN','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'organic', 'ga4', null, 'e2_t5_rejection_v2_organic_with_paid_hierarchy_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_organic_with_paid_hierarchy_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_organic_with_paid_hierarchy_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_organic_with_paid_hierarchy_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:organic_with_paid_hierarchy', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, 'e2_t5_property', null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('ORGANIC_WITH_PAID_HIERARCHY','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('ORGANIC_WITH_PAID_HIERARCHY','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_hierarchy_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('ORGANIC_WITH_PAID_HIERARCHY','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_missing_support_key_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_missing_support_key_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_missing_support_key_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_missing_support_key_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:missing_support_key', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('MISSING_SUPPORT_KEY','23514',array['performance_dataset_rows_v2_metric_support_keys_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('MISSING_SUPPORT_KEY','23514',array['performance_dataset_rows_v2_metric_support_keys_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_metric_support_keys_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('MISSING_SUPPORT_KEY','23514',array['performance_dataset_rows_v2_metric_support_keys_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_invalid_support_enum_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_invalid_support_enum_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_invalid_support_enum_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_invalid_support_enum_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:invalid_support_enum', '{"impression":"invalid","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('INVALID_SUPPORT_ENUM','23514',array['performance_dataset_rows_v2_metric_support_keys_chk','performance_dataset_rows_v2_metric_value_support_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_SUPPORT_ENUM','23514',array['performance_dataset_rows_v2_metric_support_keys_chk','performance_dataset_rows_v2_metric_value_support_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_metric_support_keys_chk','performance_dataset_rows_v2_metric_value_support_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('INVALID_SUPPORT_ENUM','23514',array['performance_dataset_rows_v2_metric_support_keys_chk','performance_dataset_rows_v2_metric_value_support_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_supported_metric_is_null_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_supported_metric_is_null_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_supported_metric_is_null_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_supported_metric_is_null_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:supported_metric_is_null', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, null, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('SUPPORTED_METRIC_IS_NULL','23514',array['performance_dataset_rows_v2_metric_value_support_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('SUPPORTED_METRIC_IS_NULL','23514',array['performance_dataset_rows_v2_metric_value_support_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_metric_value_support_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('SUPPORTED_METRIC_IS_NULL','23514',array['performance_dataset_rows_v2_metric_value_support_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_unsupported_metric_is_non_null_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_unsupported_metric_is_non_null_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_unsupported_metric_is_non_null_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_unsupported_metric_is_non_null_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:unsupported_metric_is_non_null', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, 1, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('UNSUPPORTED_METRIC_IS_NON_NULL','23514',array['performance_dataset_rows_v2_metric_value_support_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('UNSUPPORTED_METRIC_IS_NON_NULL','23514',array['performance_dataset_rows_v2_metric_value_support_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_metric_value_support_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('UNSUPPORTED_METRIC_IS_NON_NULL','23514',array['performance_dataset_rows_v2_metric_value_support_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_unknown_metric_is_non_null_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_unknown_metric_is_non_null_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_unknown_metric_is_non_null_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_unknown_metric_is_non_null_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:unknown_metric_is_non_null', '{"impression":"supported","ad_click":"supported","session":"unknown","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, 1, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('UNKNOWN_METRIC_IS_NON_NULL','23514',array['performance_dataset_rows_v2_metric_value_support_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('UNKNOWN_METRIC_IS_NON_NULL','23514',array['performance_dataset_rows_v2_metric_value_support_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23514' and case when '23514'='23514' then nullif(v_constraint,'')=any(array['performance_dataset_rows_v2_metric_value_support_chk']::text[]) else nullif(v_column,'')=null end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('UNKNOWN_METRIC_IS_NON_NULL','23514',array['performance_dataset_rows_v2_metric_value_support_chk']::text[],null,v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_required_entity_id_null_account', '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_required_entity_id_null_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_required_entity_id_null_parent', 'E2 T5 Parent', 'ad', null, 'E2 T5 Entity', 'e2_t5_rejection_v2:required_entity_id_null', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('REQUIRED_ENTITY_ID_NULL','23502',array[]::text[],'entity_id',null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('REQUIRED_ENTITY_ID_NULL','23502',array[]::text[],'entity_id',v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23502' and case when '23502'='23514' then nullif(v_constraint,'')=any(array[]::text[]) else nullif(v_column,'')='entity_id' end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('REQUIRED_ENTITY_ID_NULL','23502',array[]::text[],'entity_id',v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, null, '2026-08-26', null, 'campaign', 'e2_t5_rejection_v2_required_platform_account_null_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_required_platform_account_null_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_required_platform_account_null_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:required_platform_account_null', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('REQUIRED_PLATFORM_ACCOUNT_NULL','23502',array[]::text[],'platform_account_id',null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('REQUIRED_PLATFORM_ACCOUNT_NULL','23502',array[]::text[],'platform_account_id',v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23502' and case when '23502'='23514' then nullif(v_constraint,'')=any(array[]::text[]) else nullif(v_column,'')='platform_account_id' end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('REQUIRED_PLATFORM_ACCOUNT_NULL','23502',array[]::text[],'platform_account_id',v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
    begin
      insert into public.performance_dataset_rows_v2 (user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw)
      values (v_user_id, 'meta', 'paid', 'meta_ads', null, 'e2_t5_rejection_v2_required_business_date_null_account', null, null, 'campaign', 'e2_t5_rejection_v2_required_business_date_null_root', 'E2 T5 Root', 'adset', 'e2_t5_rejection_v2_required_business_date_null_parent', 'E2 T5 Parent', 'ad', 'e2_t5_rejection_v2_required_business_date_null_entity', 'E2 T5 Entity', 'e2_t5_rejection_v2:required_business_date_null', '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb, 100, 10, null, 25, 0, 0, 2, 40, 1, 30, 'USD', 'TRY', 34.5, '2026-08-26', 'e2_t5_fixed_fx', 'v1', 'Europe/Istanbul', 'v1', 'v1', 'e2-t5-v2', 'real', false, null, null, '{"fixture_namespace":"e2_t5_rejection_v2"}'::jsonb);
      insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
      values ('REQUIRED_BUSINESS_DATE_NULL','23502',array[]::text[],'business_date',null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    exception
      when check_violation or not_null_violation then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('REQUIRED_BUSINESS_DATE_NULL','23502',array[]::text[],'business_date',v_state,nullif(v_constraint,''),nullif(v_column,''),true,
          v_state='23502' and case when '23502'='23514' then nullif(v_constraint,'')=any(array[]::text[]) else nullif(v_column,'')='business_date' end,
          v_dataset_before,v_v1_before,v_snapshots_before);
      when others then
        get stacked diagnostics v_state=returned_sqlstate,v_constraint=constraint_name,v_column=column_name;
        insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
        values ('REQUIRED_BUSINESS_DATE_NULL','23502',array[]::text[],'business_date',v_state,nullif(v_constraint,''),nullif(v_column,''),false,false,v_dataset_before,v_v1_before,v_snapshots_before);
    end;
  else
    insert into pg_temp.e2_t5_rejection_evidence(case_code,expected_sqlstate,expected_constraints,expected_column,actual_sqlstate,actual_constraint,actual_column,rejected,passed,dataset_before,v1_before,snapshots_before)
    values
      ('INVALID_PLATFORM','23514',array['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_platform_chk','performance_dataset_rows_v2_source_semantics_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('INVALID_TRAFFIC_TYPE','23514',array['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_source_semantics_chk','performance_dataset_rows_v2_traffic_type_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('INVALID_SOURCE_SYSTEM','23514',array['performance_dataset_rows_v2_source_semantics_chk','performance_dataset_rows_v2_source_system_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('INVALID_CHANNEL_ENUM','23514',array['performance_dataset_rows_v2_channel_chk','performance_dataset_rows_v2_source_semantics_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('INVALID_CAMPAIGN_TYPE','23514',array['performance_dataset_rows_v2_campaign_type_chk','performance_dataset_rows_v2_hierarchy_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('INVALID_ROOT_ENTITY_TYPE','23514',array['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_root_type_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('INVALID_PARENT_ENTITY_TYPE','23514',array['performance_dataset_rows_v2_hierarchy_chk','performance_dataset_rows_v2_parent_type_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('INVALID_ENTITY_TYPE','23514',array['performance_dataset_rows_v2_entity_type_chk','performance_dataset_rows_v2_hierarchy_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('INVALID_SOURCE_CONFIDENCE','23514',array['performance_dataset_rows_v2_source_confidence_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('INVALID_SOURCE_CURRENCY','23514',array['performance_dataset_rows_v2_source_currency_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('INVALID_TARGET_CURRENCY','23514',array['performance_dataset_rows_v2_target_currency_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('NON_POSITIVE_FX_RATE','23514',array['performance_dataset_rows_v2_fx_rate_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('METRIC_SUPPORT_NOT_OBJECT','23514',array['performance_dataset_rows_v2_metric_support_keys_chk','performance_dataset_rows_v2_metric_support_object_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('RAW_NOT_OBJECT','23514',array['performance_dataset_rows_v2_raw_object_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('SYNTHETIC_TRUE','23514',array['performance_dataset_rows_v2_synthetic_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('META_WRONG_SOURCE','23514',array['performance_dataset_rows_v2_source_semantics_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('PAID_WITH_GA4_PROPERTY','23514',array['performance_dataset_rows_v2_source_semantics_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('ORGANIC_WITHOUT_GA4_PROPERTY','23514',array['performance_dataset_rows_v2_source_semantics_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('KLAVIYO_PAID_WITH_NULL_CHANNEL','23514',array['performance_dataset_rows_v2_source_semantics_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('KLAVIYO_PAID_WITH_INVALID_SOURCE_PAIR','23514',array['performance_dataset_rows_v2_source_semantics_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('META_WITH_ADGROUP_PARENT','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('GOOGLE_STANDARD_WITHOUT_ADGROUP','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('GOOGLE_PMAX_WITH_FAKE_PARENT','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('TIKTOK_WITH_ADSET_PARENT','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('KLAVIYO_CAMPAIGN_WITH_PARENT','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('KLAVIYO_FLOW_AS_CAMPAIGN','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('ORGANIC_WITH_PAID_HIERARCHY','23514',array['performance_dataset_rows_v2_hierarchy_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('MISSING_SUPPORT_KEY','23514',array['performance_dataset_rows_v2_metric_support_keys_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('INVALID_SUPPORT_ENUM','23514',array['performance_dataset_rows_v2_metric_support_keys_chk','performance_dataset_rows_v2_metric_value_support_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('SUPPORTED_METRIC_IS_NULL','23514',array['performance_dataset_rows_v2_metric_value_support_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('UNSUPPORTED_METRIC_IS_NON_NULL','23514',array['performance_dataset_rows_v2_metric_value_support_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('UNKNOWN_METRIC_IS_NON_NULL','23514',array['performance_dataset_rows_v2_metric_value_support_chk']::text[],null,null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('REQUIRED_ENTITY_ID_NULL','23502',array[]::text[],'entity_id',null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('REQUIRED_PLATFORM_ACCOUNT_NULL','23502',array[]::text[],'platform_account_id',null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before),
      ('REQUIRED_BUSINESS_DATE_NULL','23502',array[]::text[],'business_date',null,null,null,false,false,v_dataset_before,v_v1_before,v_snapshots_before);
  end if;
end
$e2_t5$;
with summary as (
  select count(*) evaluated_case_count,count(*) filter(where passed) passed_case_count,
    count(*) filter(where not passed) failed_case_count,count(*) filter(where not rejected) unexpected_accept_count,
    coalesce(min(dataset_before),-1) dataset_before,coalesce(min(v1_before),-1) v1_before,
    coalesce(min(snapshots_before),-1) snapshots_before
  from pg_temp.e2_t5_rejection_evidence
), residue as (
  select count(*) residue_count from public.performance_dataset_rows_v2 where entity_key like 'e2\_t5\_rejection\_v2:%' escape '\'
), parity as (
  select
    (select count(*) from public.performance_dataset_rows_v2)=s.dataset_before dataset_unchanged,
    (select count(*) from public.performance_dataset_rows)=s.v1_before v1_unchanged,
    (select count(*) from public.dashboard_snapshots)=s.snapshots_before snapshots_unchanged,
    (select count(*) from public.oauth_transactions)=0 oauth_unchanged,
    (select count(*) from public.platform_connections where connected)=(select connected_count from pg_temp.e2_t5_security_baseline) connected_unchanged,
    (select count(*) from public.platform_connection_tokens)=(select encrypted_count from pg_temp.e2_t5_security_baseline) encrypted_unchanged,
    (select count(*) from public.platform_connections pc where pc.connected and not exists(select 1 from public.platform_connection_tokens pt where pt.user_id=pc.user_id and pt.platform=pc.platform))=0 missing_encrypted_unchanged,
    (select count(*) from public.platform_connection_tokens pt where not exists(select 1 from public.platform_connections pc where pc.user_id=pt.user_id and pc.platform=pt.platform and pc.connected))=0 orphan_encrypted_unchanged,
    (select count(*) from public.platform_connections where access_token is not null or refresh_token is not null)=0 plaintext_unchanged,
    (select count(*) from supabase_migrations.schema_migrations)=37 ledger_unchanged
  from summary s
), payload as (
  select jsonb_build_object(
    'operation_code','e2_t5_rejection_v2','expected_case_count',35,'evaluated_case_count',s.evaluated_case_count,
    'passed_case_count',s.passed_case_count,'failed_case_count',s.failed_case_count,
    'unexpected_accept_count',s.unexpected_accept_count,'residue_count',r.residue_count,
    'dataset_unchanged',p.dataset_unchanged,
    'v1_unchanged',p.v1_unchanged,
    'snapshots_unchanged',p.snapshots_unchanged,
    'oauth_unchanged',p.oauth_unchanged,
    'connected_unchanged',p.connected_unchanged,
    'encrypted_unchanged',p.encrypted_unchanged,
    'missing_encrypted_unchanged',p.missing_encrypted_unchanged,
    'orphan_encrypted_unchanged',p.orphan_encrypted_unchanged,
    'plaintext_unchanged',p.plaintext_unchanged,
    'ledger_unchanged',p.ledger_unchanged,
    'overall_passed',s.evaluated_case_count=35 and s.passed_case_count=35 and s.failed_case_count=0 and s.unexpected_accept_count=0 and r.residue_count=0
      and p.dataset_unchanged and p.v1_unchanged and p.snapshots_unchanged and p.oauth_unchanged
      and p.connected_unchanged and p.encrypted_unchanged and p.missing_encrypted_unchanged
      and p.orphan_encrypted_unchanged and p.plaintext_unchanged and p.ledger_unchanged,
    'cases',coalesce((select jsonb_agg(jsonb_build_object(
      'case_code',e.case_code,'expected_sqlstate',e.expected_sqlstate,'actual_sqlstate',e.actual_sqlstate,
      'expected_constraints',to_jsonb(e.expected_constraints),'actual_constraint',e.actual_constraint,
      'expected_column',e.expected_column,'actual_column',e.actual_column,'rejected',e.rejected,'passed',e.passed
    ) order by array_position(array['INVALID_PLATFORM','INVALID_TRAFFIC_TYPE','INVALID_SOURCE_SYSTEM','INVALID_CHANNEL_ENUM','INVALID_CAMPAIGN_TYPE','INVALID_ROOT_ENTITY_TYPE','INVALID_PARENT_ENTITY_TYPE','INVALID_ENTITY_TYPE','INVALID_SOURCE_CONFIDENCE','INVALID_SOURCE_CURRENCY','INVALID_TARGET_CURRENCY','NON_POSITIVE_FX_RATE','METRIC_SUPPORT_NOT_OBJECT','RAW_NOT_OBJECT','SYNTHETIC_TRUE','META_WRONG_SOURCE','PAID_WITH_GA4_PROPERTY','ORGANIC_WITHOUT_GA4_PROPERTY','KLAVIYO_PAID_WITH_NULL_CHANNEL','KLAVIYO_PAID_WITH_INVALID_SOURCE_PAIR','META_WITH_ADGROUP_PARENT','GOOGLE_STANDARD_WITHOUT_ADGROUP','GOOGLE_PMAX_WITH_FAKE_PARENT','TIKTOK_WITH_ADSET_PARENT','KLAVIYO_CAMPAIGN_WITH_PARENT','KLAVIYO_FLOW_AS_CAMPAIGN','ORGANIC_WITH_PAID_HIERARCHY','MISSING_SUPPORT_KEY','INVALID_SUPPORT_ENUM','SUPPORTED_METRIC_IS_NULL','UNSUPPORTED_METRIC_IS_NON_NULL','UNKNOWN_METRIC_IS_NON_NULL','REQUIRED_ENTITY_ID_NULL','REQUIRED_PLATFORM_ACCOUNT_NULL','REQUIRED_BUSINESS_DATE_NULL']::text[],e.case_code)) from pg_temp.e2_t5_rejection_evidence e),'[]'::jsonb)
  ) evidence from summary s cross join residue r cross join parity p
)
select evidence from payload;
rollback;
