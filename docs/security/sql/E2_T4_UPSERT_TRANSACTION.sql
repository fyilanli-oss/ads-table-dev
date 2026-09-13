-- E2_T4_TRANSACTION_V2 CONTROLLED PAYLOAD: one final evidence SELECT, mandatory final ROLLBACK; COMMIT/retry forbidden.
begin;
lock table public.performance_dataset_rows_v2 in share row exclusive mode;
lock table public.performance_dataset_rows, public.dashboard_snapshots, public.oauth_transactions, public.platform_connections, public.platform_connection_tokens in share mode;
create temp table pg_temp.e2_t4_security_baseline as select
  (select count(*) from public.platform_connections where connected) connected_count,
  (select count(*) from public.platform_connection_tokens) encrypted_count;
with constants as (
  select 'meta:e2_t4_same_key_v2_account:paid:none:campaign:e2_t4_same_key_v2_campaign:ad:e2_t4_same_key_v2_ad'::text entity_key
), eligible_user as (
  select u.id from public.users u where exists(select 1 from auth.users a where a.id=u.id) order by u.id limit 1
), gates as (
  select (select count(*) from supabase_migrations.schema_migrations)=37 ledger_ok,
    (select count(*) from public.performance_dataset_rows_v2 d cross join constants k where d.entity_key=k.entity_key)=0 fixture_absent,
    (select count(*) from eligible_user)=1 user_ok,
    (select count(*) from public.oauth_transactions)=0 oauth_ok,
    (select connected_count=encrypted_count from pg_temp.e2_t4_security_baseline) population_parity_ok,
    (select count(*) from public.platform_connections pc where pc.connected and not exists(select 1 from public.platform_connection_tokens pt where pt.user_id=pc.user_id and pt.platform=pc.platform))=0 missing_ok,
    (select count(*) from public.platform_connection_tokens pt where not exists(select 1 from public.platform_connections pc where pc.user_id=pt.user_id and pc.platform=pt.platform and pc.connected))=0 orphan_ok,
    (select count(*) from public.platform_connections where access_token is not null or refresh_token is not null)=0 plaintext_ok,
    (select count(*) from public.performance_dataset_rows_v2) dataset_before,
    (select count(*) from public.performance_dataset_rows) v1_before,
    (select count(*) from public.dashboard_snapshots) snapshot_before
)
  insert into public.performance_dataset_rows_v2 (
    user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,
    root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,
    entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,
    add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,
    fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,
    adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw
  )
  select u.id,'meta','paid','meta_ads',null,'e2_t4_same_key_v2_account','2026-08-25',null,
    'campaign','e2_t4_same_key_v2_campaign','E2 T4 Campaign','adset','e2_t4_same_key_v2_adset','E2 T4 AdSet',
    'ad','e2_t4_same_key_v2_ad','E2 T4 Ad',k.entity_key,
    '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb,
    100,10,null,25,0,0,2,40,1,30,'USD','TRY',34.5,'2026-08-25','e2_t4_fixed_fx','v1',
    'Europe/Istanbul','v1','v1','e2-t4-meta-v2-initial','real',false,null,null,
    jsonb_build_object(
      'fixture_namespace','e2_t4_same_key_v2',
      'revision','initial',
      'transaction_marker',pg_current_xact_id()::text
    )
  from eligible_user u cross join constants k cross join gates g
  where g.ledger_ok and g.fixture_absent and g.user_ok and g.oauth_ok and g.population_parity_ok and g.missing_ok and g.orphan_ok and g.plaintext_ok
;
with constants as (
  select 'meta:e2_t4_same_key_v2_account:paid:none:campaign:e2_t4_same_key_v2_campaign:ad:e2_t4_same_key_v2_ad'::text entity_key
), eligible_user as (
  select u.id from public.users u where exists(select 1 from auth.users a where a.id=u.id) order by u.id limit 1
)
  insert into public.performance_dataset_rows_v2 (
    user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,
    root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,
    entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,
    add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,
    fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,
    adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw
  )
  select u.id,'meta','paid','meta_ads',null,'e2_t4_same_key_v2_account','2026-08-25',null,
    'campaign','e2_t4_same_key_v2_campaign','E2 T4 Campaign','adset','e2_t4_same_key_v2_adset','E2 T4 AdSet',
    'ad','e2_t4_same_key_v2_ad','E2 T4 Ad',k.entity_key,
    '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb,
    240,24,null,55.5,0,0,5,110,3,90,'USD','TRY',34.5,'2026-08-25','e2_t4_fixed_fx','v1',
    'Europe/Istanbul','v1','v1','e2-t4-meta-v2-updated','real',false,null,null,
    '{"fixture_namespace":"e2_t4_same_key_v2","revision":"updated"}'::jsonb
  from eligible_user u cross join constants k
  where (select count(*) from public.performance_dataset_rows_v2 d where d.user_id=u.id and d.entity_key=k.entity_key
    and d.impressions=100 and d.sessions is null and d.add_to_cart=0
    and d.adapter_version='e2-t4-meta-v2-initial'
    and d.raw->>'revision'='initial'
    and d.raw->>'transaction_marker'=pg_current_xact_id()::text)=1
  on conflict (user_id,platform,platform_account_id,business_date,traffic_type,entity_key) do update set
    metric_support=excluded.metric_support,impressions=excluded.impressions,ad_clicks=excluded.ad_clicks,
    sessions=excluded.sessions,spend=excluded.spend,add_to_cart=excluded.add_to_cart,
    add_to_cart_value=excluded.add_to_cart_value,checkout=excluded.checkout,checkout_value=excluded.checkout_value,
    purchase=excluded.purchase,purchase_value=excluded.purchase_value,source_currency=excluded.source_currency,
    target_currency=excluded.target_currency,fx_rate=excluded.fx_rate,fx_rate_date=excluded.fx_rate_date,
    fx_provider=excluded.fx_provider,fx_engine_version=excluded.fx_engine_version,source_timezone=excluded.source_timezone,
    time_engine_version=excluded.time_engine_version,canonical_contract_version=excluded.canonical_contract_version,
    adapter_version=excluded.adapter_version,source_confidence=excluded.source_confidence,synthetic=excluded.synthetic,
    ga4_property_id=excluded.ga4_property_id,source_job_id=excluded.source_job_id,raw=excluded.raw,updated_at=now()
;
with constants as (
  select 'meta:e2_t4_same_key_v2_account:paid:none:campaign:e2_t4_same_key_v2_campaign:ad:e2_t4_same_key_v2_ad'::text entity_key
), fixture as (
  select d.* from public.performance_dataset_rows_v2 d cross join constants k where d.entity_key=k.entity_key
), duplicate_groups as (
  select user_id,platform,platform_account_id,business_date,traffic_type,entity_key,count(*) row_count
  from fixture group by user_id,platform,platform_account_id,business_date,traffic_type,entity_key
), state as (
  select (select count(*) from fixture) fixture_row_count,
    (select count(*) from duplicate_groups where row_count>1) duplicate_group_count,
    (select coalesce(sum((row_count-1)::bigint),0::bigint)::bigint from duplicate_groups where row_count>1) duplicate_excess_row_count,
    (select count(*) from fixture where impressions=240 and ad_clicks=24 and spend=55.5 and checkout=5 and purchase=3
      and sessions is null and metric_support->>'session'='unsupported'
      and add_to_cart=0 and metric_support->>'add_to_cart'='supported'
      and adapter_version='e2-t4-meta-v2-updated' and raw->>'revision'='updated') updated_contract_match_count
)
select
  case when s.updated_contract_match_count=1 and s.fixture_row_count=1 then 1 else 0 end initial_operation_count,
  case when s.updated_contract_match_count=1 and s.fixture_row_count=1 then 1 else 0 end upsert_operation_count,
  s.fixture_row_count, s.duplicate_group_count, s.duplicate_excess_row_count::bigint as duplicate_excess_row_count, s.updated_contract_match_count,
  (select count(*) from public.performance_dataset_rows_v2)-s.fixture_row_count dataset_before,
  (select count(*) from public.performance_dataset_rows) v1_before,
  (select count(*) from public.performance_dataset_rows) v1_after,
  (select count(*) from public.dashboard_snapshots) snapshot_before,
  (select count(*) from public.dashboard_snapshots) snapshot_after,
  (select count(*)=1 from fixture where platform='meta' and traffic_type='paid' and source_system='meta_ads'
    and platform_account_id='e2_t4_same_key_v2_account' and business_date='2026-08-25' and channel is null) identity_unchanged,
  (select count(*)=1 from fixture where root_entity_type='campaign' and root_entity_id='e2_t4_same_key_v2_campaign'
    and parent_entity_type='adset' and parent_entity_id='e2_t4_same_key_v2_adset'
    and entity_type='ad' and entity_id='e2_t4_same_key_v2_ad') hierarchy_unchanged,
  (select count(*)=1 from fixture where sessions is null and metric_support->>'session'='unsupported') unsupported_null_preserved,
  (select count(*)=1 from fixture where add_to_cart=0 and metric_support->>'add_to_cart'='supported') supported_zero_preserved,
  (select count(*)=0 from public.oauth_transactions) oauth_unchanged,
  (select count(*)=(select connected_count from pg_temp.e2_t4_security_baseline) from public.platform_connections where connected) connected_unchanged,
  (select count(*)=(select encrypted_count from pg_temp.e2_t4_security_baseline) from public.platform_connection_tokens) encrypted_unchanged,
  (select count(*)=0 from public.platform_connections pc where pc.connected and not exists(select 1 from public.platform_connection_tokens pt where pt.user_id=pc.user_id and pt.platform=pc.platform)) missing_encrypted_unchanged,
  (select count(*)=0 from public.platform_connection_tokens pt where not exists(select 1 from public.platform_connections pc where pc.user_id=pt.user_id and pc.platform=pt.platform and pc.connected)) orphan_encrypted_unchanged,
  (select count(*)=0 from public.platform_connections where access_token is not null or refresh_token is not null) plaintext_unchanged
from state s;
rollback;
