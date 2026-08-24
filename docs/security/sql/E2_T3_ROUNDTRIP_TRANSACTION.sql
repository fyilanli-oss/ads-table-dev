-- Controlled E2-T3 operation. Execute once only after approved preflight. Never remove ROLLBACK.
begin;
lock table public.performance_dataset_rows_v2 in row exclusive mode;
with constants as (
  select 'meta:e2_t3_static_v1_account:paid:none:campaign:e2_t3_static_v1_campaign:ad:e2_t3_static_v1_ad'::text entity_key
), eligible_user as (
  select u.id from public.users u where exists (select 1 from auth.users a where a.id=u.id) order by u.id limit 1
), baseline as (
  select
    (select count(*) from supabase_migrations.schema_migrations)=37 ledger_ok,
    (select count(*) from public.performance_dataset_rows_v2)=0 dataset_ok,
    (select count(*) from public.performance_dataset_rows_v2 d cross join constants k where d.entity_key=k.entity_key)=0 fixture_absent,
    (select count(*) from eligible_user)=1 user_ok,
    (select count(*) from public.oauth_transactions)=0 oauth_ok,
    (select count(*) from public.platform_connections where connected)=7 connected_ok,
    (select count(*) from public.platform_connection_tokens)=7 encrypted_ok,
    (select count(*) from public.performance_dataset_rows) v1_before,
    (select count(*) from public.dashboard_snapshots) snapshot_before
), inserted as (
  insert into public.performance_dataset_rows_v2 (
    user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,
    campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,
    entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,
    add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,
    fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,
    adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw
  )
  select u.id,'meta','paid','meta_ads',null,'e2_t3_static_v1_account','2026-08-24',
    null,'campaign','e2_t3_static_v1_campaign','E2 T3 Campaign','adset','e2_t3_static_v1_adset','E2 T3 AdSet',
    'ad','e2_t3_static_v1_ad','E2 T3 Ad',k.entity_key,
    '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb,
    1250,125,null,250.5,0,0,8,360,4,240,'USD','TRY',34.25,'2026-08-24','e2_t3_fixed_fx','v1',
    'Europe/Istanbul','v1','v1','e2-t3-meta-v1','real',false,null,null,
    '{"fixture_namespace":"e2_t3_static_v1","source":"repository_acceptance"}'::jsonb
  from eligible_user u cross join constants k cross join baseline b
  where b.ledger_ok and b.dataset_ok and b.fixture_absent and b.user_ok and b.oauth_ok and b.connected_ok and b.encrypted_ok
  returning platform,traffic_type,source_system,channel,platform_account_id,business_date,campaign_type,
    root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,
    entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,
    add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,
    fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,
    adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw
), evidence as (
  select count(*) inserted_count,
    count(*) filter (where platform='meta' and traffic_type='paid' and source_system='meta_ads' and channel is null
      and campaign_type is null and root_entity_type='campaign' and parent_entity_type='adset' and entity_type='ad'
      and metric_support->>'session'='unsupported' and sessions is null
      and metric_support->>'add_to_cart'='supported' and add_to_cart=0
      and impressions=1250 and spend=250.5 and fx_rate=34.25 and source_currency='USD' and target_currency='TRY'
      and business_date='2026-08-24' and source_timezone='Europe/Istanbul' and source_confidence='real'
      and synthetic=false and canonical_contract_version='v1' and adapter_version='e2-t3-meta-v1') contract_match_count,
    coalesce(jsonb_agg(to_jsonb(inserted) order by entity_key),'[]'::jsonb) redacted_physical
  from inserted
), parity as (
  select b.*, (select count(*) from public.performance_dataset_rows)=b.v1_before v1_unchanged,
    (select count(*) from public.dashboard_snapshots)=b.snapshot_before snapshot_unchanged,
    (select count(*) from public.oauth_transactions)=0 oauth_unchanged,
    (select count(*) from public.platform_connection_tokens)=7 tokens_unchanged
  from baseline b
)
select 'E2_T3_TRANSACTION' operation_code, e.inserted_count, e.contract_match_count,
  (select count(*) from public.performance_dataset_rows_v2 d cross join constants k where d.entity_key=k.entity_key) read_back_count,
  p.v1_unchanged, p.snapshot_unchanged, p.oauth_unchanged, p.tokens_unchanged,
  e.inserted_count=1 and e.contract_match_count=1
    and (select count(*) from public.performance_dataset_rows_v2 d cross join constants k where d.entity_key=k.entity_key)=1
    and p.v1_unchanged and p.snapshot_unchanged and p.oauth_unchanged and p.tokens_unchanged as passed,
  e.redacted_physical
from evidence e cross join parity p;
rollback;
