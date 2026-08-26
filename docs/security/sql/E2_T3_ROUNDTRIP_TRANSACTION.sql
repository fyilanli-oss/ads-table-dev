-- Controlled E2-T3 v2 operation. Send as one intact payload after a new approved preflight. Never remove ROLLBACK.
begin;
lock table public.performance_dataset_rows_v2 in share row exclusive mode;
lock table public.performance_dataset_rows, public.dashboard_snapshots, public.oauth_transactions,
  public.platform_connections, public.platform_connection_tokens in share mode;

create temp table pg_temp.e2_t3_v2_baseline on commit drop as
select
  (select count(*) from public.performance_dataset_rows_v2) dataset_before,
  (select count(*) from public.performance_dataset_rows_v2 where entity_key='meta:e2_t3_static_v2_account:paid:none:campaign:e2_t3_static_v2_campaign:ad:e2_t3_static_v2_ad') fixture_before,
  (select count(*) from public.performance_dataset_rows) v1_before,
  (select count(*) from public.dashboard_snapshots) snapshot_before,
  (select count(*) from public.oauth_transactions) oauth_before,
  (select count(*) from public.platform_connections where connected) connected_before,
  (select count(*) from public.platform_connection_tokens) encrypted_before,
  (select count(*) from supabase_migrations.schema_migrations) ledger_before,
  (select count(*) from public.users u where exists (select 1 from auth.users a where a.id=u.id))=1 eligible_user_ok,
  (select count(*) from public.platform_connections pc where pc.connected and not exists (
    select 1 from public.platform_connection_tokens pt where pt.user_id=pc.user_id and pt.platform=pc.platform))=0 missing_ok,
  (select count(*) from public.platform_connection_tokens pt where not exists (
    select 1 from public.platform_connections pc where pc.user_id=pt.user_id and pc.platform=pt.platform and pc.connected))=0 orphan_ok,
  (select count(*) from public.platform_connections where access_token is not null or refresh_token is not null)=0 plaintext_ok;

insert into public.performance_dataset_rows_v2 (
  user_id,platform,traffic_type,source_system,channel,platform_account_id,business_date,
  campaign_type,root_entity_type,root_entity_id,root_entity_name,parent_entity_type,parent_entity_id,parent_entity_name,
  entity_type,entity_id,entity_name,entity_key,metric_support,impressions,ad_clicks,sessions,spend,
  add_to_cart,add_to_cart_value,checkout,checkout_value,purchase,purchase_value,source_currency,target_currency,
  fx_rate,fx_rate_date,fx_provider,fx_engine_version,source_timezone,time_engine_version,canonical_contract_version,
  adapter_version,source_confidence,synthetic,ga4_property_id,source_job_id,raw
)
select u.id,'meta','paid','meta_ads',null,'e2_t3_static_v2_account','2026-08-24',
  null,'campaign','e2_t3_static_v2_campaign','E2 T3 Campaign','adset','e2_t3_static_v2_adset','E2 T3 AdSet',
  'ad','e2_t3_static_v2_ad','E2 T3 Ad','meta:e2_t3_static_v2_account:paid:none:campaign:e2_t3_static_v2_campaign:ad:e2_t3_static_v2_ad',
  '{"impression":"supported","ad_click":"supported","session":"unsupported","spend_value":"supported","add_to_cart":"supported","add_to_cart_value":"supported","checkout":"supported","checkout_value":"supported","purchase":"supported","purchase_value":"supported"}'::jsonb,
  1250,125,null,250.5,0,0,8,360,4,240,'USD','TRY',34.25,'2026-08-24','e2_t3_fixed_fx','v1',
  'Europe/Istanbul','v1','v1','e2-t3-meta-v2','real',false,null,null,
  '{"fixture_namespace":"e2_t3_static_v2","source":"repository_acceptance"}'::jsonb
from (select u.id from public.users u where exists (select 1 from auth.users a where a.id=u.id) order by u.id limit 1) u
cross join pg_temp.e2_t3_v2_baseline b
where b.ledger_before=37 and b.dataset_before=0 and b.fixture_before=0 and b.eligible_user_ok
  and b.oauth_before=0 and b.connected_before=b.encrypted_before and b.missing_ok and b.orphan_ok and b.plaintext_ok;

with fixture as (
  select d.* from public.performance_dataset_rows_v2 d
  where d.entity_key='meta:e2_t3_static_v2_account:paid:none:campaign:e2_t3_static_v2_campaign:ad:e2_t3_static_v2_ad'
), evidence as (
  select count(*) read_back_count,
    count(*) filter (where platform='meta' and traffic_type='paid' and source_system='meta_ads' and channel is null
      and platform_account_id='e2_t3_static_v2_account' and campaign_type is null
      and root_entity_type='campaign' and root_entity_id='e2_t3_static_v2_campaign'
      and parent_entity_type='adset' and parent_entity_id='e2_t3_static_v2_adset'
      and entity_type='ad' and entity_id='e2_t3_static_v2_ad'
      and metric_support->>'session'='unsupported' and sessions is null
      and metric_support->>'add_to_cart'='supported' and add_to_cart=0
      and impressions=1250 and spend=250.5 and fx_rate=34.25 and source_currency='USD' and target_currency='TRY'
      and business_date='2026-08-24' and source_timezone='Europe/Istanbul' and source_confidence='real'
      and synthetic=false and canonical_contract_version='v1' and adapter_version='e2-t3-meta-v2'
      and raw->>'fixture_namespace'='e2_t3_static_v2') contract_match_count,
    coalesce(jsonb_agg(to_jsonb(fixture) - 'user_id' - 'updated_at' order by entity_key),'[]'::jsonb) redacted_physical
  from fixture
), result as (
  select b.*, e.*,
    e.read_back_count-b.fixture_before inserted_count,
    (select count(*) from public.performance_dataset_rows_v2)=b.dataset_before+1 dataset_unchanged,
    (select count(*) from public.performance_dataset_rows)=b.v1_before v1_unchanged,
    (select count(*) from public.dashboard_snapshots)=b.snapshot_before snapshot_unchanged,
    (select count(*) from public.oauth_transactions)=b.oauth_before oauth_unchanged,
    (select count(*) from public.platform_connections where connected)=b.connected_before connected_unchanged,
    (select count(*) from public.platform_connection_tokens)=b.encrypted_before encrypted_unchanged,
    (select count(*) from public.platform_connections pc where pc.connected and not exists (
      select 1 from public.platform_connection_tokens pt where pt.user_id=pc.user_id and pt.platform=pc.platform))=0 missing_encrypted_unchanged,
    (select count(*) from public.platform_connection_tokens pt where not exists (
      select 1 from public.platform_connections pc where pc.user_id=pt.user_id and pc.platform=pt.platform and pc.connected))=0 orphan_encrypted_unchanged,
    (select count(*) from public.platform_connections where access_token is not null or refresh_token is not null)=0 plaintext_unchanged,
    (select count(*) from supabase_migrations.schema_migrations)=b.ledger_before ledger_unchanged
  from pg_temp.e2_t3_v2_baseline b cross join evidence e
)
select 'E2_T3_TRANSACTION_V2' operation_code, inserted_count, contract_match_count, read_back_count,
  dataset_unchanged, v1_unchanged, snapshot_unchanged, oauth_unchanged, connected_unchanged, encrypted_unchanged,
  missing_encrypted_unchanged, orphan_encrypted_unchanged, plaintext_unchanged, ledger_unchanged,
  fixture_before=0 and inserted_count=1 and read_back_count=1 and contract_match_count=1
    and ledger_before=37 and dataset_before=0 and eligible_user_ok and oauth_before=0
    and connected_before=encrypted_before and missing_ok and orphan_ok and plaintext_ok
    and dataset_unchanged and v1_unchanged and snapshot_unchanged and oauth_unchanged
    and connected_unchanged and encrypted_unchanged and missing_encrypted_unchanged
    and orphan_encrypted_unchanged and plaintext_unchanged and ledger_unchanged as passed,
  redacted_physical
from result;
rollback;
