-- AdsTable PHASE 2 — Canonical Dataset V2 raw fact store
-- Parallel table only. Legacy snapshot and performance_dataset_rows V1 are intentionally untouched.

create table public.performance_dataset_rows_v2 (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.users(id),

  platform text not null,
  traffic_type text not null,
  source_system text not null,
  channel text null,
  platform_account_id text not null,
  business_date date not null,

  campaign_type text null,
  root_entity_type text not null,
  root_entity_id text not null,
  root_entity_name text null,
  parent_entity_type text null,
  parent_entity_id text null,
  parent_entity_name text null,
  entity_type text not null,
  entity_id text not null,
  entity_name text not null,
  entity_key text not null,

  metric_support jsonb not null,

  impressions numeric null,
  ad_clicks numeric null,
  sessions numeric null,
  spend numeric null,
  add_to_cart numeric null,
  add_to_cart_value numeric null,
  checkout numeric null,
  checkout_value numeric null,
  purchase numeric null,
  purchase_value numeric null,

  source_currency text not null,
  target_currency text not null,
  fx_rate numeric not null,
  fx_rate_date date not null,
  fx_provider text not null,
  fx_engine_version text not null,

  source_timezone text not null,
  time_engine_version text not null,

  canonical_contract_version text not null,
  adapter_version text not null,
  source_confidence text not null,
  synthetic boolean not null default false,
  ga4_property_id text null,
  source_job_id uuid null,
  raw jsonb not null default '{}'::jsonb,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  constraint performance_dataset_rows_v2_platform_chk
    check (platform in ('meta', 'google', 'tiktok', 'klaviyo')),
  constraint performance_dataset_rows_v2_traffic_type_chk
    check (traffic_type in ('paid', 'organic')),
  constraint performance_dataset_rows_v2_source_system_chk
    check (source_system in ('meta_ads', 'google_ads', 'tiktok_ads', 'klaviyo', 'ga4')),
  constraint performance_dataset_rows_v2_channel_chk
    check (channel is null or channel in ('email', 'sms')),
  constraint performance_dataset_rows_v2_campaign_type_chk
    check (campaign_type is null or campaign_type in ('standard', 'performance_max')),
  constraint performance_dataset_rows_v2_root_type_chk
    check (root_entity_type in ('campaign', 'flow', 'organic')),
  constraint performance_dataset_rows_v2_parent_type_chk
    check (parent_entity_type is null or parent_entity_type in ('adset', 'adgroup', 'campaign', 'flow')),
  constraint performance_dataset_rows_v2_entity_type_chk
    check (entity_type in ('ad', 'asset_group', 'campaign_message', 'flow_message', 'organic')),
  constraint performance_dataset_rows_v2_source_confidence_chk
    check (source_confidence in ('real', 'fallback', 'partial')),
  constraint performance_dataset_rows_v2_source_currency_chk
    check (source_currency ~ '^[A-Z]{3}$'),
  constraint performance_dataset_rows_v2_target_currency_chk
    check (target_currency ~ '^[A-Z]{3}$'),
  constraint performance_dataset_rows_v2_fx_rate_chk
    check (fx_rate > 0),
  constraint performance_dataset_rows_v2_metric_support_object_chk
    check (jsonb_typeof(metric_support) = 'object'),
  constraint performance_dataset_rows_v2_raw_object_chk
    check (jsonb_typeof(raw) = 'object'),
  constraint performance_dataset_rows_v2_synthetic_chk
    check (synthetic = false),

  -- Provider/source/channel semantics.
  constraint performance_dataset_rows_v2_source_semantics_chk
    check (
      (
        traffic_type = 'organic'
        and source_system = 'ga4'
        and channel is null
        and ga4_property_id is not null
      )
      or
      (
        traffic_type = 'paid'
        and ga4_property_id is null
        and (
          (platform = 'meta' and source_system = 'meta_ads' and channel is null)
          or (platform = 'google' and source_system = 'google_ads' and channel is null)
          or (platform = 'tiktok' and source_system = 'tiktok_ads' and channel is null)
          or (platform = 'klaviyo' and source_system = 'klaviyo' and channel in ('email', 'sms'))
        )
      )
    ),

  -- Capability-aware leaf hierarchy. Missing provider levels are not invented.
  constraint performance_dataset_rows_v2_hierarchy_chk
    check (
      (
        traffic_type = 'organic'
        and campaign_type is null
        and root_entity_type = 'organic'
        and parent_entity_type is null
        and parent_entity_id is null
        and parent_entity_name is null
        and entity_type = 'organic'
      )
      or
      (
        traffic_type = 'paid'
        and platform = 'meta'
        and campaign_type is null
        and root_entity_type = 'campaign'
        and parent_entity_type = 'adset'
        and parent_entity_id is not null
        and entity_type = 'ad'
      )
      or
      (
        traffic_type = 'paid'
        and platform = 'google'
        and campaign_type = 'standard'
        and root_entity_type = 'campaign'
        and parent_entity_type = 'adgroup'
        and parent_entity_id is not null
        and entity_type = 'ad'
      )
      or
      (
        traffic_type = 'paid'
        and platform = 'google'
        and campaign_type = 'performance_max'
        and root_entity_type = 'campaign'
        and parent_entity_type is null
        and parent_entity_id is null
        and parent_entity_name is null
        and entity_type = 'asset_group'
      )
      or
      (
        traffic_type = 'paid'
        and platform = 'tiktok'
        and campaign_type is null
        and root_entity_type = 'campaign'
        and parent_entity_type = 'adgroup'
        and parent_entity_id is not null
        and entity_type = 'ad'
      )
      or
      (
        traffic_type = 'paid'
        and platform = 'klaviyo'
        and campaign_type is null
        and root_entity_type = 'campaign'
        and parent_entity_type is null
        and parent_entity_id is null
        and parent_entity_name is null
        and entity_type = 'campaign_message'
      )
      or
      (
        traffic_type = 'paid'
        and platform = 'klaviyo'
        and campaign_type is null
        and root_entity_type = 'flow'
        and parent_entity_type is null
        and parent_entity_id is null
        and parent_entity_name is null
        and entity_type = 'flow_message'
      )
    ),

  -- Every support key must exist and carry a valid enum.
  constraint performance_dataset_rows_v2_metric_support_keys_chk
    check (
      metric_support ?& array[
        'impression','ad_click','session','spend_value','add_to_cart',
        'add_to_cart_value','checkout','checkout_value','purchase','purchase_value'
      ]
      and (metric_support->>'impression') in ('supported','unsupported','unknown')
      and (metric_support->>'ad_click') in ('supported','unsupported','unknown')
      and (metric_support->>'session') in ('supported','unsupported','unknown')
      and (metric_support->>'spend_value') in ('supported','unsupported','unknown')
      and (metric_support->>'add_to_cart') in ('supported','unsupported','unknown')
      and (metric_support->>'add_to_cart_value') in ('supported','unsupported','unknown')
      and (metric_support->>'checkout') in ('supported','unsupported','unknown')
      and (metric_support->>'checkout_value') in ('supported','unsupported','unknown')
      and (metric_support->>'purchase') in ('supported','unsupported','unknown')
      and (metric_support->>'purchase_value') in ('supported','unsupported','unknown')
    ),

  -- measured zero is valid; unsupported/unknown must remain NULL.
  constraint performance_dataset_rows_v2_metric_value_support_chk
    check (
      (((metric_support->>'impression') = 'supported' and impressions is not null) or ((metric_support->>'impression') in ('unsupported','unknown') and impressions is null))
      and (((metric_support->>'ad_click') = 'supported' and ad_clicks is not null) or ((metric_support->>'ad_click') in ('unsupported','unknown') and ad_clicks is null))
      and (((metric_support->>'session') = 'supported' and sessions is not null) or ((metric_support->>'session') in ('unsupported','unknown') and sessions is null))
      and (((metric_support->>'spend_value') = 'supported' and spend is not null) or ((metric_support->>'spend_value') in ('unsupported','unknown') and spend is null))
      and (((metric_support->>'add_to_cart') = 'supported' and add_to_cart is not null) or ((metric_support->>'add_to_cart') in ('unsupported','unknown') and add_to_cart is null))
      and (((metric_support->>'add_to_cart_value') = 'supported' and add_to_cart_value is not null) or ((metric_support->>'add_to_cart_value') in ('unsupported','unknown') and add_to_cart_value is null))
      and (((metric_support->>'checkout') = 'supported' and checkout is not null) or ((metric_support->>'checkout') in ('unsupported','unknown') and checkout is null))
      and (((metric_support->>'checkout_value') = 'supported' and checkout_value is not null) or ((metric_support->>'checkout_value') in ('unsupported','unknown') and checkout_value is null))
      and (((metric_support->>'purchase') = 'supported' and purchase is not null) or ((metric_support->>'purchase') in ('unsupported','unknown') and purchase is null))
      and (((metric_support->>'purchase_value') = 'supported' and purchase_value is not null) or ((metric_support->>'purchase_value') in ('unsupported','unknown') and purchase_value is null))
    )
);

create unique index performance_dataset_rows_v2_canonical_uidx
  on public.performance_dataset_rows_v2
  (user_id, platform, platform_account_id, business_date, traffic_type, entity_key);

create index performance_dataset_rows_v2_user_date_idx
  on public.performance_dataset_rows_v2
  (user_id, business_date);

create index performance_dataset_rows_v2_account_scope_date_idx
  on public.performance_dataset_rows_v2
  (user_id, platform, platform_account_id, traffic_type, business_date);

create index performance_dataset_rows_v2_entity_history_idx
  on public.performance_dataset_rows_v2
  (user_id, platform, platform_account_id, entity_key, business_date);

alter table public.performance_dataset_rows_v2 enable row level security;

create policy performance_dataset_rows_v2_select_own
  on public.performance_dataset_rows_v2
  for select
  to authenticated
  using ((select auth.uid()) = user_id);

revoke all on table public.performance_dataset_rows_v2 from anon, authenticated;
grant select on table public.performance_dataset_rows_v2 to authenticated;
grant all on table public.performance_dataset_rows_v2 to service_role;
