-- PHASE 2 live acceptance correction:
-- PostgreSQL CHECK accepts NULL expressions, so Paid Klaviyo must explicitly require a non-null channel.

alter table public.performance_dataset_rows_v2
  drop constraint performance_dataset_rows_v2_source_semantics_chk;

alter table public.performance_dataset_rows_v2
  add constraint performance_dataset_rows_v2_source_semantics_chk
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
        or (platform = 'klaviyo' and source_system = 'klaviyo' and channel is not null and channel in ('email', 'sms'))
      )
    )
  );
