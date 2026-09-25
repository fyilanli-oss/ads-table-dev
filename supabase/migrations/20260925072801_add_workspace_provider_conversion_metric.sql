-- R6-D2-C1 additive workspace/account-scoped conversion metric binding.
-- This migration only adds nullable canonical metadata. It does not discover a
-- provider metric, mutate an existing connection, or activate Dataset V2.

alter table public.workspace_provider_connections
  add column conversion_metric_id text,
  add column conversion_metric_name text,
  add column conversion_metric_integration_name text,
  add column conversion_metric_integration_category text,
  add column conversion_metric_verified_at timestamptz;

alter table public.workspace_provider_connections
  add constraint workspace_provider_conversion_metric_complete check (
    (
      conversion_metric_id is null
      and conversion_metric_name is null
      and conversion_metric_integration_name is null
      and conversion_metric_integration_category is null
      and conversion_metric_verified_at is null
    )
    or (
      provider = 'klaviyo'
      and status = 'connected'
      and active_account_id is not null
      and length(btrim(conversion_metric_id)) > 0
      and length(btrim(conversion_metric_name)) > 0
      and lower(btrim(conversion_metric_name)) = 'placed order'
      and length(btrim(conversion_metric_integration_name)) > 0
      and (
        conversion_metric_integration_category is null
        or length(btrim(conversion_metric_integration_category)) > 0
      )
      and conversion_metric_verified_at is not null
    )
  );

comment on column public.workspace_provider_connections.conversion_metric_id is
  'Provider-verified conversion metric bound to this workspace/provider account; never a global environment setting.';
comment on column public.workspace_provider_connections.conversion_metric_integration_name is
  'Provider-reported integration provenance. The commerce authorization adapter is not inferred as the metric source.';
