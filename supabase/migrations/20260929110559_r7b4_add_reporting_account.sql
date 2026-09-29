-- R7-B4 separates the connected account set from the single reporting preference.
-- The preference remains workspace/provider-owned and survives disconnect for historical reporting.

alter table public.workspace_provider_connections
  add column reporting_account_id text,
  add column reporting_account_name text,
  add column reporting_account_selected_at timestamptz;

update public.workspace_provider_connections
set
  reporting_account_id = active_account_id,
  reporting_account_name = coalesce(active_account_name, active_account_id),
  reporting_account_selected_at = coalesce(account_verified_at, updated_at)
where provider in ('meta', 'google_ads')
  and status = 'connected'
  and active_account_id is not null;

alter table public.workspace_provider_connections
  add constraint workspace_provider_reporting_account_shape check (
    (
      provider = 'klaviyo'
      and reporting_account_id is null
      and reporting_account_name is null
      and reporting_account_selected_at is null
    )
    or
    (
      provider in ('meta', 'google_ads')
      and (
        (
          status = 'connected'
          and reporting_account_id is not null
          and length(btrim(reporting_account_id)) > 0
          and reporting_account_name is not null
          and length(btrim(reporting_account_name)) > 0
          and reporting_account_selected_at is not null
          and selected_accounts @> jsonb_build_array(
            jsonb_build_object(
              'id', reporting_account_id,
              'name', reporting_account_name
            )
          )
        )
        or
        (
          status <> 'connected'
          and (
            (
              reporting_account_id is null
              and reporting_account_name is null
              and reporting_account_selected_at is null
            )
            or
            (
              reporting_account_id is not null
              and length(btrim(reporting_account_id)) > 0
              and reporting_account_name is not null
              and length(btrim(reporting_account_name)) > 0
              and reporting_account_selected_at is not null
            )
          )
        )
      )
    )
  );

comment on column public.workspace_provider_connections.reporting_account_id is
  'Single Meta or Google Ads account selected for reporting views; separate from the full connected selected_accounts set and preserved after disconnect.';
comment on column public.workspace_provider_connections.reporting_account_name is
  'Provider-verified display name captured with reporting_account_id.';
comment on column public.workspace_provider_connections.reporting_account_selected_at is
  'Timestamp of the latest explicit or connection-default reporting account choice.';
