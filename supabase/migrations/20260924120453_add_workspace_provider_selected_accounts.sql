-- R7 account-cardinality correction.
-- One workspace/provider connection owns one OAuth grant; the verified accounts
-- selected under that grant are stored atomically on the connection row.

alter table public.workspace_provider_connections
  add column selected_accounts jsonb not null default '[]'::jsonb;

update public.workspace_provider_connections
set selected_accounts = jsonb_build_array(jsonb_build_object(
  'id', active_account_id,
  'name', coalesce(active_account_name, active_account_id),
  'currency', source_currency
))
where status = 'connected'
  and active_account_id is not null
  and source_currency is not null;

alter table public.workspace_provider_connections
  add constraint workspace_provider_selected_accounts_shape check (
    jsonb_typeof(selected_accounts) = 'array'
    and jsonb_array_length(selected_accounts) <= 3
  ),
  add constraint workspace_provider_pending_accounts_empty check (
    status <> 'pending_account_selection' or jsonb_array_length(selected_accounts) = 0
  ),
  add constraint workspace_provider_connected_account_count check (
    status <> 'connected' or (
      (provider in ('meta', 'google_ads') and jsonb_array_length(selected_accounts) between 1 and 3)
      or (provider = 'klaviyo' and jsonb_array_length(selected_accounts) = 1)
    )
  );

comment on column public.workspace_provider_connections.selected_accounts is
  'Server-verified provider accounts selected under this OAuth grant: 1-3 for Meta/Google Ads and exactly 1 for Klaviyo.';
