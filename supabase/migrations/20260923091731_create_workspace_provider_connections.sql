-- R4-B additive production migration. Applied with explicit approval on 2026-09-23.
-- The connection belongs to AdsTable workspace; a commerce channel is only an authorization adapter.

create table public.workspace_provider_connections (
  workspace_id uuid not null references public.workspaces(id) on update restrict on delete restrict,
  provider text not null check (provider in ('meta', 'google_ads', 'klaviyo')),
  status text not null check (status in ('pending_account_selection', 'connected', 'disconnected', 'revoked')),
  active_account_id text,
  active_account_name text,
  source_currency text,
  monthly_plan_cost numeric(10,2),
  access_token_envelope jsonb,
  refresh_token_envelope jsonb,
  access_token_expires_at timestamptz,
  refresh_token_expires_at timestamptz,
  granted_scopes text[] not null default '{}',
  last_authorized_via text not null check (length(btrim(last_authorized_via)) > 0),
  account_verified_at timestamptz,
  connected_at timestamptz,
  disconnected_at timestamptz,
  connection_version bigint not null default 1 check (connection_version > 0),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (workspace_id, provider),
  constraint workspace_provider_source_currency_valid check (
    source_currency is null or source_currency ~ '^[A-Z]{3}$'
  ),
  constraint workspace_provider_plan_cost_valid check (
    monthly_plan_cost is null or
    (provider = 'klaviyo' and monthly_plan_cost >= 0 and monthly_plan_cost <= 99999999.99)
  ),
  constraint workspace_provider_access_envelope_shape check (
    access_token_envelope is null or (
      jsonb_typeof(access_token_envelope) = 'object'
      and access_token_envelope ?& array['version', 'keyId', 'iv', 'tag', 'ciphertext']
      and access_token_envelope - array['version', 'keyId', 'iv', 'tag', 'ciphertext'] = '{}'::jsonb
    )
  ),
  constraint workspace_provider_refresh_envelope_shape check (
    refresh_token_envelope is null or (
      jsonb_typeof(refresh_token_envelope) = 'object'
      and refresh_token_envelope ?& array['version', 'keyId', 'iv', 'tag', 'ciphertext']
      and refresh_token_envelope - array['version', 'keyId', 'iv', 'tag', 'ciphertext'] = '{}'::jsonb
    )
  ),
  constraint workspace_provider_pending_shape check (
    status <> 'pending_account_selection' or (
      access_token_envelope is not null
      and active_account_id is null
      and account_verified_at is null
      and connected_at is null
    )
  ),
  constraint workspace_provider_connected_shape check (
    status <> 'connected' or (
      access_token_envelope is not null
      and active_account_id is not null
      and source_currency is not null
      and account_verified_at is not null
      and connected_at is not null
      and disconnected_at is null
    )
  ),
  constraint workspace_provider_klaviyo_connected_cost check (
    provider <> 'klaviyo' or status <> 'connected' or monthly_plan_cost is not null
  ),
  constraint workspace_provider_non_klaviyo_cost_empty check (
    provider = 'klaviyo' or monthly_plan_cost is null
  ),
  constraint workspace_provider_timestamp_order check (updated_at >= created_at)
);

create index workspace_provider_connections_status_updated_idx
  on public.workspace_provider_connections (status, updated_at);

alter table public.workspace_provider_connections enable row level security;
alter table public.workspace_provider_connections force row level security;

revoke all on table public.workspace_provider_connections from public, anon, authenticated;
revoke all on table public.workspace_provider_connections from service_role;
grant select, insert, update, delete on table public.workspace_provider_connections to service_role;

comment on table public.workspace_provider_connections is
  'Canonical AdsTable data-source connection authority owned by workspace, independent of Shopify or any future commerce adapter.';
comment on column public.workspace_provider_connections.last_authorized_via is
  'Audit-only authorization adapter source; never connection ownership or tenant authority.';
