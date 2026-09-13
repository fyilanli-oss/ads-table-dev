-- E10-T6-C2A: encrypted provider connections owned by a verified Shopify workspace.
create table public.shopify_workspace_provider_connections (
  workspace_id uuid not null,
  shop_id text not null,
  provider text not null check (provider in ('meta', 'google_ads', 'klaviyo', 'tiktok', 'pinterest')),
  status text not null check (status in ('pending_account_selection', 'connected', 'revoked')),
  active_account_id text,
  access_token_envelope jsonb not null,
  refresh_token_envelope jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (workspace_id, provider),
  foreign key (shop_id, workspace_id)
    references public.shopify_installations (shop_id, workspace_id) on delete cascade,
  check ((status = 'connected' and active_account_id is not null) or (status <> 'connected')),
  check (jsonb_typeof(access_token_envelope) = 'object'
    and access_token_envelope ?& array['version', 'keyId', 'iv', 'tag', 'ciphertext']),
  check (refresh_token_envelope is null or (jsonb_typeof(refresh_token_envelope) = 'object'
    and refresh_token_envelope ?& array['version', 'keyId', 'iv', 'tag', 'ciphertext']))
);

alter table public.shopify_workspace_provider_connections enable row level security;
alter table public.shopify_workspace_provider_connections force row level security;
revoke all on table public.shopify_workspace_provider_connections from public, anon, authenticated;
grant select, insert, update, delete on table public.shopify_workspace_provider_connections to service_role;
