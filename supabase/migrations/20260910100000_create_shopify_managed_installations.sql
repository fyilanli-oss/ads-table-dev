-- E10-T6-B: server-only Shopify managed-install binding and encrypted token storage.
create table if not exists public.shopify_installations (
  shop_id text primary key check (length(btrim(shop_id)) > 0),
  shop_domain text not null unique check (shop_domain ~ '^[a-z0-9][a-z0-9-]*\.myshopify\.com$'),
  workspace_id uuid not null unique default gen_random_uuid(),
  shopify_user_id text not null check (length(btrim(shopify_user_id)) > 0),
  status text not null default 'active' check (status in ('active', 'revoked', 'uninstalled', 'redacted')),
  install_generation integer not null default 1 check (install_generation > 0),
  access_token_envelope jsonb not null,
  refresh_token_envelope jsonb,
  access_token_expires_at timestamptz not null,
  refresh_token_expires_at timestamptz,
  granted_scopes text[] not null default '{}',
  installed_at timestamptz not null default now(),
  last_verified_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint shopify_access_envelope_shape check (
    jsonb_typeof(access_token_envelope) = 'object'
    and access_token_envelope ?& array['version', 'keyId', 'iv', 'tag', 'ciphertext']
    and access_token_envelope - array['version', 'keyId', 'iv', 'tag', 'ciphertext'] = '{}'::jsonb
  ),
  constraint shopify_refresh_envelope_shape check (
    refresh_token_envelope is null or (
      jsonb_typeof(refresh_token_envelope) = 'object'
      and refresh_token_envelope ?& array['version', 'keyId', 'iv', 'tag', 'ciphertext']
      and refresh_token_envelope - array['version', 'keyId', 'iv', 'tag', 'ciphertext'] = '{}'::jsonb
    )
  )
);

alter table public.shopify_installations enable row level security;
alter table public.shopify_installations force row level security;
revoke all on table public.shopify_installations from public, anon, authenticated;
grant select, insert, update, delete on table public.shopify_installations to service_role;

create or replace function public.complete_shopify_managed_install(
  p_shop_id text,
  p_shop_domain text,
  p_shopify_user_id text,
  p_access_token_envelope jsonb,
  p_refresh_token_envelope jsonb,
  p_access_token_expires_at timestamptz,
  p_refresh_token_expires_at timestamptz,
  p_granted_scopes text[]
)
returns table (shop_id text, shop_domain text, workspace_id uuid, status text)
language plpgsql
security invoker
set search_path = public, pg_temp
as $$
begin
  if p_shop_id is null or btrim(p_shop_id) = ''
    or p_shop_domain !~ '^[a-z0-9][a-z0-9-]*\.myshopify\.com$'
    or p_shopify_user_id is null or btrim(p_shopify_user_id) = ''
    or p_access_token_envelope is null
    or p_access_token_expires_at <= now() then
    raise exception 'INVALID_SHOPIFY_MANAGED_INSTALL';
  end if;

  return query
  insert into public.shopify_installations as installation (
    shop_id, shop_domain, shopify_user_id, status,
    access_token_envelope, refresh_token_envelope,
    access_token_expires_at, refresh_token_expires_at, granted_scopes
  ) values (
    p_shop_id, p_shop_domain, p_shopify_user_id, 'active',
    p_access_token_envelope, p_refresh_token_envelope,
    p_access_token_expires_at, p_refresh_token_expires_at, coalesce(p_granted_scopes, '{}')
  )
  on conflict on constraint shopify_installations_pkey do update set
    shop_domain = excluded.shop_domain,
    shopify_user_id = excluded.shopify_user_id,
    status = 'active',
    access_token_envelope = excluded.access_token_envelope,
    refresh_token_envelope = excluded.refresh_token_envelope,
    access_token_expires_at = excluded.access_token_expires_at,
    refresh_token_expires_at = excluded.refresh_token_expires_at,
    granted_scopes = excluded.granted_scopes,
    last_verified_at = now(),
    updated_at = now()
  returning installation.shop_id, installation.shop_domain, installation.workspace_id, installation.status;
end;
$$;

revoke all on function public.complete_shopify_managed_install(text, text, text, jsonb, jsonb, timestamptz, timestamptz, text[]) from public, anon, authenticated;
grant execute on function public.complete_shopify_managed_install(text, text, text, jsonb, jsonb, timestamptz, timestamptz, text[]) to service_role;

comment on table public.shopify_installations is
  'Server-only Shopify shop/workspace bindings and AES-256-GCM token envelopes; plaintext tokens are forbidden.';
