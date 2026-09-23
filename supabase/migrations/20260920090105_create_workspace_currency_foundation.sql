-- R2 repository migration. Production application requires a separate explicit approval.
-- The workspace is the canonical AdsTable tenant. Shopify is one installation adapter.

create table public.workspaces (
  id uuid primary key default gen_random_uuid(),
  status text not null default 'active'
    check (status in ('active', 'suspended', 'archived')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint workspaces_timestamp_order check (updated_at >= created_at)
);

create table public.workspace_settings (
  workspace_id uuid primary key references public.workspaces(id) on delete cascade,
  reporting_currency text not null
    check (reporting_currency ~ '^[A-Z]{3}$'),
  reporting_currency_source text not null default 'merchant_selected'
    check (reporting_currency_source = 'merchant_selected'),
  reporting_currency_version bigint not null default 1
    check (reporting_currency_version > 0),
  currency_configured_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint workspace_settings_timestamp_order check (
    updated_at >= created_at and currency_configured_at >= created_at
  )
);

-- Preserve workspace UUIDs already issued by the verified Shopify installation adapter.
-- No email, domain, actor, or provider-account matching is permitted here.
insert into public.workspaces (id, status, created_at, updated_at)
select
  installation.workspace_id,
  'active',
  min(installation.installed_at),
  max(installation.updated_at)
from public.shopify_installations as installation
group by installation.workspace_id
on conflict (id) do nothing;

alter table public.shopify_installations
  add constraint shopify_installations_workspace_fk
  foreign key (workspace_id) references public.workspaces(id)
  on update restrict on delete restrict
  not valid;

alter table public.shopify_installations
  validate constraint shopify_installations_workspace_fk;

alter table public.workspaces enable row level security;
alter table public.workspaces force row level security;
alter table public.workspace_settings enable row level security;
alter table public.workspace_settings force row level security;

revoke all on table public.workspaces from public, anon, authenticated;
revoke all on table public.workspace_settings from public, anon, authenticated;
revoke all on table public.workspaces from service_role;
revoke all on table public.workspace_settings from service_role;
grant select, insert, update, delete on table public.workspaces to service_role;
grant select, insert, update, delete on table public.workspace_settings to service_role;

comment on table public.workspaces is
  'Canonical AdsTable tenant registry. Commerce installations resolve to this workspace server-side.';
comment on table public.workspace_settings is
  'Server-authoritative workspace settings. Reporting currency is merchant-selected and never inferred from Shopify.';
comment on column public.workspace_settings.reporting_currency is
  'ISO-like three-letter reporting target selected explicitly by the merchant before Data Sources is available.';
comment on column public.workspace_settings.reporting_currency_source is
  'Must remain merchant_selected; Shopify store or presentment currency is not an authority.';
