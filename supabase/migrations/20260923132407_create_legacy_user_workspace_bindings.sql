-- R5-A additive foundation. It creates an explicit, server-only migration authority.
-- No binding is seeded: account ID, email, domain or a single remaining workspace are not tenant authority.

create table public.legacy_user_workspace_bindings (
  legacy_user_id uuid not null references public.users(id) on update restrict on delete restrict,
  workspace_id uuid not null references public.workspaces(id) on update restrict on delete restrict,
  status text not null default 'active' check (status in ('active', 'revoked')),
  evidence_type text not null check (evidence_type = 'human_attested'),
  evidence_reference text not null check (length(btrim(evidence_reference)) > 0),
  verified_at timestamptz not null,
  revoked_at timestamptz,
  binding_version bigint not null default 1 check (binding_version > 0),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (legacy_user_id, workspace_id),
  constraint legacy_user_workspace_binding_state check (
    (status = 'active' and revoked_at is null)
    or (status = 'revoked' and revoked_at is not null)
  ),
  constraint legacy_user_workspace_binding_timestamps check (
    updated_at >= created_at and verified_at <= updated_at
  )
);

create unique index legacy_user_workspace_one_active_idx
  on public.legacy_user_workspace_bindings (legacy_user_id)
  where status = 'active';

create index legacy_user_workspace_active_workspace_idx
  on public.legacy_user_workspace_bindings (workspace_id, legacy_user_id)
  where status = 'active';

alter table public.legacy_user_workspace_bindings enable row level security;
alter table public.legacy_user_workspace_bindings force row level security;

revoke all on table public.legacy_user_workspace_bindings from public, anon, authenticated;
revoke all on table public.legacy_user_workspace_bindings from service_role;
grant select, insert, update, delete on table public.legacy_user_workspace_bindings to service_role;

comment on table public.legacy_user_workspace_bindings is
  'Explicit human-attested authority for legacy user to canonical workspace migration; never inferred from email, domain or provider account ID.';
comment on column public.legacy_user_workspace_bindings.evidence_reference is
  'Non-secret audit reference for the human attestation; it must not contain tokens or provider credentials.';
