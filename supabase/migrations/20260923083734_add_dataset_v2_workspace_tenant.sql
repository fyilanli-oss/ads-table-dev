-- R3-A additive workspace tenant preparation for Dataset V2.
-- Applied to adstable-dev after explicit production approval on 2026-09-23.
-- Existing user_id columns and indexes remain compatibility-only until R10.

alter table public.performance_dataset_rows_v2
  add column workspace_id uuid;

alter table public.performance_dataset_rows_v2
  add constraint performance_dataset_rows_v2_workspace_fk
  foreign key (workspace_id) references public.workspaces(id)
  on update restrict on delete restrict
  not valid;

alter table public.performance_dataset_rows_v2
  validate constraint performance_dataset_rows_v2_workspace_fk;

create unique index performance_dataset_rows_v2_workspace_canonical_uidx
  on public.performance_dataset_rows_v2
  (workspace_id, platform, platform_account_id, business_date, traffic_type, entity_key)
  where workspace_id is not null;

create index performance_dataset_rows_v2_workspace_date_idx
  on public.performance_dataset_rows_v2
  (workspace_id, business_date)
  where workspace_id is not null;

create index performance_dataset_rows_v2_workspace_account_scope_date_idx
  on public.performance_dataset_rows_v2
  (workspace_id, platform, platform_account_id, traffic_type, business_date)
  where workspace_id is not null;

create index performance_dataset_rows_v2_workspace_entity_history_idx
  on public.performance_dataset_rows_v2
  (workspace_id, platform, platform_account_id, entity_key, business_date)
  where workspace_id is not null;

comment on column public.performance_dataset_rows_v2.workspace_id is
  'Canonical AdsTable tenant. Nullable only during the R3 staged transition; browser input is never workspace authority.';
