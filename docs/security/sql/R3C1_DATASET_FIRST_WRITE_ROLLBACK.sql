-- R3-C1 rollback is allowed only while Dataset V2 remains empty.

begin;

do $$
begin
  if exists (select 1 from public.performance_dataset_rows_v2) then
    raise exception 'R3C1_ROLLBACK_BLOCKED_DATASET_ROWS_EXIST';
  end if;
end;
$$;

alter table public.performance_dataset_rows_v2
  alter column workspace_id drop not null;

create unique index if not exists performance_dataset_rows_v2_canonical_uidx
  on public.performance_dataset_rows_v2
  (user_id, platform, platform_account_id, business_date, traffic_type, entity_key);

create index if not exists performance_dataset_rows_v2_user_date_idx
  on public.performance_dataset_rows_v2 (user_id, business_date);

create index if not exists performance_dataset_rows_v2_account_scope_date_idx
  on public.performance_dataset_rows_v2
  (user_id, platform, platform_account_id, traffic_type, business_date);

create index if not exists performance_dataset_rows_v2_entity_history_idx
  on public.performance_dataset_rows_v2
  (user_id, platform, platform_account_id, entity_key, business_date);

grant select on table public.performance_dataset_rows_v2 to authenticated;

create policy performance_dataset_rows_v2_select_own
  on public.performance_dataset_rows_v2
  for select
  to authenticated
  using ((select auth.uid()) = user_id);

comment on column public.performance_dataset_rows_v2.workspace_id is
  'Canonical AdsTable tenant. Nullable only during the R3 staged transition; browser input is never workspace authority.';

comment on column public.performance_dataset_rows_v2.user_id is
  'Optional compatibility actor during the workspace-first transition. workspace_id is the canonical tenant; final legacy retirement remains gated by R6-D and R3-C.';

commit;
