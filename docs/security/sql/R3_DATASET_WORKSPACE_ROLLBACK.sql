-- R3-A rollback is allowed only before any workspace-bound Dataset V2 row exists.

begin;

do $$
begin
  if exists (
    select 1 from public.performance_dataset_rows_v2
    where workspace_id is not null
  ) then
    raise exception 'R3_ROLLBACK_BLOCKED_WORKSPACE_ROWS_EXIST';
  end if;
end;
$$;

drop index if exists public.performance_dataset_rows_v2_workspace_entity_history_idx;
drop index if exists public.performance_dataset_rows_v2_workspace_account_scope_date_idx;
drop index if exists public.performance_dataset_rows_v2_workspace_date_idx;
drop index if exists public.performance_dataset_rows_v2_workspace_canonical_uidx;

alter table public.performance_dataset_rows_v2
  drop constraint if exists performance_dataset_rows_v2_workspace_fk,
  drop column if exists workspace_id;

commit;
