-- R3-C1: enforce workspace tenancy before the first Dataset V2 fact is written.
-- No provider contact, row backfill, Dataset insert, or production runtime activation occurs here.

do $$
begin
  if exists (
    select 1
    from public.performance_dataset_rows_v2
    where workspace_id is null
  ) then
    raise exception 'R3C1_BLOCKED_UNBOUND_DATASET_ROWS';
  end if;
end;
$$;

alter table public.performance_dataset_rows_v2
  alter column workspace_id set not null;

drop policy if exists performance_dataset_rows_v2_select_own
  on public.performance_dataset_rows_v2;

revoke select on table public.performance_dataset_rows_v2 from authenticated;

drop index if exists public.performance_dataset_rows_v2_entity_history_idx;
drop index if exists public.performance_dataset_rows_v2_account_scope_date_idx;
drop index if exists public.performance_dataset_rows_v2_user_date_idx;
drop index if exists public.performance_dataset_rows_v2_canonical_uidx;

comment on column public.performance_dataset_rows_v2.workspace_id is
  'Required canonical AdsTable tenant. It is resolved by trusted server authority and never accepted from browser input.';

comment on column public.performance_dataset_rows_v2.user_id is
  'Optional non-authoritative compatibility actor. workspace_id is always the canonical tenant.';
