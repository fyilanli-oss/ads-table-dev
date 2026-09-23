-- R6-C: prepare Dataset V2 for a workspace-first writer.
-- This migration does not activate a provider, write facts, enforce workspace_id
-- NOT NULL, or retire the legacy user-scoped indexes and SELECT policy.

do $$
begin
  if exists (
    select 1
    from public.performance_dataset_rows_v2
    where workspace_id is null
  ) then
    raise exception 'R6C_ACTIVATION_BLOCKED_UNBOUND_DATASET_ROWS';
  end if;
end;
$$;

alter table public.performance_dataset_rows_v2
  alter column user_id drop not null;

comment on column public.performance_dataset_rows_v2.user_id is
  'Optional compatibility actor during the workspace-first transition. workspace_id is the canonical tenant; final legacy retirement remains gated by R6-D and R3-C.';

