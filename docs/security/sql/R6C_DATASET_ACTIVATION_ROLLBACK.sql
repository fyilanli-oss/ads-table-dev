-- R6-C controlled rollback. Run only with separate production approval.

do $$
begin
  if exists (
    select 1
    from public.performance_dataset_rows_v2
    where user_id is null
  ) then
    raise exception 'R6C_ROLLBACK_BLOCKED_NULL_USER_ROWS_EXIST';
  end if;
end;
$$;

alter table public.performance_dataset_rows_v2
  alter column user_id set not null;

comment on column public.performance_dataset_rows_v2.user_id is
  'Legacy user tenant retained during the staged workspace migration.';

