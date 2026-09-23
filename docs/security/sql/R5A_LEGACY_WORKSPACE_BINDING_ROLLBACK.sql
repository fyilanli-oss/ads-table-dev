-- Safe only while the table is empty. R5-B/R5-C must not use this rollback after a binding exists.
do $$
begin
  if exists (select 1 from public.legacy_user_workspace_bindings) then
    raise exception 'R5A_ROLLBACK_BLOCKED_NONEMPTY_BINDINGS';
  end if;
end $$;

drop table public.legacy_user_workspace_bindings;
