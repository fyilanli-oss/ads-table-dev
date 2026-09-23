-- R4-B rollback is allowed only while the canonical table is empty and no runtime consumes it.
do $$
begin
  if to_regclass('public.workspace_provider_connections') is null then
    return;
  end if;
  if exists (select 1 from public.workspace_provider_connections) then
    raise exception 'R4_ROLLBACK_BLOCKED_CANONICAL_CONNECTION_ROWS_EXIST';
  end if;
end $$;

drop table if exists public.workspace_provider_connections;
