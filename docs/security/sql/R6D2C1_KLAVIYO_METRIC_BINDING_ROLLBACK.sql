-- R6-D2-C1 rollback. Safe only before any canonical metric binding is written.
do $$
begin
  if exists (
    select 1
    from public.workspace_provider_connections
    where conversion_metric_id is not null
       or conversion_metric_name is not null
       or conversion_metric_integration_name is not null
       or conversion_metric_integration_category is not null
       or conversion_metric_verified_at is not null
  ) then
    raise exception 'R6D2C1_ROLLBACK_BLOCKED_METRIC_BINDINGS_EXIST';
  end if;
end
$$;

alter table public.workspace_provider_connections
  drop constraint if exists workspace_provider_conversion_metric_complete,
  drop column if exists conversion_metric_verified_at,
  drop column if exists conversion_metric_integration_category,
  drop column if exists conversion_metric_integration_name,
  drop column if exists conversion_metric_name,
  drop column if exists conversion_metric_id;
