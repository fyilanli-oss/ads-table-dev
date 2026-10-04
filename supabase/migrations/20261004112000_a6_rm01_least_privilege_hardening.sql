begin;

-- A6-RM-01: remove unsafe implicit authority for future postgres-owned public objects.
-- supabase_admin is an internal provider role; project postgres cannot and must not impersonate it.
alter default privileges for role postgres in schema public
  revoke all privileges on tables from anon, authenticated;
alter default privileges for role postgres in schema public
  revoke all privileges on sequences from anon, authenticated;
alter default privileges for role postgres in schema public
  revoke execute on functions from public, anon, authenticated;


-- Remove non-Data-API privileges from the exact audited legacy inventory.
revoke truncate, references, trigger on table
  public.dashboard_snapshots,
  public.fx_rates,
  public.fx_rates_daily,
  public.insight_logs,
  public.performance_dataset_rows,
  public.platform_account_ownerships,
  public.platform_ad_accounts,
  public.platform_businesses,
  public.platform_connections,
  public.snapshot_jobs,
  public.snapshot_schedules,
  public.subscriptions,
  public.user_settings,
  public.users
from anon, authenticated;

-- Internal/trigger functions are not public RPC endpoints.
revoke all privileges on function public.expire_trials() from public, anon, authenticated;
revoke all privileges on function public.handle_new_user() from public, anon, authenticated;
revoke all privileges on function public.enforce_platform_account_limit_guard() from public, anon, authenticated;

-- Preserve the current server-side legacy trial call until E10-T7 consumer-zero.
grant execute on function public.expire_trials() to service_role;
grant execute on function public.handle_new_user() to service_role;
grant execute on function public.enforce_platform_account_limit_guard() to service_role;

-- All application relations referenced by the functions are already schema-qualified.
alter function public.expire_trials() set search_path = '';
alter function public.handle_new_user() set search_path = '';
alter function public.enforce_platform_account_limit_guard() set search_path = '';

commit;
