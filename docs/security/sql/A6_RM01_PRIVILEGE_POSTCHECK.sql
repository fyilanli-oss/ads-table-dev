-- A6-RM-01 postcheck. PASS requires every returned violation_count to equal zero.
with audited_functions(signature) as (
  values
    ('public.expire_trials()'),
    ('public.handle_new_user()'),
    ('public.enforce_platform_account_limit_guard()')
),
function_external_execute as (
  select count(*)::integer as violation_count
  from audited_functions
  where has_function_privilege('anon', signature, 'EXECUTE')
     or has_function_privilege('authenticated', signature, 'EXECUTE')
),
service_role_trial_execute as (
  select case when has_function_privilege('service_role','public.expire_trials()','EXECUTE')
    then 0 else 1 end::integer as violation_count
),
legacy_non_dml as (
  select count(*)::integer as violation_count
  from information_schema.role_table_grants
  where table_schema='public'
    and table_name in (
      'dashboard_snapshots','fx_rates','fx_rates_daily','insight_logs',
      'performance_dataset_rows','platform_account_ownerships','platform_ad_accounts',
      'platform_businesses','platform_connections','snapshot_jobs','snapshot_schedules',
      'subscriptions','user_settings','users'
    )
    and grantee in ('anon','authenticated')
    and privilege_type in ('TRUNCATE','TRIGGER','REFERENCES')
),
mutable_search_path as (
  select count(*)::integer as violation_count
  from pg_proc p
  join pg_namespace n on n.oid=p.pronamespace
  where n.nspname='public'
    and p.proname in ('expire_trials','handle_new_user','enforce_platform_account_limit_guard')
    and not ('search_path=""' = any(coalesce(p.proconfig,array[]::text[])))
)
select 'function_external_execute' as gate, violation_count from function_external_execute
union all
select 'service_role_trial_execute', violation_count from service_role_trial_execute
union all
select 'legacy_non_dml', violation_count from legacy_non_dml
union all
select 'mutable_search_path', violation_count from mutable_search_path
order by gate;
