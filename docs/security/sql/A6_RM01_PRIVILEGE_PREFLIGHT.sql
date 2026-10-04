-- A6-RM-01 read-only privilege preflight. No customer rows or secret values are selected.
with audited_functions(signature) as (
  values
    ('public.expire_trials()'),
    ('public.handle_new_user()'),
    ('public.enforce_platform_account_limit_guard()')
)
select
  signature,
  has_function_privilege('anon', signature, 'EXECUTE') as anon_execute,
  has_function_privilege('authenticated', signature, 'EXECUTE') as authenticated_execute,
  has_function_privilege('service_role', signature, 'EXECUTE') as service_role_execute
from audited_functions
order by signature;

select
  grantee,
  privilege_type,
  count(*)::integer as object_count
from information_schema.role_table_grants
where table_schema='public'
  and grantee in ('anon','authenticated')
  and privilege_type in ('TRUNCATE','TRIGGER','REFERENCES')
group by grantee, privilege_type
order by grantee, privilege_type;

select
  defaclrole::regrole::text as owner,
  defaclobjtype as object_type,
  defaclacl::text as acl
from pg_default_acl
where defaclnamespace='public'::regnamespace
  and defaclrole in (
    'postgres'::regrole,
    'supabase_admin'::regrole
  )
order by owner, object_type;
