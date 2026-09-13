-- DB-LEDGER-T2 read-only acceptance. Supply captured preflight counts in the runbook evidence.
with targets(version, name) as (
  values
    ('20260818090000'::text, 'create_oauth_transactions'::text),
    ('20260818120000', 'create_platform_connection_tokens'),
    ('20260819120000', 'harden_platform_connection_tokens_service_role_grants'),
    ('20260824120000', 'harden_oauth_transactions_service_role_grants')
), checks(check_code, actual_count, expected_count) as (
  select 'TOTAL_LEDGER_ROWS', count(*), 37 from supabase_migrations.schema_migrations
  union all select 'NON_TARGET_LEDGER_ROWS', count(*), 33 from supabase_migrations.schema_migrations m where not exists (select 1 from targets t where t.version=m.version)
  union all select 'TARGET_LEDGER_EXACT', count(*), 4 from supabase_migrations.schema_migrations m join targets t using(version) where m.name=t.name
  union all select 'TARGET_VERSION_DUPLICATES', count(*), 0 from (select m.version from supabase_migrations.schema_migrations m join targets t using(version) group by m.version having count(*)<>1) d
  union all select 'OAUTH_SERVICE_PRIVILEGES', count(*), 3 from information_schema.role_table_grants where table_schema='public' and table_name='oauth_transactions' and grantee='service_role' and privilege_type in ('SELECT','INSERT','DELETE')
  union all select 'OAUTH_SERVICE_EXTRA_PRIVILEGES', count(*), 0 from information_schema.role_table_grants where table_schema='public' and table_name='oauth_transactions' and grantee='service_role' and privilege_type not in ('SELECT','INSERT','DELETE')
  union all select 'OAUTH_NON_SERVICE_PRIVILEGES', count(*), 0 from information_schema.role_table_grants where table_schema='public' and table_name='oauth_transactions' and grantee in ('PUBLIC','anon','authenticated')
  union all select 'OAUTH_FUNCTION_SERVICE_EXECUTE', count(*), 2 from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid=p.pronamespace where n.nspname='public' and p.proname in ('consume_oauth_transaction','cleanup_expired_oauth_transactions') and pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE')
  union all select 'OAUTH_FUNCTION_UNEXPECTED_EXECUTE', count(*), 0 from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid=p.pronamespace cross join (values ('public'),('anon'),('authenticated')) r(role_name) where n.nspname='public' and p.proname in ('consume_oauth_transaction','cleanup_expired_oauth_transactions') and pg_catalog.has_function_privilege(r.role_name,p.oid,'EXECUTE')
  union all select 'OAUTH_SCHEMA_STATE', count(*), 1 from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='oauth_transactions' and c.relrowsecurity and not c.relforcerowsecurity
  union all select 'TOKEN_SCHEMA_STATE', count(*), 1 from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='platform_connection_tokens' and c.relrowsecurity and c.relforcerowsecurity
  union all select 'TOKEN_SERVICE_PRIVILEGES', count(*), 4 from information_schema.role_table_grants where table_schema='public' and table_name='platform_connection_tokens' and grantee='service_role' and privilege_type in ('SELECT','INSERT','UPDATE','DELETE')
  union all select 'TOKEN_SERVICE_EXTRA_PRIVILEGES', count(*), 0 from information_schema.role_table_grants where table_schema='public' and table_name='platform_connection_tokens' and grantee='service_role' and privilege_type not in ('SELECT','INSERT','UPDATE','DELETE')
  union all select 'OAUTH_ROWS', count(*), 0 from public.oauth_transactions
  union all select 'CONNECTED_CONNECTIONS', count(*), 7 from public.platform_connections where connected=true
  union all select 'ENCRYPTED_TOKEN_ROWS', count(*), 7 from public.platform_connection_tokens
  union all select 'MISSING_ENCRYPTED', count(*), 0 from public.platform_connections pc where pc.connected=true and not exists (select 1 from public.platform_connection_tokens pt where pt.user_id=pc.user_id and pt.platform=pc.platform)
  union all select 'PLAINTEXT_ACCESS', count(*), 0 from public.platform_connections where access_token is not null
  union all select 'PLAINTEXT_REFRESH', count(*), 0 from public.platform_connections where refresh_token is not null
)
select check_code, actual_count, expected_count, actual_count=expected_count as passed
from checks order by check_code;
