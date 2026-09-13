-- DB-LEDGER-T2 read-only preflight. Run separately; every result must be PASS.
with targets(version, name) as (
  values
    ('20260818090000'::text, 'create_oauth_transactions'::text),
    ('20260818120000', 'create_platform_connection_tokens'),
    ('20260819120000', 'harden_platform_connection_tokens_service_role_grants'),
    ('20260824120000', 'harden_oauth_transactions_service_role_grants')
), target_ledger as (
  select t.version, t.name expected_name, m.name live_name
  from targets t left join supabase_migrations.schema_migrations m using (version)
), expected_oauth_columns(name, type_name, nullable, default_value) as (
  values
    ('state_hash','text','NO',null::text), ('user_id','uuid','NO',null),
    ('provider','text','NO',null), ('redirect_uri','text','NO',null),
    ('pkce_verifier','text','YES',null), ('created_at','timestamp with time zone','NO','now()'),
    ('expires_at','timestamp with time zone','NO',null)
), expected_token_columns(name, type_name, nullable, default_value) as (
  values
    ('user_id','uuid','NO',null::text), ('platform','text','NO',null),
    ('access_token_envelope','jsonb','YES',null), ('refresh_token_envelope','jsonb','YES',null),
    ('created_at','timestamp with time zone','NO','now()'), ('updated_at','timestamp with time zone','NO','now()')
), checks(check_code, actual_count, expected_count) as (
  select 'TOTAL_LEDGER_ROWS', count(*), 33 from supabase_migrations.schema_migrations
  union all select 'TARGET_LEDGER_ROWS', count(*) filter (where live_name is not null), 0 from target_ledger
  union all select 'TARGET_VERSION_WRONG_NAME', count(*) filter (where live_name is not null and live_name <> expected_name), 0 from target_ledger
  union all select 'OAUTH_TABLE', count(*), 1 from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='oauth_transactions' and c.relkind='r'
  union all select 'TOKEN_TABLE', count(*), 1 from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='platform_connection_tokens' and c.relkind='r'
  union all select 'OAUTH_COLUMN_DRIFT', count(*), 0 from expected_oauth_columns e full join (select * from information_schema.columns where table_schema='public' and table_name='oauth_transactions') c on c.column_name=e.name where e.name is null or c.column_name is null or c.data_type<>e.type_name or c.is_nullable<>e.nullable or c.column_default is distinct from e.default_value
  union all select 'TOKEN_COLUMN_DRIFT', count(*), 0 from expected_token_columns e full join (select * from information_schema.columns where table_schema='public' and table_name='platform_connection_tokens') c on c.column_name=e.name where e.name is null or c.column_name is null or c.data_type<>e.type_name or c.is_nullable<>e.nullable or c.column_default is distinct from e.default_value
  union all select 'OAUTH_CONSTRAINTS', count(*), 5 from pg_catalog.pg_constraint x join pg_catalog.pg_class c on c.oid=x.conrelid join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='oauth_transactions' and x.convalidated
  union all select 'TOKEN_CONSTRAINTS', count(*), 6 from pg_catalog.pg_constraint x join pg_catalog.pg_class c on c.oid=x.conrelid join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='platform_connection_tokens' and x.convalidated
  union all select 'OAUTH_INDEXES_VALID', count(*), 2 from pg_catalog.pg_index i join pg_catalog.pg_class c on c.oid=i.indrelid join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='oauth_transactions' and i.indisvalid and i.indisready
  union all select 'OAUTH_FUNCTION_SIGNATURES', count(*), 2 from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid=p.pronamespace where n.nspname='public' and ((p.proname='consume_oauth_transaction' and pg_catalog.pg_get_function_identity_arguments(p.oid)='p_state_hash text, p_provider text, p_redirect_uri text') or (p.proname='cleanup_expired_oauth_transactions' and pg_catalog.pg_get_function_identity_arguments(p.oid)=''))
  union all select 'OAUTH_FUNCTION_OVERLOADS', count(*), 2 from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid=p.pronamespace where n.nspname='public' and p.proname in ('consume_oauth_transaction','cleanup_expired_oauth_transactions')
  union all select 'OAUTH_RLS_ENABLED', count(*), 1 from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='oauth_transactions' and c.relrowsecurity and not c.relforcerowsecurity
  union all select 'TOKEN_RLS_FORCED', count(*), 1 from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='platform_connection_tokens' and c.relrowsecurity and c.relforcerowsecurity
  union all select 'OAUTH_NON_SERVICE_PRIVILEGES', count(*), 0 from information_schema.role_table_grants where table_schema='public' and table_name='oauth_transactions' and grantee in ('PUBLIC','anon','authenticated')
  union all select 'OAUTH_SERVICE_PRIVILEGES', count(*), 7 from information_schema.role_table_grants where table_schema='public' and table_name='oauth_transactions' and grantee='service_role' and privilege_type in ('SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER')
  union all select 'OAUTH_SERVICE_EXTRA_PRIVILEGES', count(*), 0 from information_schema.role_table_grants where table_schema='public' and table_name='oauth_transactions' and grantee='service_role' and privilege_type not in ('SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER')
  union all select 'TOKEN_NON_SERVICE_PRIVILEGES', count(*), 0 from information_schema.role_table_grants where table_schema='public' and table_name='platform_connection_tokens' and grantee in ('PUBLIC','anon','authenticated')
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
