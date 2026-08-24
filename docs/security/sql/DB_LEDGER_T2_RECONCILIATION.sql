-- CONTROLLED PRODUCTION OPERATION. Not a product migration; never run via db push.
-- Run only after DB_LEDGER_T2_PREFLIGHT.sql, checksum verification, and explicit approval.
begin;

lock table supabase_migrations.schema_migrations in share row exclusive mode;
lock table public.oauth_transactions in share row exclusive mode;
lock table public.platform_connection_tokens in share mode;
lock table public.platform_connections in share mode;

do $precondition$
declare
  target_count bigint;
  ledger_count bigint;
  oauth_rows bigint;
  connected_count bigint;
  encrypted_count bigint;
  missing_encrypted bigint;
  plaintext_access bigint;
  plaintext_refresh bigint;
  oauth_functions bigint;
  oauth_constraints bigint;
  token_constraints bigint;
  oauth_service_grants bigint;
  oauth_non_service_grants bigint;
  token_service_grants bigint;
  token_service_extras bigint;
  rls_state bigint;
begin
  select count(*) into target_count from supabase_migrations.schema_migrations
  where version in ('20260818090000','20260818120000','20260819120000','20260824120000');
  select count(*) into ledger_count from supabase_migrations.schema_migrations;
  select count(*) into oauth_rows from public.oauth_transactions;
  select count(*) into connected_count from public.platform_connections where connected=true;
  select count(*) into encrypted_count from public.platform_connection_tokens;
  select count(*) into missing_encrypted from public.platform_connections pc where pc.connected=true and not exists (select 1 from public.platform_connection_tokens pt where pt.user_id=pc.user_id and pt.platform=pc.platform);
  select count(*) into plaintext_access from public.platform_connections where access_token is not null;
  select count(*) into plaintext_refresh from public.platform_connections where refresh_token is not null;
  select count(*) into oauth_functions from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid=p.pronamespace where n.nspname='public' and p.proname in ('consume_oauth_transaction','cleanup_expired_oauth_transactions');
  select count(*) into oauth_constraints from pg_catalog.pg_constraint x join pg_catalog.pg_class c on c.oid=x.conrelid join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='oauth_transactions' and x.convalidated;
  select count(*) into token_constraints from pg_catalog.pg_constraint x join pg_catalog.pg_class c on c.oid=x.conrelid join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='platform_connection_tokens' and x.convalidated;
  select count(*) into oauth_service_grants from information_schema.role_table_grants where table_schema='public' and table_name='oauth_transactions' and grantee='service_role' and privilege_type in ('SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER');
  select count(*) into oauth_non_service_grants from information_schema.role_table_grants where table_schema='public' and table_name='oauth_transactions' and grantee in ('PUBLIC','anon','authenticated');
  select count(*) into token_service_grants from information_schema.role_table_grants where table_schema='public' and table_name='platform_connection_tokens' and grantee='service_role' and privilege_type in ('SELECT','INSERT','UPDATE','DELETE');
  select count(*) into token_service_extras from information_schema.role_table_grants where table_schema='public' and table_name='platform_connection_tokens' and grantee='service_role' and privilege_type not in ('SELECT','INSERT','UPDATE','DELETE');
  select count(*) into rls_state from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and ((c.relname='oauth_transactions' and c.relrowsecurity and not c.relforcerowsecurity) or (c.relname='platform_connection_tokens' and c.relrowsecurity and c.relforcerowsecurity));

  if target_count<>0 or ledger_count<>33 then raise exception using message='DB_LEDGER_T2_PRECONDITION_LEDGER_FAILED'; end if;
  if to_regclass('public.oauth_transactions') is null or to_regclass('public.platform_connection_tokens') is null then raise exception using message='DB_LEDGER_T2_PRECONDITION_OBJECT_FAILED'; end if;
  if oauth_rows<>0 then raise exception using message='DB_LEDGER_T2_PRECONDITION_OAUTH_APPROVAL_REQUIRED'; end if;
  if oauth_functions<>2 or oauth_constraints<>5 or token_constraints<>6 or rls_state<>2 then raise exception using message='DB_LEDGER_T2_PRECONDITION_SCHEMA_FAILED'; end if;
  if oauth_service_grants<>7 or oauth_non_service_grants<>0 or token_service_grants<>4 or token_service_extras<>0 then raise exception using message='DB_LEDGER_T2_PRECONDITION_GRANT_FAILED'; end if;
  if connected_count<>7 or encrypted_count<>7 or missing_encrypted<>0 or plaintext_access<>0 or plaintext_refresh<>0 then raise exception using message='DB_LEDGER_T2_PRECONDITION_TOKEN_FAILED'; end if;
end
$precondition$;

revoke all privileges on table public.oauth_transactions from service_role;
grant select, insert, delete on table public.oauth_transactions to service_role;
revoke all privileges on table public.oauth_transactions from public;
revoke all privileges on table public.oauth_transactions from anon;
revoke all privileges on table public.oauth_transactions from authenticated;
alter table public.oauth_transactions enable row level security;

do $reconcile$
declare
  affected_count bigint;
begin
  insert into supabase_migrations.schema_migrations(version,name)
  values
    ('20260818090000','create_oauth_transactions'),
    ('20260818120000','create_platform_connection_tokens'),
    ('20260819120000','harden_platform_connection_tokens_service_role_grants'),
    ('20260824120000','harden_oauth_transactions_service_role_grants');
  get diagnostics affected_count = row_count;
  if affected_count<>4 then raise exception using message='DB_LEDGER_T2_AFFECTED_ROW_COUNT_FAILED'; end if;
end
$reconcile$;

do $postcondition$
declare
  exact_targets bigint;
  ledger_count bigint;
  oauth_service_grants bigint;
  oauth_service_extras bigint;
  oauth_non_service_grants bigint;
  oauth_rows bigint;
  connected_count bigint;
  encrypted_count bigint;
  missing_encrypted bigint;
  plaintext_access bigint;
  plaintext_refresh bigint;
begin
  select count(*) into exact_targets from supabase_migrations.schema_migrations m join (values
    ('20260818090000'::text,'create_oauth_transactions'::text),
    ('20260818120000','create_platform_connection_tokens'),
    ('20260819120000','harden_platform_connection_tokens_service_role_grants'),
    ('20260824120000','harden_oauth_transactions_service_role_grants')) t(version,name) using(version,name);
  select count(*) into ledger_count from supabase_migrations.schema_migrations;
  select count(*) into oauth_service_grants from information_schema.role_table_grants where table_schema='public' and table_name='oauth_transactions' and grantee='service_role' and privilege_type in ('SELECT','INSERT','DELETE');
  select count(*) into oauth_service_extras from information_schema.role_table_grants where table_schema='public' and table_name='oauth_transactions' and grantee='service_role' and privilege_type not in ('SELECT','INSERT','DELETE');
  select count(*) into oauth_non_service_grants from information_schema.role_table_grants where table_schema='public' and table_name='oauth_transactions' and grantee in ('PUBLIC','anon','authenticated');
  select count(*) into oauth_rows from public.oauth_transactions;
  select count(*) into connected_count from public.platform_connections where connected=true;
  select count(*) into encrypted_count from public.platform_connection_tokens;
  select count(*) into missing_encrypted from public.platform_connections pc where pc.connected=true and not exists (select 1 from public.platform_connection_tokens pt where pt.user_id=pc.user_id and pt.platform=pc.platform);
  select count(*) into plaintext_access from public.platform_connections where access_token is not null;
  select count(*) into plaintext_refresh from public.platform_connections where refresh_token is not null;

  if exact_targets<>4 or ledger_count<>37 then raise exception using message='DB_LEDGER_T2_POSTCONDITION_LEDGER_FAILED'; end if;
  if oauth_service_grants<>3 or oauth_service_extras<>0 or oauth_non_service_grants<>0 then raise exception using message='DB_LEDGER_T2_POSTCONDITION_GRANT_FAILED'; end if;
  if oauth_rows<>0 or connected_count<>7 or encrypted_count<>7 or missing_encrypted<>0 or plaintext_access<>0 or plaintext_refresh<>0 then raise exception using message='DB_LEDGER_T2_POSTCONDITION_DATA_FAILED'; end if;
end
$postcondition$;

commit;
