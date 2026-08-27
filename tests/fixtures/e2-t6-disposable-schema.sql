\set ON_ERROR_STOP on
create role authenticated;
create role anon;
create role service_role bypassrls;
create schema auth;
create schema supabase_migrations;
create function auth.uid() returns uuid language sql stable as $$ select nullif(current_setting('request.jwt.claim.sub', true), '')::uuid $$;
create table auth.users (id uuid primary key);
create table public.users (id uuid primary key);
create table public.performance_dataset_rows_v2 (
  id uuid default gen_random_uuid() primary key, user_id uuid not null, platform text not null, traffic_type text not null,
  source_system text not null, channel text, platform_account_id text not null, business_date date not null, campaign_type text,
  root_entity_type text not null, root_entity_id text not null, root_entity_name text, parent_entity_type text,
  parent_entity_id text, parent_entity_name text, entity_type text not null, entity_id text not null, entity_name text not null,
  entity_key text not null, metric_support jsonb not null, impressions numeric, ad_clicks numeric, sessions numeric, spend numeric,
  add_to_cart numeric, add_to_cart_value numeric, checkout numeric, checkout_value numeric, purchase numeric, purchase_value numeric,
  source_currency text not null, target_currency text not null, fx_rate numeric not null, fx_rate_date date not null,
  fx_provider text not null, fx_engine_version text not null, source_timezone text not null, time_engine_version text not null,
  canonical_contract_version text not null, adapter_version text not null, source_confidence text not null, synthetic boolean not null,
  ga4_property_id text, source_job_id uuid, raw jsonb not null, created_at timestamptz default now(), updated_at timestamptz default now()
);
create table public.performance_dataset_rows (id bigint generated always as identity);
create table public.dashboard_snapshots (id bigint generated always as identity);
create table public.oauth_transactions (id bigint generated always as identity);
create table public.platform_connections (user_id uuid, platform text, connected boolean, access_token text, refresh_token text);
create table public.platform_connection_tokens (user_id uuid, platform text);
create table supabase_migrations.schema_migrations (version text);
insert into auth.users values ('00000000-0000-4000-8000-000000000001'), ('00000000-0000-4000-8000-000000000002');
insert into public.users select id from auth.users;
insert into supabase_migrations.schema_migrations select value::text from generate_series(1, 37) value;
alter table public.performance_dataset_rows_v2 enable row level security;
create policy performance_dataset_rows_v2_select_own on public.performance_dataset_rows_v2 for select to authenticated using ((select auth.uid()) = user_id);
revoke all on public.performance_dataset_rows_v2 from anon, authenticated;
grant select on public.performance_dataset_rows_v2 to authenticated;
grant all on public.performance_dataset_rows_v2 to service_role;
grant usage on schema public, auth to authenticated, anon, service_role;
grant execute on function auth.uid() to authenticated;
