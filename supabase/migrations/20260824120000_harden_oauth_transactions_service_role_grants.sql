-- Forward-only least-privilege correction for the server-only OAuth transaction store.
revoke all privileges on table public.oauth_transactions from service_role;
grant select, insert, delete on table public.oauth_transactions to service_role;

revoke all privileges on table public.oauth_transactions from public;
revoke all privileges on table public.oauth_transactions from anon;
revoke all privileges on table public.oauth_transactions from authenticated;

alter table public.oauth_transactions enable row level security;
