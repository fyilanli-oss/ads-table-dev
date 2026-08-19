-- Restore least-privilege table grants without changing token data or schema shape.
revoke all privileges on table public.platform_connection_tokens from service_role;
grant select, insert, update, delete on table public.platform_connection_tokens to service_role;

-- Retain the server-only boundary if grants drift before this migration is applied.
revoke all privileges on table public.platform_connection_tokens from public;
revoke all privileges on table public.platform_connection_tokens from anon;
revoke all privileges on table public.platform_connection_tokens from authenticated;

alter table public.platform_connection_tokens enable row level security;
alter table public.platform_connection_tokens force row level security;
