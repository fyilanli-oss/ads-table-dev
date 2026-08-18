-- Apply this migration before deploying server code that uses the OAuth transaction store.
create table if not exists public.oauth_transactions (
  state_hash text primary key check (state_hash ~ '^[0-9a-f]{64}$'),
  user_id uuid not null references auth.users(id) on delete cascade,
  provider text not null check (provider in ('meta', 'google_ads', 'google_sheets', 'ga4_organic', 'klaviyo', 'tiktok')),
  redirect_uri text not null,
  pkce_verifier text,
  created_at timestamptz not null default now(),
  expires_at timestamptz not null,
  check (expires_at > created_at and expires_at <= created_at + interval '30 minutes')
);

create index if not exists oauth_transactions_expires_at_idx on public.oauth_transactions (expires_at);
alter table public.oauth_transactions enable row level security;
revoke all on table public.oauth_transactions from anon, authenticated;
grant select, insert, delete on table public.oauth_transactions to service_role;

create or replace function public.consume_oauth_transaction(
  p_state_hash text, p_provider text, p_redirect_uri text
) returns table (user_id uuid, provider text, redirect_uri text, pkce_verifier text, created_at timestamptz)
language sql security definer set search_path = public, pg_temp
as $$
  delete from public.oauth_transactions
  where oauth_transactions.state_hash = p_state_hash
    and oauth_transactions.provider = p_provider
    and oauth_transactions.redirect_uri = p_redirect_uri
    and oauth_transactions.expires_at > now()
  returning oauth_transactions.user_id, oauth_transactions.provider,
            oauth_transactions.redirect_uri, oauth_transactions.pkce_verifier, oauth_transactions.created_at;
$$;

create or replace function public.cleanup_expired_oauth_transactions()
returns bigint language plpgsql security definer set search_path = public, pg_temp
as $$
declare deleted_count bigint;
begin
  delete from public.oauth_transactions where expires_at <= now();
  get diagnostics deleted_count = row_count;
  return deleted_count;
end;
$$;

revoke all on function public.consume_oauth_transaction(text, text, text) from public, anon, authenticated;
revoke all on function public.cleanup_expired_oauth_transactions() from public, anon, authenticated;
grant execute on function public.consume_oauth_transaction(text, text, text) to service_role;
grant execute on function public.cleanup_expired_oauth_transactions() to service_role;
