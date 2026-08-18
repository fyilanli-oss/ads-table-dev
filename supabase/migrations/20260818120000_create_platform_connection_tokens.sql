-- Server-only encrypted provider token storage. Apply before enabling encrypted writes.
create table if not exists public.platform_connection_tokens (
  user_id uuid not null references auth.users(id) on delete cascade,
  platform text not null check (length(btrim(platform)) > 0),
  access_token_envelope jsonb,
  refresh_token_envelope jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (user_id, platform),
  constraint platform_connection_tokens_has_token_check check (
    access_token_envelope is not null or refresh_token_envelope is not null
  ),
  constraint platform_connection_tokens_access_envelope_check check (
    access_token_envelope is null or (
      jsonb_typeof(access_token_envelope) = 'object'
      and access_token_envelope->>'version' = 'v1'
      and length(access_token_envelope->>'keyId') > 0
      and length(access_token_envelope->>'iv') > 0
      and length(access_token_envelope->>'tag') > 0
      and length(access_token_envelope->>'ciphertext') > 0
    )
  ),
  constraint platform_connection_tokens_refresh_envelope_check check (
    refresh_token_envelope is null or (
      jsonb_typeof(refresh_token_envelope) = 'object'
      and refresh_token_envelope->>'version' = 'v1'
      and length(refresh_token_envelope->>'keyId') > 0
      and length(refresh_token_envelope->>'iv') > 0
      and length(refresh_token_envelope->>'tag') > 0
      and length(refresh_token_envelope->>'ciphertext') > 0
    )
  )
);

alter table public.platform_connection_tokens enable row level security;
alter table public.platform_connection_tokens force row level security;

revoke all on table public.platform_connection_tokens from public;
revoke all on table public.platform_connection_tokens from anon;
revoke all on table public.platform_connection_tokens from authenticated;
grant select, insert, update, delete on table public.platform_connection_tokens to service_role;

comment on table public.platform_connection_tokens is
  'Server-only AES-256-GCM envelopes. Plaintext provider tokens are forbidden.';
