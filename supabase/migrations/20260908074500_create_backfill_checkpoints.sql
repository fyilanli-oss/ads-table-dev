create table if not exists public.backfill_checkpoints (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  platform text not null check (platform in ('meta','google','tiktok','klaviyo')),
  platform_account_id text not null,
  business_date date not null,
  date_key text not null check (date_key in ('yesterday','today')),
  finality text not null check (finality in ('finalized','provisional')),
  priority smallint not null check (priority in (1,2)),
  status text not null default 'queued' check (status in ('queued','running','completed','failed','skipped')),
  cursor text null check (cursor is null or length(cursor) between 1 and 4096),
  attempt_count integer not null default 0 check (attempt_count >= 0),
  last_error_code text null check (last_error_code is null or last_error_code ~ '^[A-Z0-9_]{1,64}$'),
  check (status <> 'completed' or cursor is null),
  check (status <> 'failed' or last_error_code is not null),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (user_id, platform, platform_account_id, business_date, date_key)
);

create index if not exists backfill_checkpoints_resume_idx
  on public.backfill_checkpoints (status, priority, updated_at)
  where status in ('queued','running','failed');

alter table public.backfill_checkpoints enable row level security;
alter table public.backfill_checkpoints force row level security;
revoke all on table public.backfill_checkpoints from public, anon, authenticated;
grant select, insert, update, delete on table public.backfill_checkpoints to service_role;
