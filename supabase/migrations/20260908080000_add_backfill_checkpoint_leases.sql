alter table public.backfill_checkpoints
  add column if not exists lease_token uuid null,
  add column if not exists lease_expires_at timestamptz null,
  add constraint backfill_checkpoints_lease_pair_check
    check ((lease_token is null) = (lease_expires_at is null));

create or replace function public.claim_backfill_checkpoint(
  p_id uuid,
  p_lease_token uuid,
  p_lease_seconds integer default 300
)
returns setof public.backfill_checkpoints
language sql
security definer
set search_path = ''
as $$
  update public.backfill_checkpoints
     set status = 'running',
         lease_token = p_lease_token,
         lease_expires_at = now() + make_interval(secs => p_lease_seconds),
         attempt_count = attempt_count + 1,
         last_error_code = null,
         updated_at = now()
   where id = p_id
     and p_lease_token is not null
     and p_lease_seconds between 30 and 900
     and (
       status in ('queued','failed')
       or (status = 'running' and lease_expires_at <= now())
     )
  returning *;
$$;

revoke all on function public.claim_backfill_checkpoint(uuid,uuid,integer) from public, anon, authenticated;
grant execute on function public.claim_backfill_checkpoint(uuid,uuid,integer) to service_role;
