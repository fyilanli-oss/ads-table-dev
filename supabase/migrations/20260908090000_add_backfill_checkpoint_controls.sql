alter table public.backfill_checkpoints
  add column if not exists control_state text not null default 'active'
    check (control_state in ('active','paused','cancelled')),
  add column if not exists control_updated_at timestamptz not null default now();

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
     set status = 'running', lease_token = p_lease_token,
         lease_expires_at = now() + make_interval(secs => p_lease_seconds),
         attempt_count = attempt_count + 1, last_error_code = null, updated_at = now()
   where id = p_id and control_state = 'active' and p_lease_token is not null
     and p_lease_seconds between 30 and 900
     and (status in ('queued','failed') or (status = 'running' and lease_expires_at <= now()))
  returning *;
$$;

create or replace function public.control_backfill_checkpoint(p_id uuid,p_action text)
returns setof public.backfill_checkpoints
language plpgsql
security definer
set search_path = ''
as $$
begin
  if p_action not in ('pause','resume','cancel') then raise exception 'unsupported backfill control action'; end if;
  return query
  update public.backfill_checkpoints
     set control_state = case p_action when 'pause' then 'paused' when 'resume' then 'active' else 'cancelled' end,
         status = case when p_action = 'pause' and status = 'running' then 'queued' when p_action = 'cancel' then 'skipped' else status end,
         lease_token = null, lease_expires_at = null,
         last_error_code = case when p_action = 'cancel' then null else last_error_code end,
         control_updated_at = now(), updated_at = now()
   where id = p_id
     and status not in ('completed','skipped')
     and control_state <> 'cancelled'
     and ((p_action = 'pause' and control_state = 'active') or (p_action = 'resume' and control_state = 'paused') or p_action = 'cancel')
  returning *;
end;
$$;

revoke all on function public.claim_backfill_checkpoint(uuid,uuid,integer) from public, anon, authenticated;
grant execute on function public.claim_backfill_checkpoint(uuid,uuid,integer) to service_role;
revoke all on function public.control_backfill_checkpoint(uuid,text) from public, anon, authenticated;
grant execute on function public.control_backfill_checkpoint(uuid,text) to service_role;
