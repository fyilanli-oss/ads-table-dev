-- R7-B5-C1 additive foundation: effective-dated Klaviyo estimated 30-day Email spend history.
-- Repository preparation only until the separately approved production migration gate.

create table public.workspace_provider_email_spend_history (
  workspace_id uuid not null,
  provider text not null default 'klaviyo',
  provider_account_id text not null,
  source_currency text not null,
  estimated_30_day_email_spend numeric(10,2) not null,
  effective_from date not null,
  provenance text not null default 'user_estimated',
  correction_version bigint not null default 1,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (workspace_id, provider, provider_account_id, effective_from),
  constraint workspace_provider_email_spend_connection_fk
    foreign key (workspace_id, provider)
    references public.workspace_provider_connections(workspace_id, provider)
    on update restrict on delete restrict,
  constraint workspace_provider_email_spend_provider_check
    check (provider = 'klaviyo'),
  constraint workspace_provider_email_spend_account_check
    check (length(btrim(provider_account_id)) between 1 and 256),
  constraint workspace_provider_email_spend_currency_check
    check (source_currency ~ '^[A-Z]{3}$'),
  constraint workspace_provider_email_spend_amount_check
    check (estimated_30_day_email_spend >= 0 and estimated_30_day_email_spend <= 99999999.99),
  constraint workspace_provider_email_spend_provenance_check
    check (provenance = 'user_estimated'),
  constraint workspace_provider_email_spend_version_check
    check (correction_version > 0),
  constraint workspace_provider_email_spend_timestamp_check
    check (updated_at >= created_at)
);

create index workspace_provider_email_spend_lookup_idx
  on public.workspace_provider_email_spend_history
  (workspace_id, provider_account_id, effective_from desc);

alter table public.workspace_provider_email_spend_history enable row level security;
alter table public.workspace_provider_email_spend_history force row level security;

revoke all on table public.workspace_provider_email_spend_history from public, anon, authenticated;
revoke all on table public.workspace_provider_email_spend_history from service_role;
grant select, insert, update, delete on table public.workspace_provider_email_spend_history to service_role;

comment on table public.workspace_provider_email_spend_history is
  'Workspace/account effective-change history for merchant-entered Estimated 30-Day Klaviyo Email Spend. Consecutive 30-day windows are derived; they are not materialized.';
comment on column public.workspace_provider_email_spend_history.estimated_30_day_email_spend is
  'Merchant estimate for one normalized 30-day Email period; SMS and provider actual billing are excluded.';
comment on column public.workspace_provider_email_spend_history.effective_from is
  'Server-authoritative Klaviyo account business date. The value remains effective until the next change row.';

create or replace function public.complete_klaviyo_connection_with_spend_history(
  p_workspace_id uuid,
  p_expected_version bigint,
  p_account_id text,
  p_account_name text,
  p_source_currency text,
  p_estimated_30_day_email_spend numeric,
  p_effective_from date
) returns boolean
language plpgsql
security definer
set search_path = pg_catalog, public
as $$
declare
  v_updated integer;
  v_timestamp timestamptz := now();
begin
  update public.workspace_provider_connections
  set status = 'connected',
      active_account_id = btrim(p_account_id),
      active_account_name = btrim(p_account_name),
      source_currency = p_source_currency,
      selected_accounts = jsonb_build_array(jsonb_build_object(
        'id', btrim(p_account_id),
        'name', btrim(p_account_name),
        'currency', p_source_currency
      )),
      monthly_plan_cost = p_estimated_30_day_email_spend,
      conversion_metric_id = null,
      conversion_metric_name = null,
      conversion_metric_integration_name = null,
      conversion_metric_integration_category = null,
      conversion_metric_verified_at = null,
      add_to_cart_metric_id = null,
      add_to_cart_metric_name = null,
      add_to_cart_metric_integration_name = null,
      add_to_cart_metric_integration_category = null,
      add_to_cart_metric_verified_at = null,
      checkout_metric_id = null,
      checkout_metric_name = null,
      checkout_metric_integration_name = null,
      checkout_metric_integration_category = null,
      checkout_metric_verified_at = null,
      account_verified_at = v_timestamp,
      connected_at = v_timestamp,
      disconnected_at = null,
      connection_version = p_expected_version + 1,
      updated_at = v_timestamp
  where workspace_id = p_workspace_id
    and provider = 'klaviyo'
    and status = 'pending_account_selection'
    and connection_version = p_expected_version;
  get diagnostics v_updated = row_count;
  if v_updated <> 1 then
    raise exception 'CONNECTION_CHANGED' using errcode = 'P0001';
  end if;

  insert into public.workspace_provider_email_spend_history (
    workspace_id, provider, provider_account_id, source_currency,
    estimated_30_day_email_spend, effective_from, provenance
  ) values (
    p_workspace_id, 'klaviyo', btrim(p_account_id), p_source_currency,
    p_estimated_30_day_email_spend, p_effective_from, 'user_estimated'
  )
  on conflict (workspace_id, provider, provider_account_id, effective_from)
  do update set
    source_currency = excluded.source_currency,
    estimated_30_day_email_spend = excluded.estimated_30_day_email_spend,
    correction_version = public.workspace_provider_email_spend_history.correction_version + 1,
    updated_at = v_timestamp;

  return true;
end;
$$;

create or replace function public.update_klaviyo_estimated_30_day_email_spend(
  p_workspace_id uuid,
  p_expected_version bigint,
  p_account_id text,
  p_source_currency text,
  p_estimated_30_day_email_spend numeric,
  p_effective_from date
) returns boolean
language plpgsql
security definer
set search_path = pg_catalog, public
as $$
declare
  v_updated integer;
  v_timestamp timestamptz := now();
begin
  insert into public.workspace_provider_email_spend_history (
    workspace_id, provider, provider_account_id, source_currency,
    estimated_30_day_email_spend, effective_from, provenance
  )
  select p_workspace_id, 'klaviyo', btrim(p_account_id), p_source_currency,
         p_estimated_30_day_email_spend, p_effective_from, 'user_estimated'
  where exists (
    select 1
    from public.workspace_provider_connections
    where workspace_id = p_workspace_id
      and provider = 'klaviyo'
      and status = 'connected'
      and active_account_id = btrim(p_account_id)
      and source_currency = p_source_currency
      and connection_version = p_expected_version
  );

  if not found then
    raise exception 'CONNECTION_CHANGED' using errcode = 'P0001';
  end if;

  update public.workspace_provider_connections
  set monthly_plan_cost = p_estimated_30_day_email_spend,
      connection_version = p_expected_version + 1,
      updated_at = v_timestamp
  where workspace_id = p_workspace_id
    and provider = 'klaviyo'
    and status = 'connected'
    and active_account_id = btrim(p_account_id)
    and source_currency = p_source_currency
    and connection_version = p_expected_version;
  get diagnostics v_updated = row_count;
  if v_updated <> 1 then
    raise exception 'CONNECTION_CHANGED' using errcode = 'P0001';
  end if;

  return true;
exception
  when unique_violation then
    raise exception 'DUPLICATE_EFFECTIVE_START' using errcode = 'P0001';
end;
$$;

create or replace function public.correct_klaviyo_estimated_30_day_email_spend(
  p_workspace_id uuid,
  p_expected_version bigint,
  p_account_id text,
  p_effective_from date,
  p_estimated_30_day_email_spend numeric
) returns boolean
language plpgsql
security definer
set search_path = pg_catalog, public
as $$
declare
  v_currency text;
  v_latest date;
  v_updated integer;
  v_timestamp timestamptz := now();
begin
  select source_currency
    into v_currency
  from public.workspace_provider_connections
  where workspace_id = p_workspace_id
    and provider = 'klaviyo'
    and status = 'connected'
    and active_account_id = btrim(p_account_id)
    and connection_version = p_expected_version
  for update;

  if v_currency is null then
    raise exception 'CONNECTION_CHANGED' using errcode = 'P0001';
  end if;

  update public.workspace_provider_email_spend_history
  set estimated_30_day_email_spend = p_estimated_30_day_email_spend,
      correction_version = correction_version + 1,
      updated_at = v_timestamp
  where workspace_id = p_workspace_id
    and provider = 'klaviyo'
    and provider_account_id = btrim(p_account_id)
    and effective_from = p_effective_from;
  get diagnostics v_updated = row_count;
  if v_updated <> 1 then
    raise exception 'SPEND_HISTORY_ENTRY_NOT_FOUND' using errcode = 'P0001';
  end if;

  select max(effective_from)
    into v_latest
  from public.workspace_provider_email_spend_history
  where workspace_id = p_workspace_id
    and provider = 'klaviyo'
    and provider_account_id = btrim(p_account_id);

  update public.workspace_provider_connections
  set monthly_plan_cost = case
        when v_latest = p_effective_from then p_estimated_30_day_email_spend
        else monthly_plan_cost
      end,
      connection_version = p_expected_version + 1,
      updated_at = v_timestamp
  where workspace_id = p_workspace_id
    and provider = 'klaviyo'
    and connection_version = p_expected_version;

  return true;
end;
$$;

revoke all on function public.complete_klaviyo_connection_with_spend_history(uuid, bigint, text, text, text, numeric, date) from public, anon, authenticated;
revoke all on function public.update_klaviyo_estimated_30_day_email_spend(uuid, bigint, text, text, numeric, date) from public, anon, authenticated;
revoke all on function public.correct_klaviyo_estimated_30_day_email_spend(uuid, bigint, text, date, numeric) from public, anon, authenticated;

grant execute on function public.complete_klaviyo_connection_with_spend_history(uuid, bigint, text, text, text, numeric, date) to service_role;
grant execute on function public.update_klaviyo_estimated_30_day_email_spend(uuid, bigint, text, text, numeric, date) to service_role;
grant execute on function public.correct_klaviyo_estimated_30_day_email_spend(uuid, bigint, text, date, numeric) to service_role;
