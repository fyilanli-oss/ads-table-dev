-- Additive: existing OAuth grants remain pending until the merchant saves a verified account and cost.
alter table public.shopify_workspace_provider_connections
  add column email_monthly_plan_cost numeric(10,2),
  add column account_currency text;

alter table public.shopify_workspace_provider_connections
  add constraint shopify_provider_plan_cost_valid check (
    email_monthly_plan_cost is null or
    (provider = 'klaviyo' and email_monthly_plan_cost >= 0 and email_monthly_plan_cost <= 99999999.99)
  ),
  add constraint shopify_provider_account_currency_valid check (
    account_currency is null or account_currency ~ '^[A-Z]{3}$'
  ),
  add constraint shopify_klaviyo_connected_requires_cost check (
    provider <> 'klaviyo' or status <> 'connected' or
    (email_monthly_plan_cost is not null and account_currency is not null)
  ) not valid;

-- Fail rather than silently changing any existing connected merchant record.
alter table public.shopify_workspace_provider_connections
  validate constraint shopify_klaviyo_connected_requires_cost;
