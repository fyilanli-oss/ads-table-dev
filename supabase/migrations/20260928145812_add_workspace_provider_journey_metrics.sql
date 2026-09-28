-- R6-D5 additive Klaviyo journey metric bindings.
-- Existing conversion_metric_* columns remain the backward-compatible
-- purchase / Placed Order binding. These nullable groups add only the two
-- preceding commerce stages; the migration does not discover provider
-- metrics, mutate existing connections, or activate Dataset V2 writes.

alter table public.workspace_provider_connections
  add column add_to_cart_metric_id text,
  add column add_to_cart_metric_name text,
  add column add_to_cart_metric_integration_name text,
  add column add_to_cart_metric_integration_category text,
  add column add_to_cart_metric_verified_at timestamptz,
  add column checkout_metric_id text,
  add column checkout_metric_name text,
  add column checkout_metric_integration_name text,
  add column checkout_metric_integration_category text,
  add column checkout_metric_verified_at timestamptz;

alter table public.workspace_provider_connections
  add constraint workspace_provider_add_to_cart_metric_complete check (
    (
      add_to_cart_metric_id is null
      and add_to_cart_metric_name is null
      and add_to_cart_metric_integration_name is null
      and add_to_cart_metric_integration_category is null
      and add_to_cart_metric_verified_at is null
    )
    or (
      provider = 'klaviyo'
      and status = 'connected'
      and active_account_id is not null
      and length(btrim(add_to_cart_metric_id)) > 0
      and lower(btrim(add_to_cart_metric_name)) = 'added to cart'
      and length(btrim(add_to_cart_metric_integration_name)) > 0
      and (
        add_to_cart_metric_integration_category is null
        or length(btrim(add_to_cart_metric_integration_category)) > 0
      )
      and add_to_cart_metric_verified_at is not null
    )
  ),
  add constraint workspace_provider_checkout_metric_complete check (
    (
      checkout_metric_id is null
      and checkout_metric_name is null
      and checkout_metric_integration_name is null
      and checkout_metric_integration_category is null
      and checkout_metric_verified_at is null
    )
    or (
      provider = 'klaviyo'
      and status = 'connected'
      and active_account_id is not null
      and length(btrim(checkout_metric_id)) > 0
      and lower(btrim(checkout_metric_name)) in ('checkout started', 'started checkout')
      and length(btrim(checkout_metric_integration_name)) > 0
      and (
        checkout_metric_integration_category is null
        or length(btrim(checkout_metric_integration_category)) > 0
      )
      and checkout_metric_verified_at is not null
    )
  );

comment on column public.workspace_provider_connections.add_to_cart_metric_id is
  'Provider-verified Klaviyo Added to Cart metric bound to this workspace/provider account.';
comment on column public.workspace_provider_connections.checkout_metric_id is
  'Provider-verified Klaviyo checkout metric bound to this workspace/provider account; exact provider name is Checkout Started or Started Checkout.';

