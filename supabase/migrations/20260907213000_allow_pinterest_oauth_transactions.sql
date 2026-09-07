-- Pinterest Package 1 uses the same single-use OAuth transaction boundary as active providers.
alter table public.oauth_transactions
  drop constraint if exists oauth_transactions_provider_check;

alter table public.oauth_transactions
  add constraint oauth_transactions_provider_check
  check (provider in ('meta', 'google_ads', 'google_sheets', 'ga4_organic', 'klaviyo', 'tiktok', 'pinterest'));
