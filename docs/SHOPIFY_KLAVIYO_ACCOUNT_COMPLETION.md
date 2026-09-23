# Shopify Klaviyo account completion

After OAuth, the previous UI only displayed “Account selection is next.” The connection remained `pending_account_selection`, with no account picker or completion endpoint.

This change loads Klaviyo accounts using the workspace's encrypted grant, asks the merchant to select an account, then requires an explicit **Email Monthly Plan Cost** save. A free plan is entered as `0`. The amount uses the verified Klaviyo account's preferred currency, displayed in the field label. This does not purchase or change a Klaviyo plan.

Both endpoints authenticate a fresh Shopify session token and derive the shop and workspace on the server. Saving rechecks account ownership with Klaviyo. A concurrent reconnect or revocation prevents an older request from overwriting the connection. Expired access tokens are refreshed once and stored encrypted; a rejected grant asks the merchant to reconnect.

OAuth success now returns to the installed shop's Shopify Admin app entry, derived from the consumed transaction. Open **Data sources** there to finish account setup. Callback query parameters cannot choose the return shop. Failed callbacks retain the existing generic failure page; reopening AdsTable from Shopify Admin restores the authenticated context.

## Release order

1. Apply `supabase/migrations/20260919194248_shopify_klaviyo_account_completion.sql` to the AdsTable Supabase project before deploying the new application. It adds two nullable columns and constraints without changing grants, RLS, or existing pending connections. It deliberately fails if an existing connected Klaviyo row lacks cost/currency, rather than silently altering that row. The timestamp matches the migration version already recorded in the live ledger.
2. Deploy this branch with the existing Shopify and Klaviyo credentials and `SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED=true`. Keep `SHOPIFY_APP_URL=https://dev.adstable.app`.
3. In Shopify Admin, open AdsTable → Data sources. For an existing pending grant, choose the Klaviyo account, enter its monthly cost, and select **Save and connect**. If authorization was revoked, use Connect and consent again.
4. Verify the persisted row is `connected` with an account ID, currency, and cost, then reopen Data sources to verify the saved state.

Rollback: restore the previous application deployment and retain the additive columns. Do not drop columns or delete grants to roll back UI behavior.

## Validation

- Shopify, OAuth, presentation, configuration and new account-completion suites: 196 passing, one opt-in live Shopify test skipped.
- Architecture and canonical-boundary guards pass.
- Migration executed inside a rolled-back database transaction; columns and constraints accepted against the existing pending grant. No persistent schema changes applied.
- In-app browser with real Shopify Polaris components and mocked Shopify/Klaviyo responses: account selection → cost entry → connected state passed. This is not a live OAuth or account-write acceptance test.
- Live acceptance requires the merchant's authenticated Shopify session and their actual monthly plan cost.

References: [Klaviyo accounts](https://developers.klaviyo.com/en/reference/get_accounts), [Klaviyo OAuth refresh](https://developers.klaviyo.com/en/docs/set_up_oauth), [Shopify Select](https://shopify.dev/docs/api/app-home/latest/web-components/forms/select).
