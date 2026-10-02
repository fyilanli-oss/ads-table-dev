'use strict';

const { requireServerWorkspaceAuthority } = require('../../funnel-core/workspace-dataset-runtime');

const TABLE = 'workspace_provider_connections';
const ACTIVE_PROVIDERS = new Set(['meta', 'google_ads', 'klaviyo']);

function required(value, field) {
  if (typeof value !== 'string' || value.trim() === '') throw new TypeError(`${field} is required`);
  return value.trim();
}

function providerName(value) {
  const provider = required(value, 'provider');
  if (!ACTIVE_PROVIDERS.has(provider)) throw new Error('PROVIDER_NOT_ACTIVE_IN_R4');
  return provider;
}

function tokenContext(workspaceId, provider, tokenType) {
  return { userId: `workspace:${workspaceId}`, platform: provider, tokenType };
}

function selectedAccounts(provider, accounts) {
  if (!Array.isArray(accounts)) throw new TypeError('accounts are required');
  const limit = provider === 'klaviyo' ? 1 : 3;
  const unique = [];
  const seen = new Set();
  for (const account of accounts) {
    const id = required(account?.id, 'account.id');
    if (seen.has(id)) throw new Error('INVALID_ACCOUNT_SELECTION_COUNT');
    seen.add(id);
    const verified = {
      id,
      name: required(account?.name, 'account.name'),
      currency: required(account?.currency, 'account.currency'),
    };
    if (provider === 'google_ads') {
      const loginCustomerId = required(account?.login_customer_id || account?.loginCustomerId, 'account.login_customer_id');
      if (!/^\d+$/.test(loginCustomerId)) throw new Error('INVALID_GOOGLE_LOGIN_CUSTOMER_ID');
      verified.login_customer_id = loginCustomerId;
    }
    unique.push(Object.freeze(verified));
  }
  if (unique.length < 1 || unique.length > limit) throw new Error('INVALID_ACCOUNT_SELECTION_COUNT');
  return Object.freeze(unique);
}

function conversionMetric(metric) {
  return Object.freeze({
    id: required(metric?.id, 'metric.id'),
    name: required(metric?.name, 'metric.name'),
    integrationName: required(metric?.integration_name, 'metric.integration_name'),
    integrationCategory: typeof metric?.integration_category === 'string' && metric.integration_category.trim()
      ? metric.integration_category.trim() : null,
  });
}

function emptyKlaviyoJourneyMetricBindings() {
  return {
    add_to_cart_metric_id: null,
    add_to_cart_metric_name: null,
    add_to_cart_metric_integration_name: null,
    add_to_cart_metric_integration_category: null,
    add_to_cart_metric_verified_at: null,
    checkout_metric_id: null,
    checkout_metric_name: null,
    checkout_metric_integration_name: null,
    checkout_metric_integration_category: null,
    checkout_metric_verified_at: null,
  };
}

function metricFromRow(data, prefix) {
  return data[`${prefix}_metric_id`] ? Object.freeze({
    id: data[`${prefix}_metric_id`],
    name: data[`${prefix}_metric_name`],
    integrationName: data[`${prefix}_metric_integration_name`],
    integrationCategory: data[`${prefix}_metric_integration_category`],
    verifiedAt: data[`${prefix}_metric_verified_at`],
  }) : null;
}

function metricUpdate(prefix, metricInput, timestamp) {
  const metric = metricInput ? conversionMetric(metricInput) : null;
  return {
    [`${prefix}_metric_id`]: metric?.id || null,
    [`${prefix}_metric_name`]: metric?.name || null,
    [`${prefix}_metric_integration_name`]: metric?.integrationName || null,
    [`${prefix}_metric_integration_category`]: metric?.integrationCategory || null,
    [`${prefix}_metric_verified_at`]: metric ? timestamp : null,
  };
}

function authorityFromEmbeddedTransaction(transaction) {
  if (!transaction || transaction.surface !== 'shopify_embedded' || transaction.user_id !== null ||
    transaction.return_target !== '/shopify/app/platforms') throw new Error('EMBEDDED_OAUTH_TRANSACTION_REQUIRED');
  required(transaction.workspace_id, 'transaction.workspace_id');
  required(transaction.shop_id, 'transaction.shop_id');
  providerName(transaction.provider);
  return Object.freeze({
    authority: 'server_resolved_workspace',
    workspace_id: transaction.workspace_id,
    source: 'shopify_verified_session',
  });
}

function createCanonicalWorkspaceProviderConnectionStore({ client, vault, now = () => new Date() } = {}) {
  if (!client || typeof client.from !== 'function') throw new TypeError('Supabase server client is required');
  if (!vault || typeof vault.encrypt !== 'function' || typeof vault.decrypt !== 'function') {
    throw new TypeError('provider token vault is required');
  }

  async function beginAccountSelection({ authority, provider: providerInput, accessToken, refreshToken = null, expiresAt = null, refreshExpiresAt = null, scopes = [] } = {}) {
    const workspace = requireServerWorkspaceAuthority(authority);
    const provider = providerName(providerInput);
    required(accessToken, 'accessToken');
    const row = {
      workspace_id: workspace.workspace_id,
      provider,
      status: 'pending_account_selection',
      active_account_id: null,
      active_account_name: null,
      source_currency: null,
      monthly_plan_cost: null,
      selected_accounts: [],
      conversion_metric_id: null,
      conversion_metric_name: null,
      conversion_metric_integration_name: null,
      conversion_metric_integration_category: null,
      conversion_metric_verified_at: null,
      ...emptyKlaviyoJourneyMetricBindings(),
      access_token_envelope: vault.encrypt(accessToken, tokenContext(workspace.workspace_id, provider, 'access')),
      refresh_token_envelope: refreshToken ? vault.encrypt(refreshToken, tokenContext(workspace.workspace_id, provider, 'refresh')) : null,
      access_token_expires_at: expiresAt,
      refresh_token_expires_at: refreshExpiresAt,
      granted_scopes: Array.isArray(scopes) ? [...scopes] : [],
      last_authorized_via: workspace.source,
      account_verified_at: null,
      connected_at: null,
      disconnected_at: null
    };
    const { data, error } = await client.from(TABLE)
      .insert(row)
      .select('workspace_id,provider,status,connection_version,updated_at')
      .maybeSingle();
    if (error?.code === '23505') {
      const reopenQuery = client.from(TABLE);
      if (typeof reopenQuery.update !== 'function') throw new Error('CANONICAL_CONNECTION_ALREADY_EXISTS');
      const { data: reopened, error: reopenError } = await reopenQuery
        .update({ ...row, updated_at: now().toISOString() })
        .eq('workspace_id', workspace.workspace_id)
        .eq('provider', provider)
        .in('status', ['disconnected', 'revoked'])
        .select('workspace_id,provider,status,connection_version,updated_at')
        .maybeSingle();
      if (reopenError) throw new Error('CANONICAL_CONNECTION_WRITE_FAILED');
      if (!reopened) throw new Error('CANONICAL_CONNECTION_ALREADY_EXISTS');
      return reopened;
    }
    if (error) throw new Error('CANONICAL_CONNECTION_WRITE_FAILED');
    return data;
  }

  async function writeFromOAuthTransaction({ transaction, accessToken, refreshToken = null, expiresAt = null, refreshExpiresAt = null, scopes = [] } = {}) {
    const authority = authorityFromEmbeddedTransaction(transaction);
    return beginAccountSelection({ authority, provider: transaction.provider, accessToken, refreshToken, expiresAt, refreshExpiresAt, scopes });
  }

  async function readStatus({ authority, provider: providerInput } = {}) {
    const workspace = requireServerWorkspaceAuthority(authority);
    const provider = providerName(providerInput);
    const { data, error } = await client.from(TABLE)
      .select('provider,status,active_account_id,active_account_name,source_currency,monthly_plan_cost,selected_accounts,connection_version,updated_at')
      .eq('workspace_id', workspace.workspace_id)
      .eq('provider', provider)
      .maybeSingle();
    if (error) throw new Error('CANONICAL_CONNECTION_READ_FAILED');
    return data || null;
  }

  async function resolveConnected({ authority, provider: providerInput } = {}) {
    const workspace = requireServerWorkspaceAuthority(authority);
    const provider = providerName(providerInput);
    const { data, error } = await client.from(TABLE)
      .select('status,active_account_id,source_currency,monthly_plan_cost,selected_accounts,access_token_envelope,refresh_token_envelope,access_token_expires_at,refresh_token_expires_at,granted_scopes,connection_version,conversion_metric_id,conversion_metric_name,conversion_metric_integration_name,conversion_metric_integration_category,conversion_metric_verified_at,add_to_cart_metric_id,add_to_cart_metric_name,add_to_cart_metric_integration_name,add_to_cart_metric_integration_category,add_to_cart_metric_verified_at,checkout_metric_id,checkout_metric_name,checkout_metric_integration_name,checkout_metric_integration_category,checkout_metric_verified_at')
      .eq('workspace_id', workspace.workspace_id)
      .eq('provider', provider)
      .eq('status', 'connected')
      .maybeSingle();
    if (error) throw new Error('CANONICAL_CONNECTION_READ_FAILED');
    if (!data) return null;
    return Object.freeze({
      provider,
      status: data.status,
      activeAccountId: data.active_account_id,
      sourceCurrency: data.source_currency,
      selectedAccounts: Array.isArray(data.selected_accounts) ? data.selected_accounts : [],
      monthlyPlanCost: data.monthly_plan_cost,
      conversionMetric: data.conversion_metric_id ? Object.freeze({
        id: data.conversion_metric_id,
        name: data.conversion_metric_name,
        integrationName: data.conversion_metric_integration_name,
        integrationCategory: data.conversion_metric_integration_category,
        verifiedAt: data.conversion_metric_verified_at,
      }) : null,
      journeyMetrics: Object.freeze({
        addToCart: metricFromRow(data, 'add_to_cart'),
        checkout: metricFromRow(data, 'checkout'),
        purchase: data.conversion_metric_id ? Object.freeze({
          id: data.conversion_metric_id,
          name: data.conversion_metric_name,
          integrationName: data.conversion_metric_integration_name,
          integrationCategory: data.conversion_metric_integration_category,
          verifiedAt: data.conversion_metric_verified_at,
        }) : null,
      }),
      accessToken: vault.decrypt(data.access_token_envelope, tokenContext(workspace.workspace_id, provider, 'access')),
      refreshToken: data.refresh_token_envelope
        ? vault.decrypt(data.refresh_token_envelope, tokenContext(workspace.workspace_id, provider, 'refresh'))
        : null,
      accessTokenExpiresAt: data.access_token_expires_at,
      refreshTokenExpiresAt: data.refresh_token_expires_at,
      grantedScopes: Array.isArray(data.granted_scopes) ? Object.freeze([...data.granted_scopes]) : Object.freeze([]),
      version: data.connection_version
    });
  }

  async function readKlaviyo(authorityInput) {
    const authority = requireServerWorkspaceAuthority(authorityInput);
    const { data, error } = await client.from(TABLE)
      .select('status,active_account_id,monthly_plan_cost,source_currency,selected_accounts,connection_version,access_token_envelope,refresh_token_envelope')
      .eq('workspace_id', authority.workspace_id).eq('provider', 'klaviyo').maybeSingle();
    if (error) throw new Error('CONNECTION_READ_FAILED');
    if (!data) return null;
    return Object.freeze({
      ...data,
      email_monthly_plan_cost: data.monthly_plan_cost,
      account_currency: data.source_currency,
      updated_at: data.connection_version,
      accessToken: data.status === 'revoked' || !data.access_token_envelope ? null : vault.decrypt(data.access_token_envelope, tokenContext(authority.workspace_id, 'klaviyo', 'access')),
      refreshToken: data.status === 'revoked' || !data.refresh_token_envelope ? null : vault.decrypt(data.refresh_token_envelope, tokenContext(authority.workspace_id, 'klaviyo', 'refresh')),
    });
  }

  async function readPendingProvider({ authority: authorityInput, provider: providerInput } = {}) {
    const authority = requireServerWorkspaceAuthority(authorityInput);
    const provider = providerName(providerInput);
    const { data, error } = await client.from(TABLE)
      .select('status,active_account_id,active_account_name,source_currency,selected_accounts,connection_version,access_token_envelope,refresh_token_envelope,access_token_expires_at,refresh_token_expires_at,granted_scopes')
      .eq('workspace_id', authority.workspace_id).eq('provider', provider).maybeSingle();
    if (error) throw new Error('CONNECTION_READ_FAILED');
    if (!data) return null;
    return Object.freeze({
      ...data,
      accessToken: data.status === 'pending_account_selection' && data.access_token_envelope
        ? vault.decrypt(data.access_token_envelope, tokenContext(authority.workspace_id, provider, 'access')) : null,
      refreshToken: data.status === 'pending_account_selection' && data.refresh_token_envelope
        ? vault.decrypt(data.refresh_token_envelope, tokenContext(authority.workspace_id, provider, 'refresh')) : null,
      accessTokenExpiresAt: data.access_token_expires_at,
      refreshTokenExpiresAt: data.refresh_token_expires_at,
      grantedScopes: Array.isArray(data.granted_scopes) ? Object.freeze([...data.granted_scopes]) : Object.freeze([]),
    });
  }

  async function refreshGoogle({ authority: authorityInput, version, status, accessToken, refreshToken, expiresAt, scopes = [] } = {}) {
    const authority = requireServerWorkspaceAuthority(authorityInput);
    if (!Number.isInteger(version) || version < 1) throw new Error('CONNECTION_CHANGED');
    if (!['pending_account_selection', 'connected'].includes(status)) throw new Error('GOOGLE_CONNECTION_STATE_INVALID');
    const timestamp = now().toISOString();
    const { data, error } = await client.from(TABLE).update({
      access_token_envelope: vault.encrypt(required(accessToken, 'accessToken'), tokenContext(authority.workspace_id, 'google_ads', 'access')),
      refresh_token_envelope: vault.encrypt(required(refreshToken, 'refreshToken'), tokenContext(authority.workspace_id, 'google_ads', 'refresh')),
      access_token_expires_at: required(expiresAt, 'expiresAt'),
      granted_scopes: Array.isArray(scopes) ? [...scopes] : [],
      connection_version: version + 1,
      updated_at: timestamp,
    }).eq('workspace_id', authority.workspace_id).eq('provider', 'google_ads')
      .eq('connection_version', version).eq('status', status)
      .select('connection_version').maybeSingle();
    if (error) throw new Error('CONNECTION_WRITE_FAILED');
    if (!data) throw Object.assign(new Error('CONNECTION_CHANGED'), { code: 'CONNECTION_CHANGED', status: 409 });
    return data;
  }

  async function completeAccountSelection({ authority: authorityInput, provider: providerInput, version, accounts } = {}) {
    const authority = requireServerWorkspaceAuthority(authorityInput);
    const provider = providerName(providerInput);
    if (!Number.isInteger(version) || version < 1) throw new Error('CONNECTION_CHANGED');
    const verifiedAccounts = selectedAccounts(provider, accounts);
    const primary = verifiedAccounts[0];
    const timestamp = now().toISOString();
    const { data, error } = await client.from(TABLE).update({
      status: 'connected',
      active_account_id: primary.id,
      active_account_name: primary.name,
      source_currency: primary.currency,
      selected_accounts: verifiedAccounts,
      monthly_plan_cost: null,
      conversion_metric_id: null,
      conversion_metric_name: null,
      conversion_metric_integration_name: null,
      conversion_metric_integration_category: null,
      conversion_metric_verified_at: null,
      ...emptyKlaviyoJourneyMetricBindings(),
      account_verified_at: timestamp,
      connected_at: timestamp,
      disconnected_at: null,
      connection_version: version + 1,
      updated_at: timestamp,
    }).eq('workspace_id', authority.workspace_id).eq('provider', provider)
      .eq('connection_version', version).eq('status', 'pending_account_selection')
      .select('active_account_id').maybeSingle();
    if (error) throw new Error('CONNECTION_WRITE_FAILED');
    if (!data) throw Object.assign(new Error('CONNECTION_CHANGED'), { code: 'CONNECTION_CHANGED', status: 409 });
  }

  async function readKlaviyoStatus(authorityInput) {
    const row = await readStatus({ authority: authorityInput, provider: 'klaviyo' });
    if (!row) return null;
    return Object.freeze({ status: row.status, email_monthly_plan_cost: row.monthly_plan_cost, account_currency: row.source_currency });
  }

  async function refreshKlaviyo({ authority: authorityInput, version, accessToken, refreshToken }) {
    const authority = requireServerWorkspaceAuthority(authorityInput);
    if (!Number.isInteger(version) || version < 1) throw new Error('CONNECTION_CHANGED');
    const { data, error } = await client.from(TABLE).update({
      access_token_envelope: vault.encrypt(required(accessToken, 'accessToken'), tokenContext(authority.workspace_id, 'klaviyo', 'access')),
      refresh_token_envelope: refreshToken ? vault.encrypt(refreshToken, tokenContext(authority.workspace_id, 'klaviyo', 'refresh')) : null,
      connection_version: version + 1,
      updated_at: now().toISOString(),
    }).eq('workspace_id', authority.workspace_id).eq('provider', 'klaviyo')
      .eq('connection_version', version).eq('status', 'pending_account_selection')
      .select('connection_version').maybeSingle();
    if (error) throw new Error('CONNECTION_WRITE_FAILED');
    if (!data) throw Object.assign(new Error('CONNECTION_CHANGED'), { code: 'CONNECTION_CHANGED', status: 409 });
  }

  async function refreshConnectedKlaviyo({ authority: authorityInput, version, accessToken, refreshToken, expiresAt, refreshExpiresAt = null, scopes = [] } = {}) {
    const authority = requireServerWorkspaceAuthority(authorityInput);
    if (!Number.isInteger(version) || version < 1) throw new Error('CONNECTION_CHANGED');
    const timestamp = now().toISOString();
    const { data, error } = await client.from(TABLE).update({
      access_token_envelope: vault.encrypt(required(accessToken, 'accessToken'), tokenContext(authority.workspace_id, 'klaviyo', 'access')),
      refresh_token_envelope: vault.encrypt(required(refreshToken, 'refreshToken'), tokenContext(authority.workspace_id, 'klaviyo', 'refresh')),
      access_token_expires_at: required(expiresAt, 'expiresAt'),
      refresh_token_expires_at: refreshExpiresAt,
      granted_scopes: Array.isArray(scopes) ? [...scopes] : [],
      connection_version: version + 1,
      updated_at: timestamp,
    }).eq('workspace_id', authority.workspace_id).eq('provider', 'klaviyo')
      .eq('connection_version', version).eq('status', 'connected')
      .select('connection_version').maybeSingle();
    if (error) throw new Error('CONNECTION_WRITE_FAILED');
    if (!data) throw Object.assign(new Error('CONNECTION_CHANGED'), { code: 'CONNECTION_CHANGED', status: 409 });
    return data;
  }

  async function completeKlaviyo({ authority: authorityInput, version, account, cost }) {
    const authority = requireServerWorkspaceAuthority(authorityInput);
    if (!Number.isInteger(version) || version < 1) throw new Error('CONNECTION_CHANGED');
    const [verifiedAccount] = selectedAccounts('klaviyo', [account]);
    const timestamp = now().toISOString();
    const { data, error } = await client.from(TABLE).update({
      status: 'connected',
      active_account_id: verifiedAccount.id,
      active_account_name: verifiedAccount.name,
      source_currency: verifiedAccount.currency,
      selected_accounts: [verifiedAccount],
      monthly_plan_cost: cost,
      conversion_metric_id: null,
      conversion_metric_name: null,
      conversion_metric_integration_name: null,
      conversion_metric_integration_category: null,
      conversion_metric_verified_at: null,
      ...emptyKlaviyoJourneyMetricBindings(),
      account_verified_at: timestamp,
      connected_at: timestamp,
      disconnected_at: null,
      connection_version: version + 1,
      updated_at: timestamp,
    }).eq('workspace_id', authority.workspace_id).eq('provider', 'klaviyo')
      .eq('connection_version', version).eq('status', 'pending_account_selection')
      .select('active_account_id').maybeSingle();
    if (error) throw new Error('CONNECTION_WRITE_FAILED');
    if (!data) throw Object.assign(new Error('CONNECTION_CHANGED'), { code: 'CONNECTION_CHANGED', status: 409 });
  }

  async function disconnectKlaviyo({ authority: authorityInput, version }) {
    const authority = requireServerWorkspaceAuthority(authorityInput);
    if (!Number.isInteger(version) || version < 1) throw new Error('CONNECTION_CHANGED');
    const timestamp = now().toISOString();
    const { data, error } = await client.from(TABLE).update({
      status: 'disconnected',
      active_account_id: null,
      active_account_name: null,
      source_currency: null,
      monthly_plan_cost: null,
      selected_accounts: [],
      conversion_metric_id: null,
      conversion_metric_name: null,
      conversion_metric_integration_name: null,
      conversion_metric_integration_category: null,
      conversion_metric_verified_at: null,
      ...emptyKlaviyoJourneyMetricBindings(),
      access_token_envelope: null,
      refresh_token_envelope: null,
      access_token_expires_at: null,
      refresh_token_expires_at: null,
      disconnected_at: timestamp,
      connection_version: version + 1,
      updated_at: timestamp,
    }).eq('workspace_id', authority.workspace_id).eq('provider', 'klaviyo')
      .eq('connection_version', version).eq('status', 'connected')
      .select('status,connection_version').maybeSingle();
    if (error) throw new Error('CONNECTION_WRITE_FAILED');
    if (!data) throw Object.assign(new Error('CONNECTION_CHANGED'), {code: 'CONNECTION_CHANGED', status: 409});
    return data;
  }

  async function disconnectMeta({ authority: authorityInput, version }) {
    const authority = requireServerWorkspaceAuthority(authorityInput);
    if (!Number.isInteger(version) || version < 1) throw new Error('CONNECTION_CHANGED');
    const timestamp = now().toISOString();
    const { data, error } = await client.from(TABLE).update({
      status: 'disconnected',
      active_account_id: null,
      active_account_name: null,
      source_currency: null,
      monthly_plan_cost: null,
      selected_accounts: [],
      conversion_metric_id: null,
      conversion_metric_name: null,
      conversion_metric_integration_name: null,
      conversion_metric_integration_category: null,
      conversion_metric_verified_at: null,
      access_token_envelope: null,
      refresh_token_envelope: null,
      access_token_expires_at: null,
      refresh_token_expires_at: null,
      granted_scopes: [],
      account_verified_at: null,
      disconnected_at: timestamp,
      connection_version: version + 1,
      updated_at: timestamp,
    }).eq('workspace_id', authority.workspace_id).eq('provider', 'meta')
      .eq('connection_version', version).eq('status', 'connected')
      .select('status,connection_version').maybeSingle();
    if (error) throw new!ÛˆYHšÛ]š^[Ë\™]žH•žHYØZ[ÜËX]ÛÙ]‚ˆËX]ÛˆÛÝHœÙXÛÛ™\žKXXÝ[ÛœÈˆÛÛ[X[™›ÜHšÛ]š^[ËXXØÛÝ[[[Ù[ˆÛÛ[X[™H‹KZYHØ[˜Ù[ÜËX]Û‚ˆÜË[[Ù[‚ˆÜË\ÝXÚÏ˜ˆˆŸBˆ	ÖÈ›Y]H‹™ÛÛÙÛWØYÈ—Kš[˜ÛY\ÊY
H	‰ˆ›ÝšY\]˜Z[X›HÈË\ÝXÚÈYH‰ÚYKXXØÛÝ[ÈˆØ\H˜˜\ÙH‚ˆË[[Ù[YH‰ÚYKXXØÛÝ[[[Ù[ˆXY[™ÏH”Ù[XÝ	ÛX™[HXØÛÝ[‚ˆË\ÝXÚÈØ\H˜˜\ÙH‚ˆË\\˜YÜ˜\”Ù[XÝ™]ÙY[ˆH[™ÈXØÛÝ[È™]\›™YžH	ÛX™[KÜË\\˜YÜ˜\‚ˆËXÚÚXÙK[\ÝYH‰ÚYKXÚÚXÙHˆ˜[YOH‰ÚYKXXØÛÝ[ÈˆX™[H‰ÛX™[HXØÛÝ[Èˆ]Z[ÏH–[ÝHØ[ˆÛÛ›™XÝ\ÈÈXØÛÝ[Ëˆˆ][\OÜËXÚÚXÙK[\Ý‚ˆËX]ÛˆYH‰ÚYK\Ø]™Hˆ˜\šX[Hœš[X\žH”Ø]™H[™ÛÛ›™XÝÜËX]Û‚ˆÜË\ÝXÚÏ‚ˆËX]ÛˆÛÝHœÙXÛÛ™\žKXXÝ[ÛœÈˆÛÛ[X[™›ÜH‰ÚYKXXØÛÝ[[[Ù[ˆÛÛ[X[™H‹KZYHØ[˜Ù[ÜËX]Û‚ˆÜË[[Ù[‚ˆÜË\ÝXÚÏ˜ˆˆŸBˆÜË\ÙXÝ[Û˜ÂŸB‚™[˜Ý[Ûˆ™[™\‘[X™YY]›Ü›\ÊØÛY[Y›ÝšY\“Ð]][˜X›Y›ÝšY\]˜Z[Xš[]HHß_JHÂˆYˆ
\[ÙˆÛY[YOOHœÝš[™ÈˆXÛY[Yš[J
JH›ÝÈ™]È\Q\œ›ÜŠ˜ÛY[Y\È™\]Z\™YŠNÂˆÛÛœÝ›ÝšY\œÈHÂˆÚYˆ›Y]H‹X™[ˆ“Y]H‹\ØÜš\[ÛŽˆ“Y]HY™\\Ú[™È\™›Ü›X[˜ÙH[™Ü[™ˆŸKˆÚYˆ™ÛÛÙÛWØYÈ‹X™[ˆ‘ÛÛÙÛHYÈ‹\ØÜš\[ÛŽˆ‘ÛÛÙÛHYÈ\™›Ü›X[˜ÙH[™Ü[™ˆŸKˆÚYˆšÛ]š^[È‹X™[ˆ’Û]š^[È‹\ØÜš\[ÛŽˆ‘[XZ[\™›Ü›X[˜ÙH[™[ÛH[ˆÛÜÝˆŸKˆÚYˆZÝÚÈ‹X™[ˆ•ZÕÚÈ‹\ØÜš\[ÛŽˆ•ZÕÚÈÛÛ›™XÝ[Ûˆ\È\šÙY›ÜˆH]\ˆ™[X\ÙKˆ‹\šÙYˆY_KˆÚYˆœ[\™\Ý‹X™[ˆ”[\™\Ý‹\ØÜš\[ÛŽˆ”[\™\ÝÛÛ›™XÝ[Ûˆ\È›Ý]˜Z[X›H[ˆ\È™[X\ÙKˆ‹\šÙYˆY_KˆNÂˆÛÛœÝÙXÝ[ÛœÈH›ÝšY\œË›X\

›ÝšY\ŠHOˆ™[™\”›ÝšY\”ÙXÝ[ÛŠ›ÝšY\‹›ÝšY\“Ð]][˜X›Y›ÝšY\‹œ\šÙYÈ˜[ÙHˆ›ÝšY\]˜Z[Xš[]VÜ›ÝšY\‹šYHÏÈ›ÝšY\“Ð]][˜X›Y
JKš›Ú[Š—ˆŠNÂˆ™]\›ˆYØÝ\H[‚[[™ÏH™[ˆ‚XY‚ˆ	ÙØÝ[Y[XY
ØÛY[Y]Nˆ‘]HÛÝ\˜Ù\È8 %YÕX›HŸJ_BÚXY‚›ÙO‚ˆ	Ø\˜]šYØ][ÛŠ
_BˆË\YÙHXY[™ÏH‘]HÛÝ\˜Ù\È‚ˆË[[šÈÛÝH˜œ™XYÜ[X‹XXÝ[ÛœÈˆ™YH‹ÜÚÜYžKØ\’ÛYOÜË[[šÏ‚ˆËX˜[›™\ˆYHœÝ]\ÈˆXY[™ÏH‘]HÛÝ\˜Ù\ÈˆÛ™OHš[™›ÈˆY[ÜËX˜[›™\‚ˆ]ˆYHœ™YÛÛÙÛKXXØÙ\[˜ÙHˆY[‚ˆË\ÙXÝ[ÛˆXY[™ÏH‘ÛÛÙÛHYÈXØÙ\[˜ÙHÚXÚÈ‚ˆË\ÝXÚÈØ\H˜˜\ÙH‚ˆË\\˜YÜ˜\•\ÈÛ™K][YHÚXÚÈ™XYÈHÙ[XÝYÛÛÙÛHYÈXØÛÝ[ËÝ[™\™YÈ[™\™›Ü›X[˜ÙHX^\ÜÙ]Ü›Ý\È›ÝYÚHÛÛ\]YMHÛÛ˜XÝˆ]Ù\È›ÝÜš]H]\Ù]Œ‹ÜË\\˜YÜ˜\‚ˆË\\˜YÜ˜\YHœ™YÛÛÙÛK[Y\ÜØYÙHˆ\šXK[]™OHœÛ]HÜË\\˜YÜ˜\‚ˆËX]ÛˆYHœ™YÛÛÙÛK\[ˆˆ˜\šX[Hœš[X\žH”[ˆ™XY[Û›HXØÙ\[˜ÙOÜËX]Û‚ˆ]ˆYHœ™YÛÛÙÛKY]\Ù]\Ý\‚ˆË\ÝXÚÈØ\H˜˜\ÙH‚ˆË\\˜YÜ˜\•\ÈÛÛ›ÛYXØÙ\[˜ÙHX^HÜš]HÛ›H™X[›ÝšY\‹]™\šYšYYÛÛÙÛHYÈ›ÝÜÈÈ]\Ù]Œ‹ˆH™\šYšYY[\H™\Ý[Üš]\È›ÈÞ[]XÈ›ÝÜÈ[™Ù\È›Ý[˜X›HØÚY[\ÈÜˆ˜XÚÙš[ÜË\\˜YÜ˜\‚ˆË\\˜YÜ˜\YHœ™YÛÛÙÛKY]\Ù][Y\ÜØYÙHˆ\šXK[]™OHœÛ]HÜË\\˜YÜ˜\‚ˆËX]ÛˆYHœ™YÛÛÙÛKY]\Ù]\[ˆˆÛ™OH˜Üš]XØ[”[ˆÛÛ›ÛY]\Ù]ŒˆXØÙ\[˜ÙOÜËX]Û‚ˆÜË\ÝXÚÏ‚ˆÙ]‚ˆÜË\ÝXÚÏ‚ˆÜË\ÙXÝ[Û‚ˆÙ]‚ˆ]ˆYHœ™Ë[Y]KXXØÙ\[˜ÙHˆY[‚ˆË\ÙXÝ[ÛˆXY[™ÏH“Y]HXØÙ\[˜ÙHÚXÚÈ‚ˆË\ÝXÚÈØ\H˜˜\ÙH‚ˆË\\˜YÜ˜\•\ÈÛ™K][YHÚXÚÈ™XYÈHÙ[XÝYY]HXØÛÝ[È[™Z[H[œÚYÚÈ›ÝYÚHÛÛ\]YMÛÛ˜XÝˆ]Ù\È›ÝÜš]H]\Ù]Œ‹ÜË\\˜YÜ˜\‚ˆË\\˜YÜ˜\YHœ™Ë[Y]K[Y\ÜØYÙHˆ\šXK[]™OHœÛ]HÜË\\˜YÜ˜\‚ˆËX]ÛˆYHœ™Ë[Y]K\[ˆˆ˜\šX[Hœš[X\žH”[ˆ™XY[Û›HXØÙ\[˜ÙOÜËX]Û‚ˆ]ˆYHœ™Ë[Y]KY]\Ù]\Ý\‚ˆË\ÝXÚÈØ\H˜˜\ÙH‚ˆË\\˜YÜ˜\•\ÈÛÛ›ÛYXØÙ\[˜ÙHX^HÜš]H™X[›ÝšY\‹]™\šYšYYY]H›ÝÜÈÈ]\Ù]Œ‹ˆH™\šYšYY[\H™\Ý[Üš]\È›ÈÞ[]XÈ›ÝÜÈ[™Ù\È›Ý[˜X›HØÚY[\ÈÜˆ˜XÚÙš[ÜË\\˜YÜ˜\‚ˆË\\˜YÜ˜\YHœ™Ë[Y]KY]\Ù][Y\ÜØYÙHˆ\šXK[]™OHœÛ]HÜË\\˜YÜ˜\‚ˆËX]ÛˆYHœ™Ë[Y]KY]\Ù]\[ˆˆÛ™OH˜Üš]XØ[”[ˆÛÛ›ÛY]\Ù]ŒˆXØÙ\[˜ÙOÜËX]Û‚ˆÜË\ÝXÚÏ‚ˆÙ]‚ˆÜË\ÝXÚÏ‚ˆÜË\ÙXÝ[Û‚ˆÙ]‚ˆ]ˆYHœ™‹ZÛ]š^[ËXXØÙ\[˜ÙHˆY[‚ˆË\ÙXÝ[ÛˆXY[™ÏH’Û]š^[ÈXØÙ\[˜ÙHÚXÚÈ‚ˆË\ÝXÚÈØ\H˜˜\ÙH‚ˆË\\˜YÜ˜\•\ÈÛ™K][YHÚXÚÈ™XYÈH™\šYšYYÛ]š^[ÈXØÛÝ[Ø[\ZYÛˆ[™›ÝÈ™\Ü[™ÈT\Ëˆ]Ù\È›ÝÜš]H]\Ù]Œ‹ÜË\\˜YÜ˜\‚ˆË\\˜YÜ˜\YHœ™‹ZÛ]š^[Ë[Y\ÜØYÙHˆ\šXK[]™OHœÛ]HÜË\\˜YÜ˜\‚ˆËX]ÛˆYHœ™‹ZÛ]š^[Ë\[ˆˆ˜\šX[Hœš[X\žH”[ˆ™XY[Û›HXØÙ\[˜ÙOÜËX]Û‚ˆË\\˜YÜ˜\YHœ™KZÛ]š^[ËYXYÛ›ÜÝXË[Y\ÜØYÙHˆ\šXK[]™OHœÛ]HÜË\\˜YÜ˜\‚ˆËX]ÛˆYHœ™KZÛ]š^[ËYXYÛ›ÜÝXË\[ˆ”[ˆŽÙ\›Ý\›™^HXYÛ›ÜÝXÏÜËX]Û‚ˆ]ˆYHœ™‹ZÛ]š^[ËXÍ‹\Ý\‚ˆË\ÝXÚÈØ\H˜˜\ÙH‚ˆË\\˜YÜ˜\•\ÈÛÛ›ÛYXØÙ\[˜ÙHX^HÜš]H™X[™\šYšYYÛ]š^[È›ÝÜÈÈ]\Ù]Œ‹ˆ]™]™\ˆÜ™X]\ÈÞ[]XÈ›ÝÜÈ[™Ù\È›Ý[˜X›HØÚY[Y›ÙXÝ[ÛˆXÝ]˜][Û‹ÜË\\˜YÜ˜\‚ˆË\\˜YÜ˜\YHœ™‹ZÛ]š^[ËXÍ‹[Y\ÜØYÙHˆ\šXK[]™OHœÛ]HÜË\\˜YÜ˜\‚ˆËX]ÛˆYHœ™‹ZÛ]š^[ËXÍ‹\[ˆˆÛ™OH˜Üš]XØ[”[ˆÛÛ›ÛY]\Ù]ŒˆXØÙ\[˜ÙOÜËX]Û‚ˆÜË\ÝXÚÏ‚ˆÙ]‚ˆ]ˆYHœ™‹ZÛ]š^[Ë[Y]šXË\Ý\ˆY[‚ˆË\ÝXÚÈØ\H˜˜\ÙH‚ˆË\\˜YÜ˜\ÛÛ™š\›Z[™ÈÝÜ™\ÈÛ›H\ÈÛÜšÜÜXÙHXØÛÝ[	ÜÈ™\šYšYY™\Ü[™ÈY]šXËˆ]Ù\È›ÝÜš]H]\Ù]Œ‹ÜË\\˜YÜ˜\‚ˆË\Ù[XÝYHœ™‹ZÛ]š^[Ë[Y]šXÈˆX™[H”XÙYÜ™\ˆY]šXÈÜË\Ù[XÝ‚ˆËX]ÛˆYHœ™‹ZÛ]š^[Ë[Y]šXËXÛÛ™š\›Hˆ˜\šX[Hœš[X\žHÛÛ™š\›HY]šXÈ[™ÛÛ[YOÜËX]Û‚ˆÜË\ÝXÚÏ‚ˆÙ]‚ˆÜË\ÝXÚÏ‚ˆÜË\ÙXÝ[Û‚ˆÙ]‚ˆ]ˆYH˜Ý\œ™[˜ÞK\Ù]\ˆY[‚ˆË\ÙXÝ[ÛˆXY[™ÏH‘š[š\ÚÙ]\‚ˆËX]Ûˆ˜\šX[Hœš[X\žHˆÛÛ[X[™›ÜHœ]›Ü›\ËXÝ\œ™[˜ÞK[[Ù[ˆÛÛ[X[™H‹K\ÚÝÈÚÛÜÙH™\Ü[™ÈÝ\œ™[˜ÞOÜËX]Û‚ˆÜË\ÙXÝ[Û‚ˆÙ]‚ˆË[[Ù[YHœ]›Ü›\ËXÝ\œ™[˜ÞK[[Ù[ˆXY[™ÏHÚÛÜÙH™\Ü[™ÈÝ\œ™[˜ÞHˆÚ^™OHœÛX[LL‚ˆË\ÝXÚÈØ\H˜˜\ÙH‚ˆË\\˜YÜ˜\•\È\È[™\[™[œ›ÛHÚÜYžH[™›ÝšY\ˆXØÛÝ[Ý\œ™[˜ÚY\ËÜË\\˜YÜ˜\‚ˆË\Ù[XÝYHœ™\Ü[™ËXÝ\œ™[˜ÞHˆX™[H”™\Ü[™ÈÝ\œ™[˜ÞH‚ˆ	ÖÈ•–H‹•TÑ‹‘UTˆ‹‘Ð”‹’”H‹Ó–H‹UQ‹ÐQ‹Òˆ‹”ÑRÈ‹““ÒÈ‹‘ÒÈ‹”ˆ—K›X\
Ý\œ™[˜ÞHOˆË[Ü[Ûˆ˜[YOH‰ØÝ\œ™[˜Þ_H‰ØÝ\œ™[˜Þ_OÜË[Ü[Û˜
Kš›Ú[ŠˆŠ_BˆÜË\Ù[XÝ‚ˆË\\˜YÜ˜\YHœ]›Ü›\ËXÝ\œ™[˜ÞK[Y\ÜØYÙHˆ\šXK[]™OHœÛ]HÜË\\˜YÜ˜\‚ˆÜË\ÝXÚÏ‚ˆËX]ÛˆÛÝHœÙXÛÛ™\žKXXÝ[ÛœÈˆÛÛ[X[™›ÜHœ]›Ü›\ËXÝ\œ™[˜ÞK[[Ù[ˆÛÛ[X[™H‹KZYHØ[˜Ù[ÜËX]Û‚ˆËX]ÛˆYHœØ]™K\™\Ü[™ËXÝ\œ™[˜ÞHˆÛÝHœš[X\žKXXÝ[Ûˆˆ˜\šX[Hœš[X\žH”Ø]™H[™ÛÛ[YOÜËX]Û‚ˆÜË[[Ù[‚ˆ]ˆYHœ›ÝšY\‹\ÙXÝ[ÛœÈˆY[‰ÜÙXÝ[ÛœßOÙ]‚ˆÜË\YÙO‚ˆØÜš\‚ˆ


HOˆÂˆ\ÙHÝšXÝŽÂˆÛÛœÝÝ]\ÈHØÝ[Y[™Ù][[Y[žRY
œÝ]\ÈŠNÂˆÛÛœÝÝ\œ™[˜ÞTÙ]\HØÝ[Y[™Ù][[Y[žRY
˜Ý\œ™[˜ÞK\Ù]\ŠNÂˆÛÛœÝ›ÝšY\”ÙXÝ[ÛœÈHØÝ[Y[™Ù][[Y[žRY
œ›ÝšY\‹\ÙXÝ[ÛœÈŠNÂˆÛÛœÝÝ\œ™[˜ÞHHØÝ[Y[™Ù][[Y[žRY
œ™\Ü[™ËXÝ\œ™[˜ÞHŠNÂˆÛÛœÝØ]™PÝ\œ™[˜ÞHHØÝ[Y[™Ù][[Y[žRY
œØ]™K\™\Ü[™ËXÝ\œ™[˜ÞHŠNÂˆÛÛœÝÝ\œ™[˜ÞSY\ÜØYÙHHØÝ[Y[™Ù][[Y[žRY
œ]›Ü›\ËXÝ\œ™[˜ÞK[Y\ÜØYÙHŠNÂˆÛÛœÝÛÛÙÛPXØÙ\[˜ÙT[™[HØÝ[Y[™Ù][[Y[žRY
œ™YÛÛÙÛKXXØÙ\[˜ÙHŠNÂˆÛÛœÝÛÛÙÛPXØÙ\[˜ÙP]ÛˆHØÝ[Y[™Ù][[Y[žRY
œ™YÛÛÙÛK\[ˆŠNÂˆÛÛœÝÛÛÙÛPXØÙ\[˜ÙSY\ÜØYÙHHØÝ[Y[™Ù][[Y[žRY
œ™YÛÛÙÛK[Y\ÜØYÙHŠNÂˆÛÛœÝÛÛÙÛQ]\Ù]XØÙ\[˜ÙP]ÛˆHØÝ[Y[™Ù][[Y[žRY
œ™YÛÛÙÛKY]\Ù]\[ˆŠNÂˆÛÛœÝÛÛÙÛQ]\Ù]XØÙ\[˜ÙSY\ÜØYÙHHØÝ[Y[™Ù][[Y[žRY
œ™YÛÛÙÛKY]\Ù][Y\ÜØYÙHŠNÂˆÛÛœÝY]PXØÙ\[˜ÙT[™[HØÝ[Y[™Ù][[Y[žRY
œ™Ë[Y]KXXØÙ\[˜ÙHŠNÂˆÛÛœÝY]PXØÙ\[˜ÙP]ÛˆHØÝ[Y[™Ù][[Y[žRY
œ™Ë[Y]K\[ˆŠNÂˆÛÛœÝY]PXØÙ\[˜ÙSY\ÜØYÙHHØÝ[Y[™Ù][[Y[žRY
œ™Ë[Y]K[Y\ÜØYÙHŠNÂˆÛÛœÝY]Q]\Ù]XØÙ\[˜ÙP]ÛˆHØÝ[Y[™Ù][[Y[žRY
œ™Ë[Y]KY]\Ù]\[ˆŠNÂˆÛÛœÝY]Q]\Ù]XØÙ\[˜ÙSY\ÜØYÙHHØÝ[Y[™Ù][[Y[žRY
œ™Ë[Y]KY]\Ù][Y\ÜØYÙHŠNÂˆÛÛœÝXØÙ\[˜ÙT[™[HØÝ[Y[™Ù][[Y[žRY
œ™‹ZÛ]š^[ËXXØÙ\[˜ÙHŠNÂˆÛÛœÝXØÙ\[˜ÙP]ÛˆHØÝ[Y[™Ù][[Y[žRY
œ™‹ZÛ]š^[Ë\[ˆŠNÂˆÛÛœÝXØÙ\[˜ÙSY\ÜØYÙHHØÝ[Y[™Ù][[Y[žRY
œ™‹ZÛ]š^[Ë[Y\ÜØYÙHŠNÂˆÛÛœÝY]šXÔÝ\HØÝ[Y[™Ù][[Y[žRY
œ™‹ZÛ]š^[Ë[Y]šXË\Ý\ŠNÂˆÛÛœÝY]šXÔÙ[XÝHØÝ[Y[™Ù][[Y[žRY
œ™‹ZÛ]š^[Ë[Y]šXÈŠNÂˆÛÛœÝY]šXÐÛÛ™š\›HHØÝ[Y[™Ù][[Y[žRY
œ™‹ZÛ]š^[Ë[Y]šXËXÛÛ™š\›HŠNÂˆÛÛœÝ]\Ù]XØÙ\[˜ÙP]ÛˆHØÝ[Y[™Ù][[Y[žRY
œ™‹ZÛ]š^[ËXÍ‹\[ˆŠNÂˆÛÛœÝ]\Ù]XØÙ\[˜ÙSY\ÜØYÙHHØÝ[Y[™Ù][[Y[žRY
œ™‹ZÛ]š^[ËXÍ‹[Y\ÜØYÙHŠNÂˆÛÛœÝ›Ý\›™^QXYÛ›ÜÝXÐ]ÛˆHØÝ[Y[™Ù][[Y[žRY
œ™KZÛ]š^[ËYXYÛ›ÜÝXË\[ˆŠNÂˆÛÛœÝ›Ý\›™^QXYÛ›ÜÝXÓY\ÜØYÙHHØÝ[Y[™Ù][[Y[žRY
œ™KZÛ]š^[ËYXYÛ›ÜÝXË[Y\ÜØYÙHŠNÂˆÛÛœÝ\˜[\ÈH™]ÈT“ÙX\˜Ú\˜[\ÊØØ][Û‹œÙX\˜Ú
NÂˆÛÛœÝÙ\ÜÚ[Û”™\]Y\ÝH\Þ[˜È
]Ü[ÛœÈHßJHOˆÂˆYˆ
]Ú[™ÝËœÚÜYžH\[ÙˆÚ[™ÝËœÚÜYžKšYÚÙ[ˆOOH™[˜Ý[ÛˆŠH›ÝÈ™]È\œ›ÜŠ”ÒÔQ–WÔÑTÔÒSÓ—Ô‘TURT‘QŠNÂˆÛÛœÝÚÙ[ˆH]ØZ]Ú[™ÝËœÚÜYžKšYÚÙ[Š
NÂˆÛÛœÝ™\ÜÛœÙHH]ØZ]™]Ú
]Ë‹‹›Ü[ÛœËXY\œÎˆÐ]]Üš^˜][ÛŽˆ™X\™\ˆˆ
ÈÚÙ[‹‹‹ŠÜ[ÛœË˜›ÙHÈÈÛÛ[U\HŽˆ˜\XØ][Û‹ÚœÛÛˆŸHˆßJ__JNÂˆÛÛœÝ›ÙHH]ØZ]™\ÜÛœÙKšœÛÛŠ
K˜Ø]Ú


HOˆ
ßJJNÂˆYˆ
\™\ÜÛœÙK›ÚÊH›ÝÈ™]È\œ›ÜŠ›ÙK˜ÛÙH”‘TUQTÕÑRSQŠNÂˆ™]\›ˆ›ÙNÂˆNÂˆÛÛœÝÚÝÔ›ÝšY\œÈH™\Ü[™ÐÝ\œ™[˜ÞHOˆÂˆÝ\œ™[˜ÞTÙ]\šY[ˆHYNÂˆ›ÝšY\”ÙXÝ[ÛœËšY[ˆH˜[ÙNÂˆÝ]\ËšY[ˆHYNÂˆ
	Ú[š]X[^™PYXØÛÝ[ËÔÝš[™Ê
_JJ
NÂˆ
	Ú[š]X[^™RÛ]š^[ÐXØÛÝ[ËÔÝš[™Ê
_JJ
NÂˆNÂˆÛÛœÝØYÙ][™ÜÈH\Þ[˜È

HOˆÂˆžHÂˆÛÛœÝÙ][™ÜÈH]ØZ]Ù\ÜÚ[Û”™\]Y\Ý
‹Ø\KÜÚÜYžKÝÛÜšÜÜXÙKÜÙ][™ÜÈŠNÂˆYˆ
Ù][™ÜËœÝ]\ÈOOH˜ÛÛ™šYÝ\™YŠH™]\›ˆÚÝÔ›ÝšY\œÊÙ][™ÜËœ™\Ü[™×ØÝ\œ™[˜ÞJNÂˆÝ\œ™[˜ÞTÙ]\šY[ˆH˜[ÙNÂˆ›ÝšY\”ÙXÝ[ÛœËšY[ˆHYNÂˆÝ]\ËšY[ˆHYNÂˆHØ]ÚÂˆÝ\œ™[˜ÞTÙ]\šY[ˆHYNÂˆ›ÝšY\”ÙXÝ[ÛœËšY[ˆHYNÂˆÝ]\ËšY[ˆH˜[ÙNÂˆÝ]\ËœÙ]]šX]JšXY[™È‹“Ü[ˆYÕX›Hœ›ÛHÚÜYžHYZ[ˆŠNÂˆÝ]\ËœÙ]]šX]JÛ™H‹˜Üš]XØ[ŠNÂˆÝ]\Ë^ÛÛ[HˆŽÂˆBˆNÂˆØ]™PÝ\œ™[˜ÞK˜Y]™[\Ý[™\Š˜ÛXÚÈ‹\Þ[˜È

HOˆÂˆØ]™PÝ\œ™[˜ÞK™\ØX›YHYNÂˆØ]™PÝ\œ™[˜ÞK›ØY[™ÈHYNÂˆžHÂˆÛÛœÝ™\Ý[H]ØZ]Ù\ÜÚ[Û”™\]Y\Ý
‹Ø\KÜÚÜYžKÝÛÜšÜÜXÙKÜ™\Ü[™ËXÝ\œ™[˜ÞH‹ÛY]Ùˆ”ÔÕ‹›ÙNˆ”ÓÓ‹œÝš[™ÚYžJØÝ\œ™[˜ÞNˆÝš[™ÊÝ\œ™[˜ÞK˜[YJ_J_JNÂˆÚÝÔ›ÝšY\œÊ™\Ý[œ™\Ü[™×ØÝ\œ™[˜ÞJNÂˆHØ]Ú
\œ›ÜŠHÂˆÝ\œ™[˜ÞSY\ÜØYÙK^ÛÛ[H\œ›Ü‹›Y\ÜØYÙHOOH”‘TÔ•S‘×ÐÕT”‘SÖWÐS‘PQWÐÓÓ‘’QÕT‘QˆÈÝ\œ™[˜ÞH\È[™XYHÛÛ™šYÝ\™Yˆ™[ØY]HÛÝ\˜Ù\Ëˆˆˆ”X\ÙHÚÛÜÙHHÝ\ÜYÝ\œ™[˜ÞH[™žHYØZ[‹ˆŽÂˆHš[˜[HÈØ]™PÝ\œ™[˜ÞK™\ØX›YH˜[ÙNÈØ]™PÝ\œ™[˜ÞK›ØY[™ÈH˜[ÙNÈBˆJNÂˆYˆ
\˜[\Ë™Ù]
˜XØÙ\[˜ÙHŠHOOHœ™‹ZÛ]š^[ÈŠHÂˆXØÙ\[˜ÙT[™[šY[ˆH˜[ÙNÂˆÛÛœÝÚÝÐXØÙ\[˜ÙT™\Ý[H™\Ý[OˆÂˆXØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H™\Ý[œÝ]\ÈOOH”TÔ×Ô—Ñ—ÒÓU’VS×Ô‘PQÓÓ“WÔ‘Q“QÒ‚ˆÈ”TÔÈ8 %XØÛÝ[Ø[\ZYÛ‹›ÝË[YH[™–ÚXÚÜÈÝXØÙYYYˆ]\Ù]ŒˆÜš]\Îˆˆ‚ˆˆ•HXØÙ\[˜ÙH™\Ý[ÛÝ[›Ý™H™\šYšYYˆŽÂˆNÂˆXØÙ\[˜ÙP]Û‹˜Y]™[\Ý[™\Š˜ÛXÚÈ‹\Þ[˜È

HOˆÂˆXØÙ\[˜ÙP]Û‹™\ØX›YHYNÂˆXØÙ\[˜ÙP]Û‹›ØY[™ÈHYNÂˆXØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H”[›š[™ÈH™XY[Û›HÚXÚÜø )ˆŽÂˆžHÂˆÛÛœÝ™\Ý[H]ØZ]Ù\ÜÚ[Û”™\]Y\Ý
‹Ø\KÜÚÜYžKÜ›ÝšY\œËÚÛ]š^[ËÜ[[YKÜ™Y›YÚ‹ÛY]Ùˆ”ÔÕŸJNÂˆÚÝÐXØÙ\[˜ÙT™\Ý[
™\Ý[
NÂˆHØ]Ú
\œ›ÜŠHÂˆYˆ
\œ›Ü‹›Y\ÜØYÙHOOH’ÓU’VS×Ô‘Q“QÒÓQU’P×Ô‘TURT‘QŠHÂˆžHÂˆÛÛœÝ\ØÛÝ™\žHH]ØZ]Ù\ÜÚ[Û”™\]Y\Ý
‹Ø\KÜÚÜYžKÜ›ÝšY\œËÚÛ]š^[ËÜ[[YKÛY]šXÜÈŠNÂˆY]šXÔÙ[XÝœ™\XÙPÚ[™[Š
NÂˆ\ØÛÝ™\žK˜Ø[™Y]\Ë™›Ü‘XXÚ
Ø[™Y]HOˆÂˆÛÛœÝÜ[ÛˆHØÝ[Y[˜Ü™X]Q[[Y[
œË[Ü[ÛˆŠNÂˆÜ[Û‹˜[YHHØ[™Y]KšYÂˆÜ[Û‹^ÛÛ[HØ[™Y]K›˜[YH
Èˆ8 %ˆ
ÈØ[™Y]Kš[YÜ˜][Û—Û˜[YH
È
Ø[™Y]Kš[YÜ˜][Û—ØØ]YÛÜžHÈˆ
ˆ
ÈØ[™Y]Kš[YÜ˜][Û—ØØ]YÛÜžH
ÈŠHˆˆˆŠNÂˆY]šXÔÙ[XÝ˜\[™Ú[
Ü[ÛŠNÂˆJNÂˆY]šXÔÙ[XÝ˜[YHH\ØÛÝ™\žK˜Ø[™Y]\ÖÌOËšYˆŽÂˆY]šXÔÝ\šY[ˆH˜[ÙNÂˆXØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H”Ù[XÝH›ÝšY\‹]™\šYšYYØ[\ÈÛÝ\˜ÙKˆYÕX›HÚ[š[™]È^XÝYYÈØ\ÚXÚÛÝ][™XÙYÜ™\ˆY]šXÜÈ›Üˆ\ÈÛÜšÜÜXÙH[™Û]š^[ÈXØÛÝ[ˆŽÂˆHØ]Ú
\ØÛÝ™\žQ\œ›ÜŠHÂˆXØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H×–ÐKVŒNW×^ÌKIË\Ý
\ØÛÝ™\žQ\œ›Ü‹›Y\ÜØYÙHˆŠHÈ\ØÛÝ™\žQ\œ›Ü‹›Y\ÜØYÙHˆ’ÓU’VS×ÓQU’P×ÑTÐÓÕ‘T–WÑRSQŽÂˆBˆH[ÙHÂˆXØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H×–ÐKVŒNW×^ÌKIË\Ý
\œ›Ü‹›Y\ÜØYÙHˆŠHÈ\œ›Ü‹›Y\ÜØYÙHˆ’ÓU’VS×Ô‘Q“QÒÑRSQŽÂˆBˆXØÙ\[˜ÙP]Û‹™\ØX›YH˜[ÙNÂˆHš[˜[HÈXØÙ\[˜ÙP]Û‹›ØY[™ÈH˜[ÙNÈBˆJNÂˆ›Ý\›™^QXYÛ›ÜÝXÐ]Û‹˜Y]™[\Ý[™\Š˜ÛXÚÈ‹\Þ[˜È

HOˆÂˆ›Ý\›™^QXYÛ›ÜÝXÐ]Û‹™\ØX›YHYNÂˆ›Ý\›™^QXYÛ›ÜÝXÐ]Û‹›ØY[™ÈHYNÂˆ›Ý\›™^QXYÛ›ÜÝXÓY\ÜØYÙK^ÛÛ[H”™XY[™ÈHŽÙ\[X™\ˆØ[\ZYÛˆ[™›ÝÈ›Ý\›™^H™\Üø )ˆŽÂˆžHÂˆÛÛœÝ™\Ý[H]ØZ]Ù\ÜÚ[Û”™\]Y\Ý
‹Ø\KÜÚÜYžKÜ›ÝšY\œËÚÛ]š^[ËÜ[[YKÚ›Ý\›™^KYXYÛ›ÜÝXÈ‹ÂˆY]Ùˆ”ÔÕ‹ˆ›ÙNˆ”ÓÓ‹œÝš[™ÚYžJÜ›ÝšY\—Ù]NˆŒŒ‹LKLŽŸJKˆJNÂˆÛÛœÝXYÛ›ÜÝXÈH™\Ý[š›Ý\›™^WÙXYÛ›ÜÝXÜÎÂˆÛÛœÝÝ[[X\žHHÝYÙHOˆÈˆ
ÈÝYÙK˜Ø[\ZYÛ‹œ›Ý×ØÛÝ[
È‹Èˆ
ÈÝYÙK˜Ø[\ZYÛ‹˜ÛÛ™\œÚ[Û—ØÛÝ[
È‹ˆˆ
ÈÝYÙK™›ÝËœ›Ý×ØÛÝ[
È‹Èˆ
ÈÝYÙK™›ÝË˜ÛÛ™\œÚ[Û—ØÛÝ[Âˆ›Ý\›™^QXYÛ›ÜÝXÓY\ÜØYÙK^ÛÛ[H™\Ý[œÝ]\ÈOOH”TÔ×Ô—ÑWÒÓU’VS×Ò“ÕT“‘VWÑPQÓ“ÔÕPÈ‚ˆÈ”TÔÈ8 %Œ‹LKLŽˆ›ÝÜËØÛÛ™\œÚ[ÛœÎˆXÙYÜ™\ˆˆ
ÈÝ[[X\žJXYÛ›ÜÝXËœ\˜Ú\ÙJH
ÈŽÈYYÈØ\ˆ
ÈÝ[[X\žJXYÛ›ÜÝXË˜YÝ×ØØ\
H
ÈŽÈÚXÚÛÝ]ˆ
ÈÝ[[X\žJXYÛ›ÜÝXË˜ÚXÚÛÝ]
H
È‹ˆ[›X]ÚYY\ÜØYÙHÙ^\ÎˆUÈˆ
È
XYÛ›ÜÝXË˜YÝ×ØØ\˜Ø[\ZYÛ‹[›X]ÚYÚÙ^WØÛÝ[
ÈXYÛ›ÜÝXË˜YÝ×ØØ\™›ÝË[›X]ÚYÚÙ^WØÛÝ[
H
È‹ÚXÚÛÝ]ˆ
È
XYÛ›ÜÝXË˜ÚXÚÛÝ]˜Ø[\ZYÛ‹[›X]ÚYÚÙ^WØÛÝ[
ÈXYÛ›ÜÝXË˜ÚXÚÛÝ]™›ÝË[›X]ÚYÚÙ^WØÛÝ[
H
È‹ˆ]\Ù]ŒˆÜš]\Îˆˆ‚ˆˆ•H›Ý\›™^HXYÛ›ÜÝXÈ™\Ý[ÛÝ[›Ý™H™\šYšYYˆŽÂˆHØ]Ú
\œ›ÜŠHÂˆ›Ý\›™^QXYÛ›ÜÝXÓY\ÜØYÙK^ÛÛ[H×–ÐKVŒNW×^ÌKIË\Ý
\œ›Ü‹›Y\ÜØYÙHˆŠHÈ\œ›Ü‹›Y\ÜØYÙHˆ’ÓU’VS×Ô‘Q“QÒÑRSQŽÂˆ›Ý\›™^QXYÛ›ÜÝXÐ]Û‹™\ØX›YH˜[ÙNÂˆHš[˜[HÈ›Ý\›™^QXYÛ›ÜÝXÐ]Û‹›ØY[™ÈH˜[ÙNÈBˆJNÂˆ]\Ù]XØÙ\[˜ÙP]Û‹˜Y]™[\Ý[™\Š˜ÛXÚÈ‹\Þ[˜È

HOˆÂˆ]\Ù]XØÙ\[˜ÙP]Û‹™\ØX›YHYNÂˆ]\Ù]XØÙ\[˜ÙP]Û‹›ØY[™ÈHYNÂˆ]\Ù]XØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H”[›š[™ÈÛ™HÛÛ›ÛY]\Ù]ŒˆXØÙ\[˜Ùx )ˆŽÂˆžHÂˆÛÛœÝ™\Ý[H]ØZ]Ù\ÜÚ[Û”™\]Y\Ý
‹Ø\KÜÚÜYžKÜ›ÝšY\œËÚÛ]š^[ËÜ[[YKØXØÙ\[˜ÙH‹ÂˆY]Ùˆ”ÔÕ‹ˆ›ÙNˆ”ÓÓ‹œÝš[™ÚYžJØÛÛ™š\›X][ÛŽˆ”•S—Ô—Ñ—ÐÍ—ÒÓU’VS×ÕÔ’UHŸJKˆJNÂˆ]\Ù]XØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H™\Ý[œÝ]\ÈOOH”TÔ×Ô—Ñ—ÐÍ—ÒÓU’VS×ÑUTÑUÕÔ’UH‚ˆÈ”TÔÈ8 %][\Yˆˆ
È™\Ý[˜][\Y
È‹\œÚ\ÝYˆˆ
È™\Ý[œ\œÚ\ÝY
È‹™\šYšYY[\Nˆˆ
È™\Ý[™[\WÜ›ÝšY\—Ü™\Ý[
È‹ˆ‚ˆˆ•H]\Ù]ŒˆXØÙ\[˜ÙH™\Ý[ÛÝ[›Ý™H™\šYšYYˆŽÂˆHØ]Ú
\œ›ÜŠHÂˆ]\Ù]XØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H
×–ÐKVŒNW×^ÌKIË\Ý
\œ›Ü‹›Y\ÜØYÙHˆŠHÈ\œ›Ü‹›Y\ÜØYÙHˆ’ÓU’VS×ÑUTÑUÐPÐÑTSÑWÑRSQŠH
È‹ˆÈ›Ý™]žNÈ™]šY]È[[YH]šY[˜ÙKˆŽÂˆHš[˜[HÈ]\Ù]XØÙ\[˜ÙP]Û‹›ØY[™ÈH˜[ÙNÈBˆJNÂˆY]šXÐÛÛ™š\›K˜Y]™[\Ý[™\Š˜ÛXÚÈ‹\Þ[˜È

HOˆÂˆY]šXÐÛÛ™š\›K™\ØX›YHYNÂˆY]šXÐÛÛ™š\›K›ØY[™ÈHYNÂˆXØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H•™\šYžZ[™È[™š[™[™ÈHÛ]š^[ÈÛÛ[Y\˜ÙHY]šXÜø )ˆŽÂˆžHÂˆ]ØZ]Ù\ÜÚ[Û”™\]Y\Ý
‹Ø\KÜÚÜYžKÜ›ÝšY\œËÚÛ]š^[ËÜ[[YKÛY]šXÜËÜÙ[XÝ‹ÛY]Ùˆ”ÔÕ‹›ÙNˆ”ÓÓ‹œÝš[™ÚYžJÛY]šX×ÚYˆÝš[™ÊY]šXÔÙ[XÝ˜[YHˆŠ_J_JNÂˆY]šXÔÝ\šY[ˆHYNÂˆXØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H“Y]šXÈÛÛ™š\›YYˆ[›š[™ÈH™XY[Û›HXØÙ\[˜Ùx )ˆŽÂˆÚÝÐXØÙ\[˜ÙT™\Ý[
]ØZ]Ù\ÜÚ[Û”™\]Y\Ý
‹Ø\KÜÚÜYžKÜ›ÝšY\œËÚÛ]š^[ËÜ[[YKÜ™Y›YÚ‹ÛY]Ùˆ”ÔÕŸJJNÂˆHØ]Ú
\œ›ÜŠHÂˆXØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H×–ÐKVŒNW×^ÌKIË\Ý
\œ›Ü‹›Y\ÜØYÙHˆŠHÈ\œ›Ü‹›Y\ÜØYÙHˆ’ÓU’VS×Ô‘Q“QÒÑRSQŽÂˆY]šXÐÛÛ™š\›K™\ØX›YH˜[ÙNÂˆHš[˜[HÈY]šXÐÛÛ™š\›K›ØY[™ÈH˜[ÙNÈBˆJNÂˆBˆYˆ
\˜[\Ë™Ù]
˜XØÙ\[˜ÙHŠHOOHœ™Ë[Y]HŠHÂˆY]PXØÙ\[˜ÙT[™[šY[ˆH˜[ÙNÂˆY]PXØÙ\[˜ÙP]Û‹˜Y]™[\Ý[™\Š˜ÛXÚÈ‹\Þ[˜È

HOˆÂˆY]PXØÙ\[˜ÙP]Û‹™\ØX›YHYNÂˆY]PXØÙ\[˜ÙP]Û‹›ØY[™ÈHYNÂˆY]PXØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H”[›š[™ÈH™XY[Û›HÚXÚÜø )ˆŽÂˆžHÂˆÛÛœÝ™\Ý[H]ØZ]Ù\ÜÚ[Û”™\]Y\Ý
‹Ø\KÜÚÜYžKÜ›ÝšY\œËÛY]KÜ[[YKÜ™Y›YÚ‹ÛY]Ùˆ”ÔÕŸJNÂˆY]PXØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H™\Ý[œÝ]\ÈOOH”TÔ×Ô—Ñ×ÑÓQUWÔ‘PQÓÓ“WÔ‘Q“QÒ‚ˆÈ”TÔÈ8 %ˆ
È™\Ý[œÙ[XÝYØXØÛÝ[ØÛÝ[
ÈˆXØÛÝ[
ÊKˆ
È™\Ý[œ›Ý×ØÛÝ[
Èˆ™\šYšYY›ÝÊÊK[YH[™–ÚXÚÜÈÝXØÙYYYˆ]\Ù]ŒˆÜš]\Îˆˆ‚ˆˆ•HXØÙ\[˜ÙH™\Ý[ÛÝ[›Ý™H™\šYšYYˆŽÂˆHØ]Ú
\œ›ÜŠHÂˆY]PXØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H×–ÐKVŒNW×^ÌKIË\Ý
\œ›Ü‹›Y\ÜØYÙHˆŠHÈ\œ›Ü‹›Y\ÜØYÙHˆ“QUWÔ‘Q“QÒÑRSQŽÂˆY]PXØÙ\[˜ÙP]Û‹™\ØX›YH˜[ÙNÂˆHš[˜[HÈY]PXØÙ\[˜ÙP]Û‹›ØY[™ÈH˜[ÙNÈBˆJNÂˆY]Q]\Ù]XØÙ\[˜ÙP]Û‹˜Y]™[\Ý[™\Š˜ÛXÚÈ‹\Þ[˜È

HOˆÂˆY]Q]\Ù]XØÙ\[˜ÙP]Û‹™\ØX›YHYNÂˆY]Q]\Ù]XØÙ\[˜ÙP]Û‹›ØY[™ÈHYNÂˆY]Q]\Ù]XØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H”[›š[™ÈÛ™HÛÛ›ÛY]\Ù]ŒˆXØÙ\[˜Ùx )ˆŽÂˆžHÂˆÛÛœÝ™\Ý[H]ØZ]Ù\ÜÚ[Û”™\]Y\Ý
‹Ø\KÜÚÜYžKÜ›ÝšY\œËÛY]KÜ[[YKØXØÙ\[˜ÙH‹ÂˆY]Ùˆ”ÔÕ‹ˆ›ÙNˆ”ÓÓ‹œÝš[™ÚYžJØÛÛ™š\›X][ÛŽˆ”•S—Ô—Ñ×ÑWÓQUWÕÔ’UHŸJKˆJNÂˆY]Q]\Ù]XØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H™\Ý[œÝ]\ÈOOH”TÔ×Ô—Ñ×ÑWÓQUWÑUTÑUÕÔ’UH‚ˆÈ”TÔÈ8 %][\Yˆˆ
È™\Ý[˜][\Y
È‹\œÚ\ÝYˆˆ
È™\Ý[œ\œÚ\ÝY
È‹™\šYšYY[\Nˆˆ
È™\Ý[™[\WÜ›ÝšY\—Ü™\Ý[
È‹ˆ‚ˆˆ•H]\Ù]ŒˆXØÙ\[˜ÙH™\Ý[ÛÝ[›Ý™H™\šYšYYˆŽÂˆHØ]Ú
\œ›ÜŠHÂˆY]Q]\Ù]XØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H
×–ÐKVŒNW×^ÌKIË\Ý
\œ›Ü‹›Y\ÜØYÙHˆŠHÈ\œ›Ü‹›Y\ÜØYÙHˆ“QUWÑUTÑUÐPÐÑTSÑWÑRSQŠH
È‹ˆÈ›Ý™]žNÈ™]šY]È[[YH]šY[˜ÙKˆŽÂˆHš[˜[HÈY]Q]\Ù]XØÙ\[˜ÙP]Û‹›ØY[™ÈH˜[ÙNÈBˆJNÂˆBˆYˆ
\˜[\Ë™Ù]
˜XØÙ\[˜ÙHŠHOOHœ™YÛÛÙÛHŠHÂˆÛÛÙÛPXØÙ\[˜ÙT[™[šY[ˆH˜[ÙNÂˆÛÛÙÛPXØÙ\[˜ÙP]Û‹˜Y]™[\Ý[™\Š˜ÛXÚÈ‹\Þ[˜È

HOˆÂˆÛÛÙÛPXØÙ\[˜ÙP]Û‹™\ØX›YHYNÂˆÛÛÙÛPXØÙ\[˜ÙP]Û‹›ØY[™ÈHYNÂˆÛÛÙÛPXØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H”[›š[™ÈH™XY[Û›HÚXÚÜø )ˆŽÂˆžHÂˆÛÛœÝ™\Ý[H]ØZ]Ù\ÜÚ[Û”™\]Y\Ý
‹Ø\KÜÚÜYžKÜ›ÝšY\œËÙÛÛÙÛWØYËÜ[[YKÜ™Y›YÚ‹ÛY]Ùˆ”ÔÕŸJNÂˆÛÛÙÛPXØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H™\Ý[œÝ]\ÈOOH”TÔ×Ô—ÑÑÑÓÓÑÓWÔ‘PQÓÓ“WÔ‘Q“QÒ‚ˆÈ”TÔÈ8 %ˆ
È™\Ý[œÙ[XÝYØXØÛÝ[ØÛÝ[
ÈˆXØÛÝ[
ÊKˆ
È™\Ý[œ›Ý×ØÛÝ[
Èˆ™\šYšYY›ÝÊÊHXÜ›ÜÜÈÝ[™\™[™\™›Ü›X[˜ÙHX^ˆ[YH[™–ÚXÚÜÈÝXØÙYYYˆ]\Ù]ŒˆÜš]\Îˆˆ‚ˆˆ•HXØÙ\[˜ÙH™\Ý[ÛÝ[›Ý™H™\šYšYYˆŽÂˆHØ]Ú
\œ›ÜŠHÂˆÛÛÙÛPXØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H×–ÐKVŒNW×^ÌKIË\Ý
\œ›Ü‹›Y\ÜØYÙHˆŠHÈ\œ›Ü‹›Y\ÜØYÙHˆ‘ÓÓÑÓWÔ‘Q“QÒÑRSQŽÂˆÛÛÙÛPXØÙ\[˜ÙP]Û‹™\ØX›YH˜[ÙNÂˆHš[˜[HÈÛÛÙÛPXØÙ\[˜ÙP]Û‹›ØY[™ÈH˜[ÙNÈBˆJNÂˆÛÛÙÛQ]\Ù]XØÙ\[˜ÙP]Û‹˜Y]™[\Ý[™\Š˜ÛXÚÈ‹\Þ[˜È

HOˆÂˆÛÛÙÛQ]\Ù]XØÙ\[˜ÙP]Û‹™\ØX›YHYNÂˆÛÛÙÛQ]\Ù]XØÙ\[˜ÙP]Û‹›ØY[™ÈHYNÂˆÛÛÙÛQ]\Ù]XØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H”[›š[™ÈÛ™HÛÛ›ÛY]\Ù]ŒˆXØÙ\[˜Ùx )ˆŽÂˆžHÂˆÛÛœÝ™\Ý[H]ØZ]Ù\ÜÚ[Û”™\]Y\Ý
‹Ø\KÜÚÜYžKÜ›ÝšY\œËÙÛÛÙÛWØYËÜ[[YKØXØÙ\[˜ÙH‹ÂˆY]Ùˆ”ÔÕ‹ˆ›ÙNˆ”ÓÓ‹œÝš[™ÚYžJØÛÛ™š\›X][ÛŽˆ”•S—Ô—ÑÑWÑÓÓÑÓWÕÔ’UHŸJKˆJNÂˆÛÛÙÛQ]\Ù]XØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H™\Ý[œÝ]\ÈOOH”TÔ×Ô—ÑÑWÑÓÓÑÓWÑUTÑUÕÔ’UH‚ˆÈ”TÔÈ8 %][\Yˆˆ
È™\Ý[˜][\Y
È‹\œÚ\ÝYˆˆ
È™\Ý[œ\œÚ\ÝY
È‹™\šYšYY[\Nˆˆ
È™\Ý[™[\WÜ›ÝšY\—Ü™\Ý[
È‹ˆ‚ˆˆ•H]\Ù]ŒˆXØÙ\[˜ÙH™\Ý[ÛÝ[›Ý™H™\šYšYYˆŽÂˆHØ]Ú
\œ›ÜŠHÂˆÛÛÙÛQ]\Ù]XØÙ\[˜ÙSY\ÜØYÙK^ÛÛ[H
×–ÐKVŒNW×^ÌKIË\Ý
\œ›Ü‹›Y\ÜØYÙHˆŠHÈ\œ›Ü‹›Y\ÜØYÙHˆ‘ÓÓÑÓWÑUTÑUÐPÐÑTSÑWÑRSQŠH
È‹ˆÈ›Ý™]žNÈ™]šY]È[[YH]šY[˜ÙKˆŽÂˆHš[˜[HÈÛÛÙÛQ]\Ù]XØÙ\[˜ÙP]Û‹›ØY[™ÈH˜[ÙNÈBˆJNÂˆBˆØÝ[Y[œ]Y\žTÙ[XÝÜ[
œËX]Û–Ù]K\›ÝšY\—HŠK™›Ü‘XXÚ

]ÛŠHOˆ]Û‹˜Y]™[\Ý[™\Š˜ÛXÚÈ‹\Þ[˜È

HOˆÂˆ]Û‹™\ØX›YHYNÂˆ]Û‹›ØY[™ÈHYNÂˆÝ]\ËœÙ]]šX]JšXY[™È‹“Ü[š[™ÈÙXÝ\™HÛÛ›™XÝ[ÛˆŠNÂˆÝ]\ËœÙ]]šX]JÛ™H‹š[™›ÈŠNÂˆÝ]\Ë^ÛÛ[H–[ÝHÚ[ÛÛ[YHÛˆH›ÝšY\‰ÜÈ]]Üš^˜][ÛˆYÙKˆŽÂˆžHÂˆYˆ
]Ú[™ÝËœÚÜYžH\[ÙˆÚ[™ÝËœÚÜYžKšYÚÙ[ˆOOH™[˜Ý[ÛˆŠH›ÝÈ™]È\œ›ÜŠ
NÂˆÛÛœÝÚÙ[ˆH]ØZ]Ú[™ÝËœÚÜYžKšYÚÙ[Š
NÂˆÛÛœÝ™\ÜÛœÙHH]ØZ]™]Ú
‹Ø\KÜÚÜYžKÜ›ÝšY\œËÈˆ
È[˜ÛÙUT’PÛÛ\Û™[
]Û‹™]\Ù]œ›ÝšY\ŠH
È‹ÛØ]]ÜÝ\‹ÂˆY]Ùˆ”ÔÕ‹ˆXY\œÎˆÐ]]Üš^˜][ÛŽˆ™X\™\ˆˆ
ÈÚÙ[ŸKˆJNÂˆÛÛœÝ›ÙHH]ØZ]™\ÜÛœÙKšœÛÛŠ
K˜Ø]Ú


HOˆ
ßJJNÂˆYˆ
\™\ÜÛœÙK›ÚÊH›ÝÈ™]È\œ›ÜŠ›ÙK˜ÛÙHÓÓ“‘PÕSÓ—ÔÕT•ÑRSQŠNÂˆYˆ
›ÙK›˜]šYØ][ÛˆOOHÜÛ]™[ˆ\[Ùˆ›ÙK˜]]Üš^˜][Û—Ý\›OOHœÝš[™ÈŠH›ÝÈ™]È\œ›ÜŠ’S•SQÓÐUUÔ‘TÔÓ”ÑHŠNÂˆÜ[Š›ÙK˜]]Üš^˜][Û—Ý\›—ÝÜŠNÂˆHØ]Ú
\œ›ÜŠHÂˆÝ]\ËœÙ]]šX]JšXY[™È‹ÛÛ›™XÝ[ÛˆÛÝ[›Ý™HÝ\YŠNÂˆÝ]\ËœÙ]]šX]JÛ™H‹˜Üš]XØ[ŠNÂˆÝ]\ËšY[ˆH˜[ÙNÂˆÝ]\Ë^ÛÛ[H×–ÐKVŒNW×^ÌKIË\Ý
\œ›Ü‹›Y\ÜØYÙHˆŠHÈ\œ›Ü‹›Y\ÜØYÙHˆÓÓ“‘PÕSÓ—ÔÕT•ÑRSQŽÂˆ]Û‹™\ØX›YH˜[ÙNÂˆ]Û‹›ØY[™ÈH˜[ÙNÂˆBˆJJNÂˆØYÙ][™ÜÊ
NÂˆYˆ
\˜[\Ë™Ù]
›Ø]]Ù\œ›ÜˆŠHOOHœ™X]]Üš^˜][Û—Ü™\]Z\™Yˆ	‰ˆ\˜[\Ë™Ù]
œ›ÝšY\ˆŠHOOH›Y]HŠHÂˆÛÛœÝ™XÛÛ›™XÝHØÝ[Y[™Ù][[Y[žRY
›Y]KXÛÛ›™XÝXXÝ[ÛˆŠNÂˆÛÛœÝY\ÜØYÙHHØÝ[Y[™Ù][[Y[žRY
›Y]K[Y\ÜØYÙHŠNÂˆYˆ
™XÛÛ›™XÝ
H™XÛÛ›™XÝ^ÛÛ[H”™XÛÛ›™XÝY]HŽÂˆYˆ
Y\ÜØYÙJHY\ÜØYÙK^ÛÛ[H“Y]H]]Üš^˜][Ûˆ]\Ý™H™[™]ÙY™Y›Ü™HXØÛÝ[Ù[XÝ[Û‹ˆŽÂˆÝ]\ËœÙ]]šX]JšXY[™È‹”™XÛÛ›™XÝY]HŠNÂˆÝ]\ËœÙ]]šX]JÛ™H‹˜Üš]XØ[ŠNÂˆÝ]\Ë^ÛÛ[HYÕX›HÛÝ[›Ý™\šYžHH˜[YY]H]]Üš^˜][Û‹ˆŽÂˆÝ]\ËšY[ˆH˜[ÙNÂˆH[ÙHYˆ
\˜[\Ëš\Ê›Ø]]ØÛÛ›™XÝYŠJHÂˆÝ]\ËœÙ]]šX]JšXY[™È‹”›ÝšY\ˆ]]Üš^™YŠNÂˆÝ]\ËœÙ]]šX]JÛ™H‹œÝXØÙ\ÜÈŠNÂˆÝ]\Ë^ÛÛ[HXØÛÝ[Ù[XÝ[Ûˆ\È™^ˆŽÂˆH[ÙHYˆ
\˜[\Ëš\Ê›Ø]]Ù\œ›ÜˆŠJHÂˆÝ]\ËœÙ]]šX]JšXY[™È‹ÛÛ›™XÝ[ÛˆØ\È›ÝÛÛ\]YŠNÂˆÝ]\ËœÙ]]šX]JÛ™H‹˜Üš]XØ[ŠNÂˆÝ]\Ë^ÛÛ[H”X\ÙHžHYØZ[‹ˆŽÂˆBˆJJ
NÂˆÜØÜš\‚Ø›ÙO‚Ú[˜ÂŸB‚™[˜Ý[ÛˆÙ][X™YYXY\œÊ™\ÊHÂˆ™\ËœÙ]
ØXÚKPÛÛ›Û‹››Ë\ÝÜ™K›ËXØXÚK]\Ý\™]˜[Y]K›ÞK\™]˜[Y]KX^XYÙOLŠNÂˆ™\ËœÙ]
Ñ‹PØXÚKPÛÛ›Û‹››Ë\ÝÜ™HŠNÂˆ™\ËœÙ]
•™\˜Ù[PÑ‹PØXÚKPÛÛ›Û‹››Ë\ÝÜ™HŠNÂˆ™\ËœÙ]
”Ý\œ›ÙØ]KPÛÛ›Û‹››Ë\ÝÜ™HŠNÂˆ™\ËœÙ]
–PYÕX›KT™[X\ÙH‹SP‘QQÒÓQWÔ‘SPTÑJNÂˆ™\ËœÙ]
ÛÛ[TÙXÝ\š]KTÛXÞH‹™œ˜[YKX[˜Ù\ÝÜœÈÎ‹ËØYZ[‹œÚÜYžK˜ÛÛHÎ‹ËÊ‹›^\ÚÜYžK˜ÛÛHŠNÂŸB‚™[˜Ý[Ûˆ™YÚ\Ý\‘[X™YY\ÛYJ\ØÛY[YJHÂˆYˆ
X\\[Ùˆ\™Ù]OOH™[˜Ý[ÛˆŠH›ÝÈ™]È\Q\œ›ÜŠ˜\™Ù]\È™\]Z\™YŠNÂˆÛÛœÝ[H™[™\‘[X™YY\ÛYJØÛY[YJNÂˆÛÛœÝ[™\ˆH
Ü™\K™\ÊHOˆÂˆÙ][X™YYXY\œÊ™\ÊNÂˆ™]\›ˆ™\Ë\Jš[ŠKœÙ[™
[
NÂˆNÂˆ\™Ù]
‹È‹[™\ŠNÂˆ\™Ù]
‹ÜÚÜYžKØ\‹[™\ŠNÂŸB‚™[˜Ý[Ûˆ™YÚ\Ý\‘[X™YY]›Ü›\Ê\ØÛY[Y›ÝšY\“Ð]][˜X›YH˜[Ù_JHÂˆYˆ
X\\[Ùˆ\™Ù]OOH™[˜Ý[ÛˆŠH›ÝÈ™]È\Q\œ›ÜŠ˜\™Ù]\È™\]Z\™YŠNÂˆÛÛœÝ[H™[™\‘[X™YY]›Ü›\ÊØÛY[Y›ÝšY\“Ð]][˜X›YJNÂˆ\™Ù]
‹ÜÚÜYžKØ\Ü]›Ü›\È‹
Ü™\K™\ÊHOˆÂˆÙ][X™YYXY\œÊ™\ÊNÂˆ™]\›ˆ™\Ë\Jš[ŠKœÙ[™
[
NÂˆJNÂŸB‚›[Ù[K™^ÜÈHØš™XÝ™œ™Y^™JÑSP‘QQÒÓQWÔ‘SPTÑK™YÚ\Ý\‘[X™YY\ÛYK™[™\‘[X™YY\ÛYK™YÚ\Ý\‘[X™YY]›Ü›\Ë™[™\‘[X™YY]›Ü›\ßJNÂ