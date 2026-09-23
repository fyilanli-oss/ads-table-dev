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
    const timestamp = now().toISOString();
    const row = {
      workspace_id: workspace.workspace_id,
      provider,
      status: 'pending_account_selection',
      active_account_id: null,
      active_account_name: null,
      source_currency: null,
      monthly_plan_cost: null,
      access_token_envelope: vault.encrypt(accessToken, tokenContext(workspace.workspace_id, provider, 'access')),
      refresh_token_envelope: refreshToken ? vault.encrypt(refreshToken, tokenContext(workspace.workspace_id, provider, 'refresh')) : null,
      access_token_expires_at: expiresAt,
      refresh_token_expires_at: refreshExpiresAt,
      granted_scopes: Array.isArray(scopes) ? [...scopes] : [],
      last_authorized_via: workspace.source,
      account_verified_at: null,
      connected_at: null,
      disconnected_at: null,
      updated_at: timestamp
    };
    const { data, error } = await client.from(TABLE)
      .insert(row)
      .select('workspace_id,provider,status,connection_version,updated_at')
      .maybeSingle();
    if (error?.code === '23505') throw new Error('CANONICAL_CONNECTION_ALREADY_EXISTS');
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
      .select('provider,status,active_account_id,active_account_name,source_currency,monthly_plan_cost,connection_version,updated_at')
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
      .select('status,active_account_id,source_currency,access_token_envelope,refresh_token_envelope,connection_version')
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
      accessToken: vault.decrypt(data.access_token_envelope, tokenContext(workspace.workspace_id, provider, 'access')),
      refreshToken: data.refresh_token_envelope
        ? vault.decrypt(data.refresh_token_envelope, tokenContext(workspace.workspace_id, provider, 'refresh'))
        : null,
      version: data.connection_version
    });
  }

  async function readKlaviyo(authorityInput) {
    const authority = requireServerWorkspaceAuthority(authorityInput);
    const { data, error } = await client.from(TABLE)
      .select('status,active_account_id,monthly_plan_cost,source_currency,connection_version,access_token_envelope,refresh_token_envelope')
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

  async function completeKlaviyo({ authority: authorityInput, version, account, cost }) {
    const authority = requireServerWorkspaceAuthority(authorityInput);
    if (!Number.isInteger(version) || version < 1) throw new Error('CONNECTION_CHANGED');
    const timestamp = now().toISOString();
    const { data, error } = await client.from(TABLE).update({
      status: 'connected',
      active_account_id: required(account?.id, 'account.id'),
      active_account_name: required(account?.name, 'account.name'),
      source_currency: required(account?.currency, 'account.currency'),
      monthly_plan_cost: cost,
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

  return Object.freeze({
    beginAccountSelection,
    writeFromOAuthTransaction,
    readStatus,
    resolveConnected,
    readKlaviyo,
    readKlaviyoStatus,
    refreshKlaviyo,
    completeKlaviyo,
  });
}

module.exports = Object.freeze({
  TABLE,
  ACTIVE_PROVIDERS,
  authorityFromEmbeddedTransaction,
  createCanonicalWorkspaceProviderConnectionStore
});
