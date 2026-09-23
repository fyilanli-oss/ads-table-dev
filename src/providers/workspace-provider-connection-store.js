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

  return Object.freeze({ beginAccountSelection, readStatus, resolveConnected });
}

module.exports = Object.freeze({
  TABLE,
  ACTIVE_PROVIDERS,
  createCanonicalWorkspaceProviderConnectionStore
});
