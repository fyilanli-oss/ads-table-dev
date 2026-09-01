'use strict';

function sandboxAdvertiser({productionConfig, advertiserId, advertiserName, sandboxBase} = {}) {
  if (!productionConfig || productionConfig.production || !productionConfig.tiktokSandboxEnabled || !productionConfig.tiktokForceSandboxReports) return null;
  const id = String(advertiserId || '').trim();
  if (!id) throw new Error('TikTok sandbox advertiser id is required');
  return Object.freeze({
    advertiser_id: id,
    advertiser_name: String(advertiserName || '').trim() || 'TikTok Sandbox Advertiser',
    status: 'active', currency: null, sandbox: true,
    reportBase: sandboxBase,
    tokenSource: 'server_sandbox_access_token'
  });
}

module.exports = Object.freeze({ sandboxAdvertiser });
