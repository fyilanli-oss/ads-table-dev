'use strict';

const { WorkspaceCanonicalWriteBoundary } = require('../../../funnel-core/workspace-canonical-write-boundary');
const { requireServerWorkspaceAuthority } = require('../../../funnel-core/workspace-dataset-runtime');
const { verifyProviderResult } = require('../workspace-provider-runtime');
const { closedProviderDate } = require('./read-only-preflight');
const { createKlaviyoWorkspaceRunner } = require('./workspace-runner');

const CONFIRMATION = 'RUN_R6_D2_C6_KLAVIYO_WRITE';

function codedError(code, status) {
  return Object.assign(new Error(code), { code, status });
}

function createKlaviyoControlledDatasetAcceptance({
  connectionStore,
  settingsStore,
  providerClient,
  resolveFxRate,
  repository,
  now = () => new Date(),
} = {}) {
  if (!connectionStore || typeof connectionStore.resolveConnected !== 'function') {
    throw new TypeError('canonical connection store is required');
  }
  if (!settingsStore || typeof settingsStore.resolveReportingCurrency !== 'function') {
    throw new TypeError('workspace settings store is required');
  }
  if (!repository || typeof repository.readCanonicalRawFacts !== 'function') {
    throw new TypeError('workspace Dataset V2 read repository is required');
  }
  const runner = createKlaviyoWorkspaceRunner({ providerClient, resolveFxRate });
  const writeBoundary = new WorkspaceCanonicalWriteBoundary({ repository });

  async function execute(authorityInput, confirmation) {
    if (confirmation !== CONFIRMATION) {
      throw codedError('KLAVIYO_DATASET_ACCEPTANCE_CONFIRMATION_REQUIRED', 409);
    }
    const authority = requireServerWorkspaceAuthority(authorityInput);
    try {
      const connection = await connectionStore.resolveConnected({ authority, provider: 'klaviyo' });
      if (!connection) throw new Error('CANONICAL_PROVIDER_CONNECTION_REQUIRED');
      const currency = await settingsStore.resolveReportingCurrency(authority);
      const providerDate = closedProviderDate(now());
      const selectedAccounts = Array.isArray(connection.selectedAccounts) ? connection.selectedAccounts : [];
      if (selectedAccounts.length !== 1 || typeof selectedAccounts[0]?.id !== 'string' || !selectedAccounts[0].id.trim()) {
        throw new Error('KLAVIYO_SINGLE_ACCOUNT_REQUIRED');
      }
      const existingRows = await repository.readCanonicalRawFacts({
        workspace_id: authority.workspace_id,
        from: providerDate,
        to: providerDate,
        platform: 'klaviyo',
        platform_account_id: selectedAccounts[0].id.trim(),
      });
      if (!Array.isArray(existingRows)) throw new Error('WORKSPACE_DATASET_READ_INVALID');
      if (existingRows.length > 0) {
        throw codedError('KLAVIYO_DATASET_ACCEPTANCE_ALREADY_EXECUTED', 409);
      }
      const result = await runner(Object.freeze({
        authority,
        connection,
        reportingCurrency: currency.reportingCurrency,
        currencyVersion: currency.currencyVersion,
        request: Object.freeze({ provider_date: providerDate }),
      }));
      const verified = verifyProviderResult({
        provider: 'klaviyo',
        connection,
        reportingCurrency: currency.reportingCurrency,
        result,
      });
      const persisted = await writeBoundary.write(verified.rows);
      if (!Array.isArray(persisted) || persisted.length !== verified.rows.length) {
        throw new Error('WORKSPACE_DATASET_WRITE_CARDINALITY_MISMATCH');
      }
      return Object.freeze({
        status: 'PASS_R6_D2_C6_KLAVIYO_DATASET_WRITE',
        attempted: verified.rows.length,
        persisted: persisted.length,
        empty_provider_result: verified.providerResultStatus === 'empty',
        provider_result_status: verified.providerResultStatus,
        selected_account_count: verified.selectedAccountCount,
        provider_date: providerDate,
        production_activation: false,
        currency_version: currency.currencyVersion,
      });
    } catch (error) {
      if (error?.code === 'KLAVIYO_DATASET_ACCEPTANCE_ALREADY_EXECUTED') throw error;
      throw codedError('KLAVIYO_DATASET_ACCEPTANCE_FAILED', 503);
    }
  }

  return Object.freeze({ execute });
}

module.exports = Object.freeze({ CONFIRMATION, createKlaviyoControlledDatasetAcceptance });
