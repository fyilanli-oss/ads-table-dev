'use strict';

const { verifyProviderResult } = require('../workspace-provider-runtime');
const { createKlaviyoWorkspaceRunner } = require('./workspace-runner');
const { closedProviderDate } = require('./read-only-preflight');

function codedError(code, status) {
  return Object.assign(new Error(code), { code, status });
}

function onlySelectedAccount(connection) {
  const accounts = Array.isArray(connection?.selectedAccounts) ? connection.selectedAccounts : [];
  if (accounts.length !== 1 || typeof accounts[0]?.id !== 'string' || !accounts[0].id.trim()) {
    throw new Error('KLAVIYO_SINGLE_SELECTED_ACCOUNT_REQUIRED');
  }
  return accounts[0].id.trim();
}

function safeFailure(error) {
  const reason = String(error?.code || error?.message || '');
  if (reason === 'KLAVIYO_REAUTHORIZE') return codedError('KLAVIYO_REAUTHORIZE', 409);
  if (reason === 'CANONICAL_PROVIDER_CONNECTION_REQUIRED' || reason === 'KLAVIYO_CANONICAL_CONNECTION_REQUIRED') {
    return codedError('KLAVIYO_HISTORICAL_INVENTORY_CONNECTION_REQUIRED', 409);
  }
  if (reason.startsWith('WORKSPACE_REPORTING_CURRENCY_')) {
    return codedError('KLAVIYO_HISTORICAL_INVENTORY_CURRENCY_REQUIRED', 409);
  }
  if (reason === 'connection.conversionMetric.id is required') {
    return codedError('KLAVIYO_HISTORICAL_INVENTORY_METRIC_REQUIRED', 409);
  }
  return codedError('KLAVIYO_HISTORICAL_INVENTORY_FAILED', 503);
}

function createKlaviyoHistoricalInventory({
  connectionStore,
  settingsStore,
  providerClient,
  tokenLifecycle = null,
  resolveFxRate,
  now = () => new Date(),
} = {}) {
  if (!connectionStore || typeof connectionStore.resolveConnected !== 'function') {
    throw new TypeError('canonical connection store is required');
  }
  if (!settingsStore || typeof settingsStore.resolveReportingCurrency !== 'function') {
    throw new TypeError('workspace settings store is required');
  }
  if (!providerClient || typeof providerClient.fetchAccount !== 'function' ||
    typeof providerClient.fetchSentCampaignDates !== 'function') {
    throw new TypeError('Klaviyo historical inventory provider client is required');
  }
  const runner = createKlaviyoWorkspaceRunner({ providerClient, resolveFxRate });

  async function execute(authority) {
    try {
      let connection = await connectionStore.resolveConnected({ authority, provider: 'klaviyo' });
      if (!connection) throw new Error('CANONICAL_PROVIDER_CONNECTION_REQUIRED');
      const currency = await settingsStore.resolveReportingCurrency(authority);
      const latestClosedDate = closedProviderDate(now());

      const operation = async activeConnection => {
        const accountId = onlySelectedAccount(activeConnection);
        const account = await providerClient.fetchAccount({
          accessToken: activeConnection.accessToken,
          accountId,
        });
        const discovered = await providerClient.fetchSentCampaignDates({
          accessToken: activeConnection.accessToken,
          timeZone: account.timezone,
        });
        const eligibleDates = discovered.filter(date => date <= latestClosedDate);
        if (eligibleDates.length === 0) {
          return Object.freeze({ providerDate: null, eligibleDates, result: null });
        }
        const providerDate = eligibleDates[0];
        const result = await runner(Object.freeze({
          authority,
          connection: activeConnection,
          reportingCurrency: currency.reportingCurrency,
          currencyVersion: currency.currencyVersion,
          request: Object.freeze({ provider_date: providerDate }),
        }));
        return Object.freeze({ providerDate, eligibleDates, result });
      };

      const execution = tokenLifecycle
        ? await tokenLifecycle.run({ authority, connection, operation })
        : { value: await operation(connection), connection };
      connection = execution.connection;
      const inventory = execution.value;

      if (!inventory.providerDate) {
        return Object.freeze({
          status: 'PASS_R6_D5_A_KLAVIYO_HISTORICAL_INVENTORY',
          provider_result_status: 'empty',
          selected_account_count: 1,
          closed_sent_date_count: 0,
          checked_date_count: 0,
          row_count: 0,
          empty_provider_result: true,
          account_api_verified: true,
          campaign_inventory_verified: true,
          campaign_reporting_verified: false,
          flow_reporting_verified: false,
          time_fx_verified: false,
          dataset_v2_write: false,
          production_activation: false,
          provider_date: null,
          currency_version: currency.currencyVersion,
        });
      }

      const verified = verifyProviderResult({
        provider: 'klaviyo',
        connection,
        reportingCurrency: currency.reportingCurrency,
        result: inventory.result,
      });
      return Object.freeze({
        status: 'PASS_R6_D5_A_KLAVIYO_HISTORICAL_INVENTORY',
        provider_result_status: verified.providerResultStatus,
        selected_account_count: verified.selectedAccountCount,
        closed_sent_date_count: inventory.eligibleDates.length,
        checked_date_count: 1,
        row_count: verified.rows.length,
        empty_provider_result: verified.providerResultStatus === 'empty',
        account_api_verified: true,
        campaign_inventory_verified: true,
        campaign_reporting_verified: true,
        flow_reporting_verified: true,
        time_fx_verified: true,
        dataset_v2_write: false,
        production_activation: false,
        provider_date: inventory.providerDate,
        currency_version: currency.currencyVersion,
      });
    } catch (error) {
      throw safeFailure(error);
    }
  }

  return Object.freeze({ execute });
}

module.exports = Object.freeze({ createKlaviyoHistoricalInventory });
