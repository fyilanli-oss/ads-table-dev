'use strict';

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
  if (reason === 'CANONICAL_PROVIDER_CONNECTION_REQUIRED') {
    return codedError('KLAVIYO_FLOW_EVENT_INVENTORY_CONNECTION_REQUIRED', 409);
  }
  return codedError('KLAVIYO_FLOW_EVENT_INVENTORY_FAILED', 503);
}

function createKlaviyoFlowEventInventory({
  connectionStore,
  providerClient,
  tokenLifecycle = null,
} = {}) {
  if (!connectionStore || typeof connectionStore.resolveConnected !== 'function') {
    throw new TypeError('canonical connection store is required');
  }
  if (!providerClient || typeof providerClient.fetchAccount !== 'function' ||
    typeof providerClient.fetchFlowEventInventory !== 'function') {
    throw new TypeError('Klaviyo Flow/Event inventory provider client is required');
  }

  async function execute(authority) {
    try {
      let connection = await connectionStore.resolveConnected({ authority, provider: 'klaviyo' });
      if (!connection) throw new Error('CANONICAL_PROVIDER_CONNECTION_REQUIRED');
      const operation = async activeConnection => {
        const accountId = onlySelectedAccount(activeConnection);
        const account = await providerClient.fetchAccount({
          accessToken: activeConnection.accessToken,
          accountId,
        });
        return providerClient.fetchFlowEventInventory({
          accessToken: activeConnection.accessToken,
          timeZone: account.timezone,
        });
      };
      const execution = tokenLifecycle
        ? await tokenLifecycle.run({ authority, connection, operation })
        : { value: await operation(connection) };
      const inventory = execution.value;
      return Object.freeze({
        status: 'PASS_R6_D5_A2_KLAVIYO_FLOW_EVENT_INVENTORY',
        flow_count: inventory.flow_count,
        flow_status_counts: inventory.flow_status_counts,
        scanned_event_count: inventory.scanned_event_count,
        event_counts: inventory.event_counts,
        attributed_event_count: inventory.attributed_event_count,
        earliest_event_date: inventory.earliest_event_date,
        latest_event_date: inventory.latest_event_date,
        event_scan_truncated: inventory.event_scan_truncated,
        account_api_verified: true,
        flow_inventory_verified: true,
        event_inventory_verified: true,
        dataset_v2_write: false,
        production_activation: false,
      });
    } catch (error) {
      throw safeFailure(error);
    }
  }

  return Object.freeze({ execute });
}

module.exports = Object.freeze({ createKlaviyoFlowEventInventory });
