'use strict';

function initializeAdAccounts() {
  for (const provider of ['meta', 'google_ads']) {
    const message = document.getElementById(provider + '-message');
    const choices = document.getElementById(provider + '-choice');
    const save = document.getElementById(provider + '-save');
    const modal = document.getElementById(provider + '-account-modal');
    const connect = document.getElementById(provider + '-connect');
    const connectAction = document.getElementById(provider + '-connect-action');
    const connected = document.getElementById(provider + '-connected');
    const disconnectModal = document.getElementById(provider + '-disconnect-modal');
    const disconnectConfirm = document.getElementById(provider + '-disconnect-confirm');
    const disconnectMessage = document.getElementById(provider + '-disconnect-message');
    const query = new URLSearchParams(location.search);
    const reauthorizationRequired = provider === 'meta' && query.get('oauth_error') === 'reauthorization_required' && query.get('provider') === 'meta';
    if (!message || !choices || !save || !modal) continue;
    let accounts = [];
    let busy = false;
    const request = async (path, body) => {
      const token = await window.shopify.idToken();
      const response = await fetch('/api/shopify/providers/' + provider + '/accounts' + path, {
        method: body ? 'POST' : 'GET',
        headers: {Authorization: 'Bearer ' + token, ...(body ? {'Content-Type': 'application/json'} : {})},
        ...(body ? {body: JSON.stringify(body)} : {}),
      });
      const result = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(result.code || 'PROVIDER_ACCOUNTS_UNAVAILABLE');
      return result;
    };
    const showError = error => {
      message.textContent = error.message === 'PROVIDER_REAUTHORIZE'
        ? 'Authorization expired. Reconnect will be available after the current connection is resolved.'
        : 'Accounts could not be loaded. Please try again.';
    };
    const loadAccounts = async () => {
      try {
        const result = await request('');
        accounts = result.accounts || [];
        choices.replaceChildren();
        for (const account of accounts) {
          const option = document.createElement('s-choice');
          option.value = account.id;
          option.textContent = account.name + ' (' + account.id + ') · ' + account.currency;
          choices.append(option);
        }
        if (!accounts.length) throw new Error('PROVIDER_ACCOUNTS_UNAVAILABLE');
        connect.hidden = true;
        if (connected) connected.hidden = true;
        message.textContent = 'Authorization complete. Select the account to connect.';
        if (typeof modal.showOverlay === 'function') modal.showOverlay();
      } catch (error) { showError(error); }
    };
    save.addEventListener('click', async () => {
      const selectedIds = Array.isArray(choices.values) ? choices.values : [];
      if (busy || selectedIds.length < 1 || selectedIds.length > 3 || selectedIds.some(id => !accounts.some(account => account.id === id))) {
        choices.error = 'Select between 1 and 3 accounts.';
        return;
      }
      busy = true; save.disabled = true; save.loading = true;
      try {
        const result = await request('/select', {account_ids: selectedIds.map(String)});
        const selected = result.accounts || [];
        message.textContent = 'Connected · ' + selected.length + ' account' + (selected.length === 1 ? '' : 's');
        connect.hidden = true;
        if (connected) connected.hidden = false;
        if (typeof modal.hideOverlay === 'function') modal.hideOverlay();
      } catch (error) { showError(error); }
      finally { busy = false; save.disabled = false; save.loading = false; }
    });
    if (disconnectConfirm && disconnectModal && disconnectMessage) {
      disconnectConfirm.addEventListener('click', async () => {
        if (busy) return;
        busy = true;
        disconnectConfirm.disabled = true;
        disconnectConfirm.loading = true;
        disconnectMessage.textContent = 'Disconnecting…';
        try {
          await request('/disconnect', {confirmation: provider === 'meta' ? 'DISCONNECT_META' : 'DISCONNECT_GOOGLE_ADS'});
          message.textContent = 'Not connected';
          connect.hidden = false;
          connected.hidden = true;
          disconnectMessage.textContent = '';
          if (typeof disconnectModal.hideOverlay === 'function') disconnectModal.hideOverlay();
        } catch (error) {
          disconnectMessage.textContent = provider === 'meta' && error.message === 'META_REVOKE_FAILED'
            ? 'Meta access could not be revoked. The connection remains active.'
            : provider === 'meta' && error.message === 'META_REAUTHORIZE'
              ? 'Meta authorization must be renewed before this connection can be revoked.'
              : (provider === 'meta' ? 'Meta' : 'Google Ads') + ' could not be disconnected. The connection remains active.';
        } finally {
          busy = false;
          disconnectConfirm.disabled = false;
          disconnectConfirm.loading = false;
        }
      });
    }
    request('/status').then(result => {
      if (result.status === 'pending_account_selection') return loadAccounts();
      if (result.status === 'connected') {
        connect.hidden = true;
        if (connected) connected.hidden = false;
        const count = Array.isArray(result.accounts) ? result.accounts.length : 0;
        message.textContent = 'Connected' + (count ? ' · ' + count + ' account' + (count === 1 ? '' : 's') : '');
      } else {
        connect.hidden = false;
        if (connected) connected.hidden = true;
        if (reauthorizationRequired) {
          if (connectAction) connectAction.textContent = 'Reconnect Meta';
          message.textContent = 'Meta authorization must be renewed before account selection.';
        } else message.textContent = 'Not connected';
      }
    }).catch(showError);
  }
}

module.exports = Object.freeze({initializeAdAccounts});
