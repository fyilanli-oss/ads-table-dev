'use strict';

function initializeAdAccounts() {
  for (const provider of ['meta', 'google_ads']) {
    const message = document.getElementById(provider + '-message');
    const select = document.getElementById(provider + '-choice');
    const save = document.getElementById(provider + '-save');
    const openModal = document.getElementById(provider + '-account-open');
    const closeModal = document.getElementById(provider + '-account-close');
    const connect = document.getElementById(provider + '-connect');
    if (!message || !select || !save) continue;
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
        select.replaceChildren();
        for (const account of accounts) {
          const option = document.createElement('s-option');
          option.value = account.id;
          option.textContent = account.name + ' (' + account.id + ') · ' + account.currency;
          select.append(option);
        }
        if (!accounts.length) throw new Error('PROVIDER_ACCOUNTS_UNAVAILABLE');
        select.value = accounts[0].id;
        connect.hidden = true;
        message.textContent = 'Authorization complete. Select the account to connect.';
        openModal.click();
      } catch (error) { showError(error); }
    };
    save.addEventListener('click', async () => {
      if (busy || !accounts.some(account => account.id === select.value)) return;
      busy = true; save.disabled = true; save.loading = true;
      try {
        const result = await request('/select', {account_id: String(select.value)});
        message.textContent = 'Connected: ' + result.account_name + ' · ' + result.currency;
        closeModal.click();
      } catch (error) { showError(error); }
      finally { busy = false; save.disabled = false; save.loading = false; }
    });
    request('/status').then(result => {
      if (result.status === 'pending_account_selection') return loadAccounts();
      if (result.status === 'connected') { connect.hidden = true; message.textContent = 'Connected'; }
      else message.textContent = 'Not connected';
    }).catch(showError);
  }
}

module.exports = Object.freeze({initializeAdAccounts});
