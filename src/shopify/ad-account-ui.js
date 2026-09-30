'use strict';

function initializeAdAccounts() {
  for (const provider of ['meta', 'google_ads']) {
    const message = document.getElementById(provider + '-message');
    const choices = document.getElementById(provider + '-choice');
    const choiceError = document.getElementById(provider + '-choice-error');
    const save = document.getElementById(provider + '-save');
    const modal = document.getElementById(provider + '-account-modal');
    const connect = document.getElementById(provider + '-connect');
    const connectAction = document.getElementById(provider + '-connect-action');
    const resume = document.getElementById(provider + '-resume');
    const resumeAction = document.getElementById(provider + '-resume-action');
    const connected = document.getElementById(provider + '-connected');
    const disconnectModal = document.getElementById(provider + '-disconnect-modal');
    const disconnectConfirm = document.getElementById(provider + '-disconnect-confirm');
    const disconnectMessage = document.getElementById(provider + '-disconnect-message');
    const reporting = document.getElementById(provider + '-reporting');
    const reportingSummary = document.getElementById(provider + '-reporting-summary');
    const reportingModal = document.getElementById(provider + '-reporting-modal');
    const reportingChoice = document.getElementById(provider + '-reporting-choice');
    const reportingSave = document.getElementById(provider + '-reporting-save');
    const reportingMessage = document.getElementById(provider + '-reporting-message');
    const query = new URLSearchParams(location.search);
    const reauthorizationRequired = provider === 'meta' && query.get('oauth_error') === 'reauthorization_required' && query.get('provider') === 'meta';
    if (!message || !choices || !choiceError || !save || !modal) continue;
    let accounts = [];
    let connectedAccounts = [];
    let reportingAccount = null;
    let selectedIds = [];
    let selectionError = '';
    let busy = false;
    const setVisible = (element, visible) => {
      if (element) element.display = visible ? 'auto' : 'none';
    };
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
    const renderReporting = result => {
      connectedAccounts = Array.isArray(result?.accounts) ? result.accounts : connectedAccounts;
      reportingAccount = result?.reporting_account || reportingAccount;
      const available = result?.status === 'connected' && connectedAccounts.length > 0 && reportingAccount?.id;
      if (!reporting || !reportingSummary || !reportingChoice) return;
      setVisible(reporting, Boolean(available));
      if (!available) return;
      const current = connectedAccounts.find(account => String(account.id) === String(reportingAccount.id));
      reportingSummary.textContent = 'Reporting Account: ' + String(current?.name || reportingAccount.name || reportingAccount.id);
      reportingChoice.replaceChildren();
      for (const account of connectedAccounts) {
        const option = document.createElement('s-option');
        option.setAttribute('value', String(account.id));
        option.textContent = String(account.name) + ' (' + String(account.id) + ')';
        reportingChoice.append(option);
      }
      reportingChoice.value = String(reportingAccount.id);
      if (reportingMessage) reportingMessage.textContent = '';
    };
    const syncSelection = () => {
      const selected = new Set(selectedIds);
      choices.querySelectorAll('s-checkbox').forEach(option => {
        option.checked = selected.has(String(option.getAttribute('value') || ''));
      });
      choiceError.textContent = selectionError;
      save.disabled = selectedIds.length < 1 || selectedIds.length > 3;
    };
    const loadAccounts = async () => {
      try {
        const result = await request('');
        accounts = result.accounts || [];
        selectedIds = [];
        selectionError = '';
        choices.replaceChildren();
        for (const [index, account] of accounts.entries()) {
          const id = String(account.id);
          const option = document.createElement('s-checkbox');
          option.setAttribute('value', id);
          option.setAttribute('name', provider + '-account-' + index);
          option.label = account.name + ' (' + account.id + ') · ' + account.currency;
          option.checked = false;
          option.defaultChecked = false;
          option.addEventListener('change', event => {
            const selectedId = String(event.currentTarget.getAttribute('value') || '');
            if (event.currentTarget.checked) {
              if (selectedIds.length >= 3 && !selectedIds.includes(selectedId)) {
                event.currentTarget.checked = false;
                selectionError = 'Select between 1 and 3 accounts.';
              } else {
                selectedIds = [...new Set([...selectedIds, selectedId])];
                selectionError = '';
              }
            } else {
              selectedIds = selectedIds.filter(id => id !== selectedId);
              selectionError = '';
            }
            syncSelection();
          });
          choices.append(option);
        }
        syncSelection();
        if (!accounts.length) throw new Error('PROVIDER_ACCOUNTS_UNAVAILABLE');
        setVisible(connect, false);
        if (resume) setVisible(resume, true);
        if (connected) setVisible(connected, false);
        message.textContent = 'Authorization complete. Select the account to connect.';
        if (typeof modal.showOverlay === 'function') modal.showOverlay();
      } catch (error) { showError(error); }
    };
    save.addEventListener('click', async () => {
      const submittedIds = selectedIds.slice();
      if (busy || submittedIds.length < 1 || submittedIds.length > 3 || submittedIds.some(id => !accounts.some(account => String(account.id) === id))) {
        selectionError = 'Select between 1 and 3 accounts.';
        syncSelection();
        return;
      }
      busy = true; save.disabled = true; save.loading = true;
      try {
        const result = await request('/select', {account_ids: submittedIds});
        const selected = result.accounts || [];
        message.textContent = 'Connected · ' + selected.length + ' account' + (selected.length === 1 ? '' : 's');
        setVisible(connect, false);
        if (resume) setVisible(resume, false);
        if (connected) setVisible(connected, true);
        renderReporting({status: 'connected', accounts: selected, reporting_account: selected[0] ? {id: selected[0].id, name: selected[0].name} : null});
        selectedIds = [];
        selectionError = '';
        syncSelection();
        if (typeof modal.hideOverlay === 'function') modal.hideOverlay();
      } catch (error) { showError(error); }
      finally { busy = false; save.disabled = selectedIds.length < 1 || selectedIds.length > 3; save.loading = false; }
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
          setVisible(connect, true);
          if (resume) setVisible(resume, false);
          setVisible(connected, false);
          if (reporting) setVisible(reporting, false);
          disconnectMessage.textContent = '';
          if (typeof disconnectModal.hideOverlay === 'function') disconnectModal.hideOverlay();
        } catch (error) {
          disconnectMessage.textContent = provider === 'meta' && error.message === 'META_REVOKE_FAILED'
            ? 'Meta access could not be revoked. The connection remains active.'
            : provider === 'meta' && error.message === 'META_REAUTHORIZE'
              ? 'Meta authorization must be renewed before this connection can be revoked.'
              : provider === 'google_ads' && error.message === 'GOOGLE_REVOKE_FAILED'
                ? 'Google access could not be revoked. The connection remains active.'
                : (provider === 'meta' ? 'Meta' : 'Google Ads') + ' could not be disconnected. The connection remains active.';
        } finally {
          busy = false;
          disconnectConfirm.disabled = false;
          disconnectConfirm.loading = false;
        }
      });
    }
    if (reportingSave && reportingChoice && reportingModal && reportingMessage) {
      reportingSave.addEventListener('click', async () => {
        const accountId = String(reportingChoice.value || '').trim();
        if (busy || !connectedAccounts.some(account => String(account.id) === accountId)) {
          reportingMessage.textContent = 'Choose one connected account.';
          return;
        }
        busy = true;
        reportingSave.disabled = true;
        reportingSave.loading = true;
        reportingMessage.textContent = 'Verifying account access…';
        try {
          const result = await request('/reporting', {account_id: accountId});
          reportingAccount = result.reporting_account;
          renderReporting({status: 'connected', accounts: connectedAccounts, reporting_account: reportingAccount});
          reportingMessage.textContent = 'Reporting account saved.';
          if (typeof reportingModal.hideOverlay === 'function') reportingModal.hideOverlay();
        } catch (error) {
          reportingMessage.textContent = error.message === 'REPORTING_ACCOUNT_ACCESS_NOT_VERIFIED'
            ? 'Account access could not be verified. The previous reporting account is unchanged.'
            : error.message === 'PROVIDER_REAUTHORIZE'
              ? 'Authorization expired. Reconnect the provider before changing the reporting account.'
              : 'Reporting account could not be saved. The previous selection is unchanged.';
        } finally {
          busy = false;
          reportingSave.disabled = false;
          reportingSave.loading = false;
        }
      });
    }
    if (resumeAction) resumeAction.addEventListener('click', loadAccounts);
    request('/status').then(result => {
      if (result.status === 'pending_account_selection') {
        setVisible(connect, false);
        if (resume) setVisible(resume, true);
        return loadAccounts();
      }
      if (result.status === 'connected') {
        setVisible(connect, false);
        if (resume) setVisible(resume, false);
        if (connected) setVisible(connected, true);
        const count = Array.isArray(result.accounts) ? result.accounts.length : 0;
        message.textContent = 'Connected' + (count ? ' · ' + count + ' account' + (count === 1 ? '' : 's') : '');
        renderReporting(result);
      } else {
        setVisible(connect, true);
        if (resume) setVisible(resume, false);
        if (connected) setVisible(connected, false);
        if (reporting) setVisible(reporting, false);
        if (reauthorizationRequired) {
          if (connectAction) connectAction.textContent = 'Reconnect Meta';
          message.textContent = 'Meta authorization must be renewed before account selection.';
        } else message.textContent = 'Not connected';
      }
    }).catch(showError);
  }
}

module.exports = Object.freeze({initializeAdAccounts});

