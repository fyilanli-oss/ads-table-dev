"use strict";

// Embedded verbatim in the isolated Shopify presentation endpoint; contains no server credentials.
function initializeKlaviyoAccounts() {
  const root = document.getElementById("klaviyo-accounts");
  if (!root) return;
  const message = document.getElementById("klaviyo-message");
  const choices = document.getElementById("klaviyo-choice");
  const choose = document.getElementById("klaviyo-choose");
  const choiceStep = document.getElementById("klaviyo-choice-step");
  const costStep = document.getElementById("klaviyo-cost-step");
  const cost = document.getElementById("klaviyo-cost");
  const save = document.getElementById("klaviyo-save");
  const retry = document.getElementById("klaviyo-retry");
  const retryStep = document.getElementById("klaviyo-retry-step");
  const connect = document.getElementById("klaviyo-connect");
  const connected = document.getElementById("klaviyo-connected");
  const accountModal = document.getElementById("klaviyo-account-modal");
  const disconnectModal = document.getElementById("klaviyo-disconnect-modal");
  const disconnectConfirm = document.getElementById("klaviyo-disconnect-confirm");
  const disconnectMessage = document.getElementById("klaviyo-disconnect-message");
  const resetStep = document.getElementById("klaviyo-reset-step");
  const resetConfirm = document.getElementById("klaviyo-reset-confirm");
  let accounts = [];
  let selected = null;
  let busy = false;
  let retryAction = null;
  const setVisible = (element, visible) => {
    if (element) element.display = visible ? "auto" : "none";
  };

  async function request(path, body) {
    if (!window.shopify || typeof window.shopify.idToken !== "function") throw new Error("SHOPIFY_SESSION_REQUIRED");
    const token = await window.shopify.idToken();
    const response = await fetch("/api/shopify/providers/klaviyo/accounts" + path, {
      method: body ? "POST" : "GET",
      headers: {Authorization: "Bearer " + token, ...(body ? {"Content-Type": "application/json"} : {})},
      ...(body ? {body: JSON.stringify(body)} : {}),
    });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(result.code || "KLAVIYO_UNAVAILABLE");
    return result;
  }

  function showError(error) {
    const messages = {
      SHOPIFY_SESSION_REQUIRED: "Open AdsTable from Shopify Admin to continue.",
      KLAVIYO_REAUTHORIZE: "Your Klaviyo authorization needs to be renewed. Select Connect to authorize again.",
      KLAVIYO_READ_ONLY_VERIFICATION_EXPIRED: "The existing Klaviyo authorization has expired. A clean connection must be prepared before reconnecting.",
      KLAVIYO_REVOKE_FAILED: "The old Klaviyo authorization could not be removed. No local connection data was changed.",
      INVALID_PLAN_COST: "Enter a monthly cost of 0 or more, with up to two decimal places.",
      INVALID_ACCOUNT: "This account could not be verified. Reload the accounts and select again.",
      CONNECTION_CHANGED: "The connection changed. Reload the accounts before saving again.",
    };
    message.textContent = messages[error.message] || "Klaviyo could not be reached. Please try again shortly.";
    setVisible(retryStep, true);
    if (error.message === "KLAVIYO_REAUTHORIZE") setVisible(connect, true);
    if (error.message === "KLAVIYO_READ_ONLY_VERIFICATION_EXPIRED") setVisible(resetStep, true);
  }

  async function loadStatus() {
    if (busy) return;
    let loadPendingAccounts = false;
    busy = true;
    retryAction = loadStatus;
    setVisible(retryStep, false);
    try {
      const result = await request("/status");
      if (result.status === "connected") {
        setVisible(connect, false);
        setVisible(connected, true);
        setVisible(resetStep, true);
        const amount = result.email_monthly_plan_cost == null ? "" : " · " + result.email_monthly_plan_cost + " " + result.currency + "/month";
        message.textContent = "Connected" + amount;
      } else if (result.status === "account_selection_required") {
        setVisible(connect, false);
        setVisible(connected, false);
        message.textContent = "Account selection required";
        loadPendingAccounts = true;
      } else if (result.status === "not_connected") {
        setVisible(connect, true);
        setVisible(connected, false);
        message.textContent = "Not connected";
      } else if (result.status === "reset_complete") {
        setVisible(connect, true);
        setVisible(connected, false);
        setVisible(resetStep, false);
        message.textContent = "Not connected";
      } else {
        setVisible(connect, false);
        setVisible(connected, false);
        message.textContent = "Temporarily unavailable";
      }
    } catch (error) {
      showError(error);
    } finally { busy = false; }
    if (loadPendingAccounts) await loadAccounts();
  }

  async function loadAccounts() {
    if (busy) return;
    busy = true;
    retryAction = loadAccounts;
    setVisible(retryStep, false);
    setVisible(choiceStep, false);
    setVisible(costStep, false);
    selected = null;
    message.textContent = "Checking Klaviyo connection…";
    try {
      const result = await request("");
      accounts = result.accounts;
      if (result.status === "not_connected") {
        message.textContent = "Select Connect to authorize Klaviyo.";
        setVisible(connect, true);
        return;
      }
      setVisible(connect, false);
      setVisible(connected, false);
      if (result.status === "connected" && accounts.some(account => account.id === result.active_account_id)) {
        const account = accounts.find(account => account.id === result.active_account_id);
        message.textContent = "Connected: " + account.name + ". Email Monthly Plan Cost: " + result.email_monthly_plan_cost + " " + account.currency + ".";
        return;
      }
      choices.replaceChildren();
      for (const account of accounts) {
        const option = document.createElement("s-option");
        option.value = account.id;
        option.textContent = account.name + " (" + account.id + ")";
        choices.append(option);
      }
      if (!accounts.length) throw new Error("KLAVIYO_UNAVAILABLE");
      choices.value = accounts[0].id;
      setVisible(choiceStep, true);
      if (accountModal && typeof accountModal.showOverlay === "function") accountModal.showOverlay();
      message.textContent = "Klaviyo is authorized. Select the account to connect.";
    } catch (error) { showError(error); }
    finally { busy = false; }
  }

  async function verifyR5ReadOnly() {
    if (busy) return;
    busy = true;
    retryAction = verifyR5ReadOnly;
    setVisible(retryStep, false);
    setVisible(connect, false);
    message.textContent = "Verifying the existing Klaviyo account…";
    try {
      const result = await request("/verify");
      if (result.status !== "verified" || result.active_account_verified !== true) throw new Error("KLAVIYO_UNAVAILABLE");
      setVisible(connect, false);
      message.textContent = "Connected · Klaviyo account verified · " + result.currency;
    } catch (error) { showError(error); }
    finally { busy = false; }
  }

  choose.addEventListener("click", () => {
    selected = accounts.find(account => account.id === choices.value);
    if (!selected?.currency) return showError(new Error("INVALID_ACCOUNT"));
    setVisible(choiceStep, false);
    setVisible(costStep, true);
    cost.value = "";
    cost.setAttribute("label", "Email Monthly Plan Cost (" + selected.currency + ")");
    message.textContent = "Selected: " + selected.name + ". Save your monthly plan cost to complete the connection. Enter 0 for a free plan.";
  });
  save.addEventListener("click", async () => {
    if (busy || !selected) return;
    busy = true;
    save.disabled = true;
    save.loading = true;
    try {
      const result = await request("/select", {account_id: selected.id, email_monthly_plan_cost: String(cost.value)});
      setVisible(costStep, false);
      setVisible(retryStep, false);
      message.textContent = "Connected: " + result.account_name + ". Email Monthly Plan Cost: " + result.email_monthly_plan_cost + " " + result.currency + ".";
      setVisible(connected, true);
      if (accountModal && typeof accountModal.hideOverlay === "function") accountModal.hideOverlay();
    } catch (error) { showError(error); }
    finally { busy = false; save.disabled = false; save.loading = false; }
  });
  retry.addEventListener("click", () => retryAction && retryAction());
  if (disconnectConfirm && disconnectModal) disconnectConfirm.addEventListener("click", async () => {
    if (busy) return;
    busy = true;
    disconnectConfirm.disabled = true;
    disconnectConfirm.loading = true;
    disconnectMessage.textContent = "Disconnecting…";
    try {
      const result = await request("/disconnect", {confirmation: "DISCONNECT_KLAVIYO"});
      if (result.status !== "not_connected") throw new Error("KLAVIYO_UNAVAILABLE");
      setVisible(connect, true);
      setVisible(connected, false);
      message.textContent = "Not connected";
      disconnectMessage.textContent = "";
      if (typeof disconnectModal.hideOverlay === "function") disconnectModal.hideOverlay();
    } catch (error) {
      disconnectMessage.textContent = error.message === "KLAVIYO_REVOKE_FAILED"
        ? "Klaviyo access could not be revoked. The connection remains active."
        : "The connection could not be disconnected. Please try again.";
    } finally {
      busy = false;
      disconnectConfirm.disabled = false;
      disconnectConfirm.loading = false;
    }
  });
  if (resetConfirm) resetConfirm.addEventListener("click", async () => {
    if (busy) return;
    busy = true;
    resetConfirm.disabled = true;
    resetConfirm.loading = true;
    setVisible(retryStep, false);
    try {
      const result = await request("/reset", {confirmation: "REVOKE_KLAVIYO_AND_START_FRESH"});
      if (result.status !== "revoked" && result.status !== "already_revoked") throw new Error("KLAVIYO_REVOKE_FAILED");
      setVisible(connect, false);
      setVisible(resetStep, false);
      message.textContent = "Old Klaviyo connection removed. Clean connection setup is not open yet.";
    } catch (error) { showError(error); }
    finally { busy = false; resetConfirm.disabled = false; resetConfirm.loading = false; }
  });
  const params = new URLSearchParams(location.search);
  if (params.get("r5_read_only_verify") === "1") verifyR5ReadOnly();
  else if (params.get("oauth_connected") === "klaviyo" && params.get("account_selection_required") === "1") loadAccounts();
  else loadStatus();
}

module.exports = {initializeKlaviyoAccounts};
