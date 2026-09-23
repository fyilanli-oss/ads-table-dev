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
  const resetStep = document.getElementById("klaviyo-reset-step");
  const resetConfirm = document.getElementById("klaviyo-reset-confirm");
  let accounts = [];
  let selected = null;
  let busy = false;
  let retryAction = null;

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
    retryStep.hidden = false;
    if (error.message === "KLAVIYO_REAUTHORIZE") connect.hidden = false;
    if (error.message === "KLAVIYO_READ_ONLY_VERIFICATION_EXPIRED") resetStep.hidden = false;
  }

  async function loadStatus() {
    if (busy) return;
    busy = true;
    retryAction = loadStatus;
    retryStep.hidden = true;
    try {
      const result = await request("/status");
      if (result.status === "connected") {
        connect.hidden = true;
        resetStep.hidden = false;
        const amount = result.email_monthly_plan_cost == null ? "" : " · " + result.email_monthly_plan_cost + " " + result.currency + "/month";
        message.textContent = "Connected" + amount;
      } else if (result.status === "account_selection_required") {
        connect.hidden = true;
        message.textContent = "Account selection required";
      } else if (result.status === "not_connected") {
        connect.hidden = false;
        message.textContent = "Not connected";
      } else if (result.status === "reset_complete") {
        connect.hidden = true;
        resetStep.hidden = true;
        message.textContent = "Old Klaviyo connection removed. Clean connection setup is not open yet.";
      } else {
        connect.hidden = true;
        message.textContent = "Temporarily unavailable";
      }
    } catch (error) {
      showError(error);
    } finally {
      busy = false;
    }
  }

  async function loadAccounts() {
    if (busy) return;
    busy = true;
    retryAction = loadAccounts;
    retryStep.hidden = true;
    choiceStep.hidden = true;
    costStep.hidden = true;
    selected = null;
    message.textContent = "Checking Klaviyo connection…";
    try {
      const result = await request("");
      accounts = result.accounts;
      if (result.status === "not_connected") {
        message.textContent = "Select Connect to authorize Klaviyo.";
        connect.hidden = false;
        return;
      }
      connect.hidden = true;
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
      choiceStep.hidden = false;
      message.textContent = "Klaviyo is authorized. Select the account to connect.";
    } catch (error) { showError(error); }
    finally { busy = false; }
  }

  async function verifyR5ReadOnly() {
    if (busy) return;
    busy = true;
    retryAction = verifyR5ReadOnly;
    retryStep.hidden = true;
    connect.hidden = true;
    message.textContent = "Verifying the existing Klaviyo account…";
    try {
      const result = await request("/verify");
      if (result.status !== "verified" || result.active_account_verified !== true) throw new Error("KLAVIYO_UNAVAILABLE");
      connect.hidden = true;
      message.textContent = "Connected · Klaviyo account verified · " + result.currency;
    } catch (error) { showError(error); }
    finally { busy = false; }
  }

  choose.addEventListener("click", () => {
    selected = accounts.find(account => account.id === choices.value);
    if (!selected?.currency) return showError(new Error("INVALID_ACCOUNT"));
    choiceStep.hidden = true;
    costStep.hidden = false;
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
      costStep.hidden = true;
      retryStep.hidden = true;
      message.textContent = "Connected: " + result.account_name + ". Email Monthly Plan Cost: " + result.email_monthly_plan_cost + " " + result.currency + ".";
    } catch (error) { showError(error); }
    finally { busy = false; save.disabled = false; save.loading = false; }
  });
  retry.addEventListener("click", () => retryAction && retryAction());
  resetConfirm.addEventListener("click", async () => {
    if (busy) return;
    busy = true;
    resetConfirm.disabled = true;
    resetConfirm.loading = true;
    retryStep.hidden = true;
    try {
      const result = await request("/reset", {confirmation: "REVOKE_KLAVIYO_AND_START_FRESH"});
      if (result.status !== "revoked" && result.status !== "already_revoked") throw new Error("KLAVIYO_REVOKE_FAILED");
      connect.hidden = true;
      resetStep.hidden = true;
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
