"use strict";

function failure(code, status = 503) {
  return Object.assign(new Error(code), {code, status});
}

function estimated30DayEmailSpend(value) {
  if (typeof value !== "string" || !/^(0|[1-9]\d{0,7})(\.\d{1,2})?$/.test(value)) {
    throw failure("INVALID_ESTIMATED_30_DAY_EMAIL_SPEND", 400);
  }
  return Number(value).toFixed(2);
}

function businessDate(timeZone, instant = new Date()) {
  try {
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).formatToParts(instant);
    const values = Object.fromEntries(parts.filter(part => part.type !== "literal").map(part => [part.type, part.value]));
    const value = `${values.year}-${values.month}-${values.day}`;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) throw new Error("invalid date");
    return value;
  } catch {
    throw failure("KLAVIYO_UNAVAILABLE");
  }
}

function createKlaviyoAccountSelection({store, fetchImpl = fetch, clientId, clientSecret}) {
  async function accounts(authority, {allowRefresh = true} = {}) {
    let connection = await store.readKlaviyo(authority);
    if (!connection || connection.status === "revoked") return {connection: null, accounts: []};
    const getAccounts = () => fetchImpl("https://a.klaviyo.com/api/accounts/", {
      headers: {Authorization: `Bearer ${connection.accessToken}`, accept: "application/vnd.api+json", revision: "2026-07-15"},
      signal: AbortSignal.timeout(15000),
      redirect: "error",
    });
    let response = await getAccounts();
    if (response.status === 401 && !allowRefresh) throw failure("KLAVIYO_READ_ONLY_VERIFICATION_EXPIRED", 409);
    if (response.status === 401 && connection.refreshToken && clientId && clientSecret) {
      const refreshed = await fetchImpl("https://a.klaviyo.com/oauth/token", {
        method: "POST", redirect: "error", signal: AbortSignal.timeout(15000),
        headers: {Authorization: `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString("base64")}`, "Content-Type": "application/x-www-form-urlencoded"},
        body: new URLSearchParams({grant_type: "refresh_token", refresh_token: connection.refreshToken}).toString(),
      });
      if (refreshed.status === 400 || refreshed.status === 401) throw failure("KLAVIYO_REAUTHORIZE", 409);
      if (!refreshed.ok) throw failure("KLAVIYO_UNAVAILABLE");
      const tokens = await refreshed.json();
      if (typeof tokens.access_token !== "string" || !tokens.access_token) throw failure("KLAVIYO_UNAVAILABLE");
      await store.refreshKlaviyo({authority, version: connection.updated_at, accessToken: tokens.access_token,
        refreshToken: typeof tokens.refresh_token === "string" && tokens.refresh_token ? tokens.refresh_token : connection.refreshToken});
      connection = await store.readKlaviyo(authority);
      if (!connection || connection.status === "revoked") throw failure("KLAVIYO_REAUTHORIZE", 409);
      response = await getAccounts();
    }
    if (response.status === 401 || response.status === 403) throw failure("KLAVIYO_REAUTHORIZE", 409);
    if (!response.ok) throw failure("KLAVIYO_UNAVAILABLE");
    const payload = await response.json();
    if (!Array.isArray(payload?.data)) throw failure("KLAVIYO_UNAVAILABLE");
    const verified = payload.data.map(item => {
      if (typeof item?.id !== "string" || !item.id || item.id.length > 256) throw failure("KLAVIYO_UNAVAILABLE");
      const attrs = item.attributes || {};
      const name = attrs.contact_information?.organization_name || attrs.name || item.id;
      return {
        id: item.id,
        name: String(name).slice(0, 256),
        currency: /^[A-Z]{3}$/.test(attrs.preferred_currency) ? attrs.preferred_currency : null,
        timezone: typeof attrs.timezone === "string" && attrs.timezone ? attrs.timezone : null,
      };
    });
    return {connection, accounts: verified};
  }

  return Object.freeze({
    async status(authority) {
      const connection = await store.readKlaviyoStatus(authority);
      if (!connection) return {status: "not_connected"};
      if (connection.status === "revoked") return {status: "reset_complete"};
      if (connection.status === "disconnected") return {status: "not_connected"};
      if (connection.status === "connected") {
        const cost = typeof connection.email_monthly_plan_cost === "number"
          ? connection.email_monthly_plan_cost.toFixed(2)
          : connection.email_monthly_plan_cost;
        if (typeof cost !== "string" || !/^(0|[1-9]\d{0,7})\.\d{2}$/.test(cost) || !/^[A-Z]{3}$/.test(connection.account_currency || "")) {
          return {status: "temporarily_unavailable"};
        }
        return {
          status: "connected",
          estimated_30_day_email_spend: cost,
          currency: connection.account_currency,
        };
      }
      return {status: connection.status === "pending_account_selection" ? "account_selection_required" : "temporarily_unavailable"};
    },
    async list(authority) {
      const result = await accounts(authority);
      return {
        status: result.connection?.status || "not_connected",
        accounts: result.accounts,
        active_account_id: result.connection?.active_account_id || null,
        estimated_30_day_email_spend: result.connection?.email_monthly_plan_cost ?? null,
      };
    },
    async verifyReadOnly(authority) {
      const {connection, accounts: verified} = await accounts(authority, {allowRefresh: false});
      if (!connection || connection.status !== "connected" || typeof connection.active_account_id !== "string") {
        throw failure("INVALID_ACCOUNT", 409);
      }
      const account = verified.find(item => item.id === connection.active_account_id);
      if (!account || !account.currency || account.currency !== connection.account_currency) {
        throw failure("INVALID_ACCOUNT", 409);
      }
      return {status: "verified", active_account_verified: true, currency: account.currency};
    },
    async complete(authority, input) {
      const cost = estimated30DayEmailSpend(input?.estimated_30_day_email_spend);
      if (typeof input?.account_id !== "string") throw failure("INVALID_ACCOUNT", 400);
      const {connection, accounts: verified} = await accounts(authority);
      const account = verified.find(item => item.id === input.account_id);
      if (!connection || !account || !account.currency || !account.timezone) throw failure("INVALID_ACCOUNT", 400);
      const effectiveFrom = businessDate(account.timezone);
      await store.completeKlaviyoWithSpendHistory({
        authority,
        version: connection.updated_at,
        account,
        estimated30DayEmailSpend: cost,
        effectiveFrom,
      });
      return {
        status: "connected",
        active_account_id: account.id,
        account_name: account.name,
        currency: account.currency,
        estimated_30_day_email_spend: cost,
        effective_from: effectiveFrom,
      };
    },
  });
}

module.exports = {
  createKlaviyoAccountSelection,
  estimated30DayEmailSpend,
  planCost: estimated30DayEmailSpend,
  businessDate,
};
