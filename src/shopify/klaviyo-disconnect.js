"use strict";

const CONFIRMATION = "DISCONNECT_KLAVIYO";

function failure(code, status = 503) {
  return Object.assign(new Error(code), {code, status});
}

function createKlaviyoDisconnect({store, fetchImpl = fetch, clientId, clientSecret} = {}) {
  if (!store || typeof store.readKlaviyo !== "function" || typeof store.disconnectKlaviyo !== "function") {
    throw new TypeError("Klaviyo disconnect store is required");
  }
  if (typeof clientId !== "string" || !clientId || typeof clientSecret !== "string" || !clientSecret) {
    throw new TypeError("Klaviyo OAuth credentials are required");
  }

  return Object.freeze({
    async execute(authority, confirmation) {
      if (confirmation !== CONFIRMATION) throw failure("KLAVIYO_DISCONNECT_CONFIRMATION_REQUIRED", 409);
      const connection = await store.readKlaviyo(authority);
      if (!connection || connection.status === "disconnected" || connection.status === "revoked") {
        return {status: "not_connected", historical_data_preserved: true};
      }
      if (connection.status !== "connected" || typeof connection.refreshToken !== "string" || !connection.refreshToken) {
        throw failure("KLAVIYO_REAUTHORIZE", 409);
      }
      const response = await fetchImpl("https://a.klaviyo.com/oauth/revoke", {
        method: "POST",
        redirect: "error",
        signal: AbortSignal.timeout(15000),
        headers: {
          Authorization: `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString("base64")}`,
          "Content-Type": "application/x-www-form-urlencoded",
        },
        body: new URLSearchParams({token_type_hint: "refresh_token", token: connection.refreshToken}).toString(),
      });
      if (!response.ok) throw failure("KLAVIYO_REVOKE_FAILED");
      await store.disconnectKlaviyo({authority, version: connection.connection_version});
      return {status: "not_connected", historical_data_preserved: true};
    },
  });
}

module.exports = Object.freeze({CONFIRMATION, createKlaviyoDisconnect});
