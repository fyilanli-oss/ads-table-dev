"use strict";

const CONFIRMATION = "REVOKE_KLAVIYO_AND_START_FRESH";

function failure(code, status = 503) {
  return Object.assign(new Error(code), {code, status});
}

function createKlaviyoControlledReset({store, fetchImpl = fetch, clientId, clientSecret} = {}) {
  if (!store || typeof store.readKlaviyoForReset !== "function" || typeof store.markKlaviyoRevoked !== "function") {
    throw new TypeError("controlled reset store is required");
  }
  if (typeof clientId !== "string" || !clientId || typeof clientSecret !== "string" || !clientSecret) {
    throw new TypeError("Klaviyo OAuth credentials are required");
  }

  return Object.freeze({
    async execute(authority, confirmation) {
      if (confirmation !== CONFIRMATION) throw failure("KLAVIYO_RESET_CONFIRMATION_REQUIRED", 409);
      const connection = await store.readKlaviyoForReset(authority);
      if (!connection || connection.status === "revoked") {
        return {status: "already_revoked", canonical_connection_created: false, historical_data_preserved: true};
      }
      if (typeof connection.refreshToken !== "string" || !connection.refreshToken) {
        throw failure("KLAVIYO_REAUTHORIZATION_REQUIRED", 409);
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

      await store.markKlaviyoRevoked({authority, version: connection.updated_at});
      return {status: "revoked", canonical_connection_created: false, historical_data_preserved: true};
    },
  });
}

module.exports = Object.freeze({CONFIRMATION, createKlaviyoControlledReset});
