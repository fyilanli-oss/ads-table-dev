"use strict";

const CONFIRMATION = "DISCONNECT_META";

function failure(code, status = 503) {
  return Object.assign(new Error(code), {code, status});
}

function createMetaDisconnect({store, fetchImpl = fetch, graphVersion = "v20.0"} = {}) {
  if (!store || typeof store.resolveConnected !== "function" || typeof store.disconnectMeta !== "function") {
    throw new TypeError("Meta disconnect store is required");
  }
  if (typeof fetchImpl !== "function") throw new TypeError("Meta disconnect fetch implementation is required");
  if (!/^v\d+\.\d+$/.test(String(graphVersion))) throw new TypeError("Meta Graph version is invalid");

  return Object.freeze({
    async execute(authority, confirmation) {
      if (confirmation !== CONFIRMATION) throw failure("META_DISCONNECT_CONFIRMATION_REQUIRED", 409);
      const connection = await store.resolveConnected({authority, provider: "meta"});
      if (!connection) return {status: "not_connected", historical_data_preserved: true};
      if (typeof connection.accessToken !== "string" || !connection.accessToken) {
        throw failure("META_REAUTHORIZE", 409);
      }

      const url = new URL(`https://graph.facebook.com/${graphVersion}/me/permissions`);
      url.searchParams.set("access_token", connection.accessToken);
      const response = await fetchImpl(url, {
        method: "DELETE",
        redirect: "error",
        signal: AbortSignal.timeout(15000),
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok || body?.success === false) throw failure("META_REVOKE_FAILED");

      await store.disconnectMeta({authority, version: connection.version});
      return {status: "not_connected", historical_data_preserved: true};
    },
  });
}

module.exports = Object.freeze({CONFIRMATION, createMetaDisconnect});
