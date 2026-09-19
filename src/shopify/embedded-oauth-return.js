"use strict";

const {normalizeShopDomain} = require("./tenant-model");

function createEmbeddedOAuthReturn({client, clientId}) {
  return async transaction => {
    const {data, error} = await client.from("shopify_installations").select("shop_domain")
      .eq("shop_id", transaction.shop_id).eq("workspace_id", transaction.workspace_id)
      .eq("status", "active").maybeSingle();
    if (error || !data) throw new Error("SHOPIFY_RETURN_UNAVAILABLE");
    // The installed shop comes from the consumed OAuth transaction, never callback query parameters.
    return `https://${normalizeShopDomain(data.shop_domain)}/admin/apps/${encodeURIComponent(clientId)}`;
  };
}

module.exports = {createEmbeddedOAuthReturn};
