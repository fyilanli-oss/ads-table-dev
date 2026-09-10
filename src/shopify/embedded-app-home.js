"use strict";

function escapeAttribute(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll('"', "&quot;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function renderEmbeddedAppHome({clientId}) {
  if (typeof clientId !== "string" || !clientId.trim()) throw new TypeError("clientId is required");
  const apiKey = escapeAttribute(clientId.trim());
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta name="shopify-api-key" content="${apiKey}">
  <script src="https://cdn.shopify.com/shopifycloud/app-bridge.js"></script>
  <title>AdsTable</title>
</head>
<body>
  <main>
    <h1>AdsTable</h1>
    <p id="status" role="status" aria-live="polite">Securing your development-store connection…</p>
  </main>
  <script>
    (() => {
      "use strict";
      const status = document.getElementById("status");
      const request = async (path, method, token) => {
        const response = await fetch(path, {
          method,
          credentials: "same-origin",
          headers: {Authorization: "Bearer " + token},
        });
        const body = await response.json().catch(() => ({}));
        if (!response.ok || body.status !== "active" || body.workspace_ready !== true) {
          throw new Error("EMBEDDED_BOOTSTRAP_FAILED");
        }
        return body;
      };
      const run = async () => {
        if (!window.shopify || typeof window.shopify.idToken !== "function") {
          throw new Error("SHOPIFY_APP_BRIDGE_REQUIRED");
        }
        const firstToken = await window.shopify.idToken();
        await request("/api/shopify/bootstrap", "POST", firstToken);
        const secondToken = await window.shopify.idToken();
        await request("/api/shopify/bootstrap", "POST", secondToken);
        const sessionToken = await window.shopify.idToken();
        await request("/api/shopify/session", "GET", sessionToken);
        status.textContent = "Development store connected securely.";
        document.documentElement.dataset.smoke = "pass";
      };
      run().catch(() => {
        status.textContent = "Connection could not be verified. Reopen the app and try again.";
        document.documentElement.dataset.smoke = "fail";
      });
    })();
  </script>
</body>
</html>`;
}

function registerEmbeddedAppHome(app, {clientId}) {
  if (!app || typeof app.get !== "function") throw new TypeError("app.get is required");
  const html = renderEmbeddedAppHome({clientId});
  const handler = (_req, res) => {
    res.set("Cache-Control", "no-store");
    res.set("Content-Security-Policy", "frame-ancestors https://admin.shopify.com https://*.myshopify.com");
    return res.type("html").send(html);
  };
  app.get("/", handler);
  app.get("/shopify/app", handler);
}

module.exports = Object.freeze({registerEmbeddedAppHome, renderEmbeddedAppHome});
