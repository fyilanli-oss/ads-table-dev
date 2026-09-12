"use strict";

const EMBEDDED_HOME_RELEASE = "e10-t6c2k";

function escapeAttribute(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll('"', "&quot;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function documentHead({clientId, title}) {
  const apiKey = escapeAttribute(clientId.trim());
  return `<meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta name="shopify-api-key" content="${apiKey}">
  <meta http-equiv="Cache-Control" content="no-store, no-cache, must-revalidate">
  <script src="https://cdn.shopify.com/shopifycloud/app-bridge.js"></script>
  <script src="https://cdn.shopify.com/shopifycloud/polaris-1.js"></script>
  <title>${title}</title>`;
}

function appNavigation() {
  return `<s-app-nav>
    <s-link href="/shopify/app" rel="home">Home</s-link>
    <s-link href="/shopify/app/platforms">Data sources</s-link>
  </s-app-nav>`;
}

function renderEmbeddedAppHome({clientId}) {
  if (typeof clientId !== "string" || !clientId.trim()) throw new TypeError("clientId is required");
  return `<!doctype html>
<html lang="en">
<head>
  ${documentHead({clientId, title: "AdsTable"})}
</head>
<body>
  ${appNavigation()}
  <s-page heading="AdsTable">
    <s-section heading="Store connection">
      <s-banner id="status" heading="Connecting to your store" tone="info">
        AdsTable is verifying the secure Shopify session.
      </s-banner>
    </s-section>
    <s-section heading="Data sources">
      <s-stack gap="base">
        <s-paragraph>Connect and manage Meta, Google Ads, TikTok, Klaviyo, and Pinterest for this Shopify workspace.</s-paragraph>
        <s-button id="platforms" variant="primary" href="/shopify/app/platforms">Manage data sources</s-button>
      </s-stack>
    </s-section>
  </s-page>
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
          const error = new Error(typeof body.code === "string" ? body.code : "EMBEDDED_BOOTSTRAP_FAILED");
          error.requestId = typeof body.requestId === "string" ? body.requestId : null;
          throw error;
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
        status.setAttribute("heading", "Development store connected securely");
        status.setAttribute("tone", "success");
        status.textContent = "Your verified Shopify workspace is ready.";
        document.documentElement.dataset.smoke = "pass";
      };
      run().catch((error) => {
        const code = /^[A-Z0-9_]{1,64}$/.test(error.message || "") ? error.message : "EMBEDDED_BOOTSTRAP_FAILED";
        const reference = /^[A-Za-z0-9._:-]{1,128}$/.test(error.requestId || "") ? " Reference: " + error.requestId : "";
        status.setAttribute("heading", "Connection could not be verified");
        status.setAttribute("tone", "critical");
        status.textContent = code + "." + reference;
        document.documentElement.dataset.smoke = "fail";
      });
    })();
  </script>
</body>
</html>`;
}

function renderProviderSection([id, label], providerOAuthEnabled) {
  const disabled = providerOAuthEnabled ? "" : " disabled";
  return `<s-section heading="${label}">
      <s-stack direction="inline" gap="base" justify-content="space-between" align-items="center">
        <s-paragraph>Connect ${label} to this Shopify workspace.</s-paragraph>
        <s-button variant="primary" data-provider="${id}"${disabled}>Connect</s-button>
      </s-stack>
    </s-section>`;
}

function renderEmbeddedPlatforms({clientId, providerOAuthEnabled}) {
  if (typeof clientId !== "string" || !clientId.trim()) throw new TypeError("clientId is required");
  const providers = [
    ["meta", "Meta"], ["google_ads", "Google Ads"], ["klaviyo", "Klaviyo"],
    ["tiktok", "TikTok"], ["pinterest", "Pinterest"],
  ];
  const sections = providers.map((provider) => renderProviderSection(provider, providerOAuthEnabled)).join("\n    ");
  return `<!doctype html>
<html lang="en">
<head>
  ${documentHead({clientId, title: "Data sources — AdsTable"})}
</head>
<body>
  ${appNavigation()}
  <s-page heading="Data sources">
    <s-link slot="breadcrumb-actions" href="/shopify/app">Home</s-link>
    <s-banner id="status" heading="Provider connections" tone="info">
      Choose a provider to begin a secure connection.
    </s-banner>
    ${sections}
  </s-page>
  <script>
    (() => {
      "use strict";
      const status = document.getElementById("status");
      document.querySelectorAll("s-button[data-provider]").forEach((button) => button.addEventListener("click", async () => {
        button.disabled = true;
        button.loading = true;
        status.setAttribute("heading", "Opening secure connection");
        status.setAttribute("tone", "info");
        status.textContent = "You will continue on the provider's authorization page.";
        try {
          if (!window.shopify || typeof window.shopify.idToken !== "function") throw new Error();
          const token = await window.shopify.idToken();
          const response = await fetch("/api/shopify/providers/" + encodeURIComponent(button.dataset.provider) + "/oauth/start", {
            method: "POST",
            headers: {Authorization: "Bearer " + token},
          });
          const body = await response.json().catch(() => ({}));
          if (!response.ok || body.navigation !== "top_level" || typeof body.authorization_url !== "string") throw new Error();
          open(body.authorization_url, "_top");
        } catch {
          status.setAttribute("heading", "Connection could not be started");
          status.setAttribute("tone", "critical");
          status.textContent = "Please try again.";
          button.disabled = false;
          button.loading = false;
        }
      }));
      const params = new URLSearchParams(location.search);
      if (params.has("oauth_connected")) {
        status.setAttribute("heading", "Provider authorized");
        status.setAttribute("tone", "success");
        status.textContent = "Account selection is next.";
      } else if (params.has("oauth_error")) {
        status.setAttribute("heading", "Connection was not completed");
        status.setAttribute("tone", "critical");
        status.textContent = "Please try again.";
      }
    })();
  </script>
</body>
</html>`;
}

function setEmbeddedHeaders(res) {
  res.set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0");
  res.set("CDN-Cache-Control", "no-store");
  res.set("Vercel-CDN-Cache-Control", "no-store");
  res.set("Surrogate-Control", "no-store");
  res.set("X-AdsTable-Release", EMBEDDED_HOME_RELEASE);
  res.set("Content-Security-Policy", "frame-ancestors https://admin.shopify.com https://*.myshopify.com");
}

function registerEmbeddedAppHome(app, {clientId}) {
  if (!app || typeof app.get !== "function") throw new TypeError("app.get is required");
  const html = renderEmbeddedAppHome({clientId});
  const handler = (_req, res) => {
    setEmbeddedHeaders(res);
    return res.type("html").send(html);
  };
  app.get("/", handler);
  app.get("/shopify/app", handler);
}

function registerEmbeddedPlatforms(app, {clientId, providerOAuthEnabled = false}) {
  if (!app || typeof app.get !== "function") throw new TypeError("app.get is required");
  const html = renderEmbeddedPlatforms({clientId, providerOAuthEnabled});
  app.get("/shopify/app/platforms", (_req, res) => {
    setEmbeddedHeaders(res);
    return res.type("html").send(html);
  });
}

module.exports = Object.freeze({EMBEDDED_HOME_RELEASE, registerEmbeddedAppHome, renderEmbeddedAppHome, registerEmbeddedPlatforms, renderEmbeddedPlatforms});
