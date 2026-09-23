"use strict";

const {initializeKlaviyoAccounts} = require("./klaviyo-account-ui");

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
        try {
          const sessionToken = await window.shopify.idToken();
          await request("/api/shopify/session", "GET", sessionToken);
        } catch (error) {
          if (error.message !== "SHOP_REAUTHORIZATION_REQUIRED") throw error;
          const bootstrapToken = await window.shopify.idToken();
          await request("/api/shopify/bootstrap", "POST", bootstrapToken);
          const verifiedToken = await window.shopify.idToken();
          await request("/api/shopify/session", "GET", verifiedToken);
        }
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

function renderProviderSection({id, label, description, parked = false}, providerOAuthEnabled) {
  const disabled = providerOAuthEnabled ? "" : " disabled";
  return `<s-section id="${id}" heading="${label}">
      <s-stack direction="inline" gap="base" justify-content="space-between" align-items="center">
        <s-stack gap="tight">
          <s-paragraph>${description}</s-paragraph>
          ${id === "klaviyo" ? `<s-paragraph id="klaviyo-message" aria-live="polite">${providerOAuthEnabled ? "Checking connection status…" : "Connection setup unavailable"}</s-paragraph>` : ""}
          ${parked ? '<s-paragraph>Parked</s-paragraph>' : ""}
        </s-stack>
        ${parked ? '<s-button disabled>Connect</s-button>' : `${id === "klaviyo" ? '<div id="klaviyo-connect">' : ""}<s-button variant="primary" data-provider="${id}"${disabled}>Connect</s-button>${id === "klaviyo" ? "</div>" : ""}`}
      </s-stack>
      ${id === "klaviyo" && providerOAuthEnabled ? `<s-stack id="klaviyo-accounts" gap="base">
        <div id="klaviyo-choice-step" hidden><s-stack gap="base">
          <s-select id="klaviyo-choice" label="Klaviyo account"></s-select>
          <s-button id="klaviyo-choose">Select account</s-button>
        </s-stack></div>
        <div id="klaviyo-cost-step" hidden><s-stack gap="base">
          <s-number-field id="klaviyo-cost" label="Email Monthly Plan Cost" min="0" max="99999999.99" step="0.01"></s-number-field>
          <s-button id="klaviyo-save" variant="primary">Save and connect</s-button>
        </s-stack></div>
        <div id="klaviyo-retry-step" hidden><s-button id="klaviyo-retry">Try again</s-button></div>
        <div id="klaviyo-reset-step" hidden>
          <s-button tone="critical" commandFor="klaviyo-reset-modal" command="--show">Remove old connection</s-button>
          <s-modal id="klaviyo-reset-modal" heading="Remove old Klaviyo connection?">
            <s-stack gap="base">
              <s-paragraph>This removes the expired Klaviyo authorization so AdsTable can establish one clean workspace connection later.</s-paragraph>
              <s-paragraph>Your historical analytics, selected account, currency, and recorded monthly cost will be preserved.</s-paragraph>
            </s-stack>
            <s-button slot="secondary-actions" commandFor="klaviyo-reset-modal" command="--hide">Cancel</s-button>
            <s-button id="klaviyo-reset-confirm" slot="primary-action" variant="primary" tone="critical" commandFor="klaviyo-reset-modal" command="--hide">Remove connection</s-button>
          </s-modal>
        </div>
      </s-stack>` : ""}
    </s-section>`;
}

function renderEmbeddedPlatforms({clientId, providerOAuthEnabled}) {
  if (typeof clientId !== "string" || !clientId.trim()) throw new TypeError("clientId is required");
  const providers = [
    {id: "meta", label: "Meta", description: "Meta advertising performance and spend."},
    {id: "google_ads", label: "Google Ads", description: "Google Ads performance and spend."},
    {id: "klaviyo", label: "Klaviyo", description: "Email performance and monthly plan cost."},
    {id: "tiktok", label: "TikTok", description: "TikTok advertising performance and spend."},
    {id: "pinterest", label: "Pinterest", description: "Pinterest connection is not available in this release.", parked: true},
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
      (${initializeKlaviyoAccounts.toString()})();
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
