"use strict";

const {initializeKlaviyoAccounts} = require("./klaviyo-account-ui");
const {initializeAdAccounts} = require("./ad-account-ui");

const EMBEDDED_HOME_RELEASE = "r7a-v2";

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
        <s-paragraph>Choose your AdsTable reporting currency, then connect Meta, Google Ads, or Klaviyo.</s-paragraph>
        <div id="setup-data-sources-container" hidden><s-button id="setup-data-sources" variant="primary" commandFor="currency-modal" command="--show">Set up data sources</s-button></div>
        <div id="manage-data-sources-container" hidden><s-button id="manage-data-sources" variant="primary" href="/shopify/app/platforms">Manage data sources</s-button></div>
      </s-stack>
    </s-section>
    <s-modal id="currency-modal" heading="Choose reporting currency" size="small-100">
      <s-stack gap="base">
        <s-paragraph>This is the currency AdsTable will use for reporting. It is independent from Shopify and provider account currencies.</s-paragraph>
        <s-select id="reporting-currency" label="Reporting currency">
          ${["TRY","USD","EUR","GBP","JPY","CNY","AUD","CAD","CHF","SEK","NOK","DKK","PLN"].map(currency => `<s-option value="${currency}">${currency}</s-option>`).join("")}
        </s-select>
        <s-paragraph id="currency-message" aria-live="polite"></s-paragraph>
      </s-stack>
      <s-button slot="secondary-actions" commandFor="currency-modal" command="--hide">Cancel</s-button>
      <s-button id="save-reporting-currency" slot="primary-action" variant="primary">Save and continue</s-button>
    </s-modal>
  </s-page>
  <script>
    (() => {
      "use strict";
      const status = document.getElementById("status");
      const setupDataSources = document.getElementById("setup-data-sources");
      const manageDataSources = document.getElementById("manage-data-sources");
      const setupDataSourcesContainer = document.getElementById("setup-data-sources-container");
      const manageDataSourcesContainer = document.getElementById("manage-data-sources-container");
      const reportingCurrency = document.getElementById("reporting-currency");
      const saveReportingCurrency = document.getElementById("save-reporting-currency");
      const currencyMessage = document.getElementById("currency-message");
      const sessionRequest = async (path, options = {}) => {
        if (!window.shopify || typeof window.shopify.idToken !== "function") throw new Error("SHOPIFY_SESSION_REQUIRED");
        const token = await window.shopify.idToken();
        const response = await fetch(path, {...options, credentials: "same-origin", headers: {Authorization: "Bearer " + token, ...(options.body ? {"Content-Type": "application/json"} : {})}});
        const body = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(body.code || "REQUEST_FAILED");
        return body;
      };
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
        const settings = await sessionRequest("/api/shopify/workspace/settings");
        if (settings.status === "configured") {
          setupDataSourcesContainer.hidden = true;
          manageDataSourcesContainer.hidden = false;
        } else {
          setupDataSourcesContainer.hidden = false;
          manageDataSourcesContainer.hidden = true;
        }
        document.documentElement.dataset.smoke = "pass";
      };
      saveReportingCurrency.addEventListener("click", async () => {
        saveReportingCurrency.disabled = true;
        saveReportingCurrency.loading = true;
        currencyMessage.textContent = "Saving…";
        try {
          await sessionRequest("/api/shopify/workspace/reporting-currency", {method: "POST", body: JSON.stringify({currency: String(reportingCurrency.value)})});
          location.assign("/shopify/app/platforms");
        } catch (error) {
          currencyMessage.textContent = error.message === "REPORTING_CURRENCY_ALREADY_CONFIGURED" ? "Currency is already configured. Open Data sources again." : "Currency could not be saved. Please try again.";
        } finally {
          saveReportingCurrency.disabled = false;
          saveReportingCurrency.loading = false;
        }
      });
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

function renderProviderSection({id, label, description, parked = false}, providerOAuthEnabled, providerAvailable = providerOAuthEnabled) {
  const disabled = providerAvailable ? "" : " disabled";
  return `<s-section id="${id}" heading="${label}">
      <s-stack direction="inline" gap="base" justify-content="space-between" align-items="center">
        <s-stack gap="tight">
          <s-paragraph>${description}</s-paragraph>
          ${["meta", "google_ads", "klaviyo"].includes(id) ? `<s-paragraph id="${id}-message" aria-live="polite">${providerAvailable ? "Checking connection status…" : "Connection setup unavailable"}</s-paragraph>` : ""}
          ${parked ? '<s-paragraph>Parked</s-paragraph>' : ""}
        </s-stack>
        ${parked ? '<s-button disabled>Unavailable</s-button>' : `<s-stack direction="inline" gap="tight"><div id="${id}-connect"><s-button variant="primary" commandFor="${id}-connect-modal" command="--show"${disabled}>Connect</s-button></div><div id="${id}-connected" hidden>${id === "klaviyo" ? `<s-button tone="critical" commandFor="${id}-disconnect-modal" command="--show">Disconnect</s-button>` : '<s-badge tone="success">Connected</s-badge>'}</div></s-stack>`}
      </s-stack>
      ${parked ? "" : `<s-modal id="${id}-connect-modal" heading="Connect ${label} to AdsTable?" size="small-100">
        <s-stack gap="base">
          <s-paragraph>You will continue to ${label} to authorize AdsTable. Authorization alone does not complete the connection.</s-paragraph>
          <s-paragraph>After authorization, you must select a verified account${id === "klaviyo" ? " and enter its Email Monthly Plan Cost" : ""}.</s-paragraph>
        </s-stack>
        <s-button slot="secondary-actions" commandFor="${id}-connect-modal" command="--hide">Cancel</s-button>
        <s-button slot="primary-action" variant="primary" data-provider="${id}" commandFor="${id}-connect-modal" command="--hide">Continue to ${label}</s-button>
      </s-modal>`}
      ${id === "klaviyo" && !parked ? `<s-modal id="${id}-disconnect-modal" heading="Disconnect ${label}?" size="small-100">
        <s-stack gap="base">
          <s-paragraph>AdsTable will stop new provider access and refresh activity for this connection.</s-paragraph>
          <s-paragraph>Historical analytics will be preserved.</s-paragraph>
          <s-paragraph id="${id}-disconnect-message" aria-live="polite"></s-paragraph>
        </s-stack>
        <s-button slot="secondary-actions" commandFor="${id}-disconnect-modal" command="--hide">Cancel</s-button>
        <s-button id="${id}-disconnect-confirm" slot="primary-action" variant="primary" tone="critical">Disconnect</s-button>
      </s-modal>` : ""}
      ${id === "klaviyo" && providerAvailable ? `<s-stack id="klaviyo-accounts" gap="base">
        <s-modal id="klaviyo-account-modal" heading="Finish Klaviyo setup">
          <div id="klaviyo-choice-step" hidden><s-stack gap="base">
            <s-paragraph>Select the Klaviyo account AdsTable may use.</s-paragraph>
            <s-select id="klaviyo-choice" label="Klaviyo account"></s-select>
            <s-button id="klaviyo-choose" variant="primary">Continue</s-button>
          </s-stack></div>
          <div id="klaviyo-cost-step" hidden><s-stack gap="base">
            <s-paragraph>The plan cost remains in the verified Klaviyo account currency. AdsTable reporting currency is handled separately.</s-paragraph>
            <s-number-field id="klaviyo-cost" label="Email Monthly Plan Cost" min="0" max="99999999.99" step="0.01"></s-number-field>
            <s-button id="klaviyo-save" variant="primary">Save and connect</s-button>
          </s-stack></div>
          <div id="klaviyo-retry-step" hidden><s-button id="klaviyo-retry">Try again</s-button></div>
          <s-button slot="secondary-actions" commandFor="klaviyo-account-modal" command="--hide">Cancel</s-button>
        </s-modal>
      </s-stack>` : ""}
      ${["meta", "google_ads"].includes(id) && providerAvailable ? `<s-stack id="${id}-accounts" gap="base">
        <s-modal id="${id}-account-modal" heading="Select ${label} account">
          <s-stack gap="base">
            <s-paragraph>Select between 1 and 3 accounts returned by ${label}.</s-paragraph>
            <s-choice-list id="${id}-choice" name="${id}-accounts" label="${label} accounts" details="You can connect up to 3 accounts." multiple></s-choice-list>
            <s-button id="${id}-save" variant="primary">Save and connect</s-button>
          </s-stack>
          <s-button slot="secondary-actions" commandFor="${id}-account-modal" command="--hide">Cancel</s-button>
        </s-modal>
      </s-stack>` : ""}
    </s-section>`;
}

function renderEmbeddedPlatforms({clientId, providerOAuthEnabled, providerAvailability = {}}) {
  if (typeof clientId !== "string" || !clientId.trim()) throw new TypeError("clientId is required");
  const providers = [
    {id: "meta", label: "Meta", description: "Meta advertising performance and spend."},
    {id: "google_ads", label: "Google Ads", description: "Google Ads performance and spend."},
    {id: "klaviyo", label: "Klaviyo", description: "Email performance and monthly plan cost."},
    {id: "tiktok", label: "TikTok", description: "TikTok connection is parked for a later release.", parked: true},
    {id: "pinterest", label: "Pinterest", description: "Pinterest connection is not available in this release.", parked: true},
  ];
  const sections = providers.map((provider) => renderProviderSection(provider, providerOAuthEnabled, provider.parked ? false : providerAvailability[provider.id] ?? providerOAuthEnabled)).join("\n    ");
  return `<!doctype html>
<html lang="en">
<head>
  ${documentHead({clientId, title: "Data sources — AdsTable"})}
</head>
<body>
  ${appNavigation()}
  <s-page heading="Data sources">
    <s-link slot="breadcrumb-actions" href="/shopify/app">Home</s-link>
    <s-banner id="status" heading="Data sources" tone="info" hidden></s-banner>
    <div id="r6d2-klaviyo-acceptance" hidden>
      <s-section heading="Klaviyo acceptance check">
        <s-stack gap="base">
          <s-paragraph>This one-time check reads the verified Klaviyo account, Campaign and Flow reporting APIs. It does not write Dataset V2.</s-paragraph>
          <s-paragraph id="r6d2-klaviyo-message" aria-live="polite"></s-paragraph>
          <s-button id="r6d2-klaviyo-run" variant="primary">Run read-only acceptance</s-button>
        </s-stack>
      </s-section>
    </div>
    <div id="currency-setup" hidden>
      <s-section heading="Finish setup">
        <s-button variant="primary" commandFor="platforms-currency-modal" command="--show">Choose reporting currency</s-button>
      </s-section>
    </div>
    <s-modal id="platforms-currency-modal" heading="Choose reporting currency" size="small-100">
      <s-stack gap="base">
        <s-paragraph>This is independent from Shopify and provider account currencies.</s-paragraph>
        <s-select id="reporting-currency" label="Reporting currency">
          ${["TRY","USD","EUR","GBP","JPY","CNY","AUD","CAD","CHF","SEK","NOK","DKK","PLN"].map(currency => `<s-option value="${currency}">${currency}</s-option>`).join("")}
        </s-select>
        <s-paragraph id="platforms-currency-message" aria-live="polite"></s-paragraph>
      </s-stack>
      <s-button slot="secondary-actions" commandFor="platforms-currency-modal" command="--hide">Cancel</s-button>
      <s-button id="save-reporting-currency" slot="primary-action" variant="primary">Save and continue</s-button>
    </s-modal>
    <div id="provider-sections" hidden>${sections}</div>
  </s-page>
  <script>
    (() => {
      "use strict";
      const status = document.getElementById("status");
      const currencySetup = document.getElementById("currency-setup");
      const providerSections = document.getElementById("provider-sections");
      const currency = document.getElementById("reporting-currency");
      const saveCurrency = document.getElementById("save-reporting-currency");
      const currencyMessage = document.getElementById("platforms-currency-message");
      const acceptancePanel = document.getElementById("r6d2-klaviyo-acceptance");
      const acceptanceButton = document.getElementById("r6d2-klaviyo-run");
      const acceptanceMessage = document.getElementById("r6d2-klaviyo-message");
      const params = new URLSearchParams(location.search);
      const sessionRequest = async (path, options = {}) => {
        if (!window.shopify || typeof window.shopify.idToken !== "function") throw new Error("SHOPIFY_SESSION_REQUIRED");
        const token = await window.shopify.idToken();
        const response = await fetch(path, {...options, headers: {Authorization: "Bearer " + token, ...(options.body ? {"Content-Type": "application/json"} : {})}});
        const body = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(body.code || "REQUEST_FAILED");
        return body;
      };
      const showProviders = reportingCurrency => {
        currencySetup.hidden = true;
        providerSections.hidden = false;
        status.hidden = true;
        (${initializeAdAccounts.toString()})();
        (${initializeKlaviyoAccounts.toString()})();
      };
      const loadSettings = async () => {
        try {
          const settings = await sessionRequest("/api/shopify/workspace/settings");
          if (settings.status === "configured") return showProviders(settings.reporting_currency);
          currencySetup.hidden = false;
          providerSections.hidden = true;
          status.hidden = true;
        } catch {
          currencySetup.hidden = true;
          providerSections.hidden = true;
          status.hidden = false;
          status.setAttribute("heading", "Open AdsTable from Shopify Admin");
          status.setAttribute("tone", "critical");
          status.textContent = "";
        }
      };
      saveCurrency.addEventListener("click", async () => {
        saveCurrency.disabled = true;
        saveCurrency.loading = true;
        try {
          const result = await sessionRequest("/api/shopify/workspace/reporting-currency", {method: "POST", body: JSON.stringify({currency: String(currency.value)})});
          showProviders(result.reporting_currency);
        } catch (error) {
          currencyMessage.textContent = error.message === "REPORTING_CURRENCY_ALREADY_CONFIGURED" ? "Currency is already configured. Reload Data sources." : "Please choose a supported currency and try again.";
        } finally { saveCurrency.disabled = false; saveCurrency.loading = false; }
      });
      if (params.get("acceptance") === "r6d2-klaviyo") {
        acceptancePanel.hidden = false;
        acceptanceButton.addEventListener("click", async () => {
          acceptanceButton.disabled = true;
          acceptanceButton.loading = true;
          acceptanceMessage.textContent = "Running the read-only checks…";
          try {
            const result = await sessionRequest("/api/shopify/providers/klaviyo/runtime/preflight", {method: "POST"});
            acceptanceMessage.textContent = result.status === "PASS_R6_D2_KLAVIYO_READ_ONLY_PREFLIGHT"
              ? "PASS — Account, Campaign, Flow, Time and FX checks succeeded. Dataset V2 writes: 0."
              : "The acceptance result could not be verified.";
          } catch (error) {
            acceptanceMessage.textContent = /^[A-Z0-9_]{1,64}$/.test(error.message || "") ? error.message : "KLAVIYO_PREFLIGHT_FAILED";
            acceptanceButton.disabled = false;
          } finally { acceptanceButton.loading = false; }
        });
      }
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
          if (!response.ok) throw new Error(body.code || "CONNECTION_START_FAILED");
          if (body.navigation !== "top_level" || typeof body.authorization_url !== "string") throw new Error("INVALID_OAUTH_RESPONSE");
          open(body.authorization_url, "_top");
        } catch (error) {
          status.setAttribute("heading", "Connection could not be started");
          status.setAttribute("tone", "critical");
          status.hidden = false;
          status.textContent = /^[A-Z0-9_]{1,64}$/.test(error.message || "") ? error.message : "CONNECTION_START_FAILED";
          button.disabled = false;
          button.loading = false;
        }
      }));
      loadSettings();
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
