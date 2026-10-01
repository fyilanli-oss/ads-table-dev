"use strict";

const {initializeKlaviyoAccounts} = require("./klaviyo-account-ui");
const {initializeAdAccounts} = require("./ad-account-ui");

const EMBEDDED_HOME_RELEASE = "r7b6-settings-standard-v3";

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
  <script src="https://cdn.shopify.com/shopifycloud/polaris-2.0-rc.js"></script>
  <title>${title}</title>`;
}

function appNavigation() {
  return `<s-app-nav>
    <s-link href="/shopify/app" rel="home">Dashboard</s-link>
    <s-link href="/shopify/app/funnel">Funnel</s-link>
    <s-link href="/shopify/app/analysis">Analysis</s-link>
    <s-link href="/shopify/app/settings">Settings</s-link>
  </s-app-nav>`;
}

function renderEmbeddedAppHome({clientId}) {
  if (typeof clientId !== "string" || !clientId.trim()) throw new TypeError("clientId is required");
  return `<!doctype html>
<html lang="en">
<head>
  ${documentHead({clientId, title: "Dashboard — AdsTable"})}
</head>
<body>
  ${appNavigation()}
  <s-page heading="Dashboard">
    <s-section heading="Store connection">
      <s-banner id="status" heading="Connecting to your store" tone="info">
        AdsTable is verifying the secure Shopify session.
      </s-banner>
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

function renderProviderSection({id, label, parked = false, showDivider = false}, providerOAuthEnabled, providerAvailable = providerOAuthEnabled) {
  const disabled = providerAvailable ? "" : " disabled";
  const connectCopy = {
    meta: [
      "Continue with the Facebook account that owns or can access the Meta ad accounts you intend to connect. Make sure that account is signed in on this device and browser.",
      "Authorization alone does not complete the connection. After authorization, select between 1 and 3 verified Meta ad accounts.",
    ],
    google_ads: [
      "Continue with a Google account that can access the Google Ads customer accounts you intend to connect. Google may describe the consent broadly; AdsTable uses the connection only to read approved Google Ads reporting data.",
      "Authorization alone does not complete the connection. After authorization, select between 1 and 3 verified Google Ads accounts.",
    ],
    klaviyo: [
      "Continue with a Klaviyo user that can access the account you intend to connect. AdsTable requests the read access needed for email performance reporting.",
      "Authorization alone does not complete the connection. After authorization, select one verified Klaviyo account and enter its Estimated 30-Day Klaviyo Email Spend in the account currency.",
    ],
  }[id] || [`Continue to ${label} to authorize AdsTable.`];
  const disconnectCopy = id === "google_ads"
    ? "AdsTable will revoke this Google authorization, including the retired Google Sheets and GA4 permissions, and stop future Google Ads access and token refresh."
    : `AdsTable will revoke this ${label} authorization and stop future provider access and token refresh for this connection.`;
  const supportsConnection = ["meta", "google_ads", "klaviyo"].includes(id);
  const supportsReporting = ["meta", "google_ads"].includes(id);
  return `<s-stack id="${id}" gap="base">
      <s-query-container>
        <s-grid gridTemplateColumns="@container (inline-size > 700px) 1fr auto, 1fr" gap="base" alignItems="center">
          <s-stack gap="small">
            <s-heading>${label}</s-heading>
            ${supportsConnection ? `<s-paragraph id="${id}-message" aria-live="polite">${providerAvailable ? "Checking connection status…" : "Connection setup unavailable"}</s-paragraph>` : ""}
            ${supportsReporting && providerAvailable ? `<s-stack id="${id}-reporting" display="none"><s-paragraph id="${id}-reporting-summary"></s-paragraph></s-stack>` : ""}
            ${parked ? '<s-paragraph>Parked</s-paragraph>' : ""}
          </s-stack>
          ${parked ? '<s-button disabled>Unavailable</s-button>' : `<s-stack direction="inline" gap="small" alignItems="center">
            <s-stack id="${id}-connect"><s-button variant="secondary" id="${id}-connect-action" commandFor="${id}-connect-modal" command="--show"${disabled}>Connect</s-button></s-stack>
            <s-stack id="${id}-resume" display="none"><s-button variant="secondary" id="${id}-resume-action">Resume setup</s-button></s-stack>
            <s-stack id="${id}-connected" display="none" direction="inline" gap="small" alignItems="center">
                 <s-badge tone="success" size="base">Connected</s-badge>
                 ${supportsConnection ? `<s-button variant="secondary" tone="critical" commandFor="${id}-disconnect-modal" command="--show">Disconnect</s-button>` : ""}
                 ${supportsReporting && providerAvailable ? `<s-button variant="secondary" id="${id}-reporting-action" commandFor="${id}-reporting-modal" command="--show">Reporting account</s-button>` : ""}
                  ${id === "klaviyo" && providerAvailable ? `<s-stack id="klaviyo-spend-controls" display="none" direction="inline" gap="small"><s-button variant="secondary" commandFor="klaviyo-spend-update-modal" command="--show">Update spend</s-button><s-button variant="secondary" id="klaviyo-spend-correct-open" commandFor="klaviyo-spend-correct-modal" command="--show">Change value</s-button></s-stack>` : ""}
            </s-stack>
          </s-stack>`}
        </s-grid>
      </s-query-container>
        ${showDivider ? "<s-divider></s-divider>" : ""}
      ${parked ? "" : `<s-modal id="${id}-connect-modal" heading="Connect ${label} to AdsTable?" size="small-100">
        <s-stack gap="base">
          ${connectCopy.map(paragraph => `<s-paragraph>${paragraph}</s-paragraph>`).join("")}
        </s-stack>
        <s-button slot="secondary-actions" commandFor="${id}-connect-modal" command="--hide">Cancel</s-button>
        <s-button slot="primary-action" variant="primary" data-provider="${id}" commandFor="${id}-connect-modal" command="--hide">Continue to ${label}</s-button>
      </s-modal>`}
      ${["meta", "google_ads", "klaviyo"].includes(id) && !parked ? `<s-modal id="${id}-disconnect-modal" heading="Disconnect ${label}?" size="small-100">
        <s-stack gap="base">
          <s-paragraph>${disconnectCopy}</s-paragraph>
          <s-paragraph>Historical analytics already stored will remain available. Other provider connections are not affected.</s-paragraph>
          <s-paragraph>Data from the disconnected period may not be recovered automatically after reconnection.</s-paragraph>
          <s-paragraph id="${id}-disconnect-message" aria-live="polite"></s-paragraph>
        </s-stack>
        <s-button slot="secondary-actions" commandFor="${id}-disconnect-modal" command="--hide">Cancel</s-button>
        <s-button id="${id}-disconnect-confirm" slot="primary-action" variant="primary" tone="critical">Disconnect</s-button>
      </s-modal>` : ""}
      ${id === "klaviyo" && providerAvailable ? `<s-stack id="klaviyo-accounts" gap="base">
        <s-modal id="klaviyo-account-modal" heading="Finish Klaviyo setup">
          <s-stack id="klaviyo-choice-step" display="none" gap="base">
            <s-paragraph>Select the Klaviyo account AdsTable may use.</s-paragraph>
            <s-select id="klaviyo-choice" label="Klaviyo account"></s-select>
            <s-button id="klaviyo-choose" variant="primary">Continue</s-button>
          </s-stack>
          <s-stack id="klaviyo-cost-step" display="none" gap="base">
            <s-paragraph>Enter your estimated Klaviyo email cost for a 30-day period. SMS costs are not included.</s-paragraph>
            <s-paragraph>The value remains in the verified Klaviyo account currency. AdsTable reporting currency is handled separately.</s-paragraph>
            <s-number-field id="klaviyo-cost" label="Estimated 30-Day Klaviyo Email Spend" min="0" max="99999999.99" step="0.01"></s-number-field>
            <s-button id="klaviyo-save" variant="primary">Save and connect</s-button>
          </s-stack>
          <s-stack id="klaviyo-retry-step" display="none"><s-button id="klaviyo-retry">Try again</s-button></s-stack>
          <s-button slot="secondary-actions" commandFor="klaviyo-account-modal" command="--hide">Cancel</s-button>
        </s-modal>
        <s-modal id="klaviyo-spend-update-modal" heading="Update Klaviyo email spend" size="small-100">
          <s-stack gap="base">
            <s-paragraph>Enter the new estimated Klaviyo email cost for the next 30-day period starting today. SMS costs are not included.</s-paragraph>
            <s-number-field id="klaviyo-spend-update-value" label="Estimated 30-Day Klaviyo Email Spend" min="0" max="99999999.99" step="0.01"></s-number-field>
            <s-paragraph id="klaviyo-spend-update-message" aria-live="polite"></s-paragraph>
          </s-stack>
          <s-button slot="secondary-actions" commandFor="klaviyo-spend-update-modal" command="--hide">Cancel</s-button>
          <s-button id="klaviyo-spend-update-save" slot="primary-action" variant="primary">Save new value</s-button>
        </s-modal>
        <s-modal id="klaviyo-spend-correct-modal" heading="Change Klaviyo email spend" size="small-100">
          <s-stack gap="base">
            <s-paragraph>Change a previously saved amount. Its original start date will not change.</s-paragraph>
            <s-select id="klaviyo-spend-history-choice" label="Saved period"></s-select>
            <s-number-field id="klaviyo-spend-correct-value" label="Updated 30-Day Klaviyo Email Spend" min="0" max="99999999.99" step="0.01"></s-number-field>
            <s-paragraph id="klaviyo-spend-correct-message" aria-live="polite"></s-paragraph>
          </s-stack>
          <s-button slot="secondary-actions" commandFor="klaviyo-spend-correct-modal" command="--hide">Cancel</s-button>
          <s-button id="klaviyo-spend-correct-save" slot="primary-action" variant="primary">Save change</s-button>
        </s-modal>
      </s-stack>` : ""}
      ${["meta", "google_ads"].includes(id) && providerAvailable ? `<s-stack id="${id}-accounts" gap="base">
        <s-modal id="${id}-account-modal" heading="Select ${label} account">
          <s-stack gap="base">
            <s-paragraph>Select between 1 and 3 accounts returned by ${label}.</s-paragraph>
            <s-paragraph>You can connect up to 3 accounts.</s-paragraph>
            <s-stack id="${id}-choice" gap="small"></s-stack>
            <s-paragraph id="${id}-choice-error" aria-live="polite"></s-paragraph>
            <s-button id="${id}-save" variant="primary" disabled>Save and connect</s-button>
          </s-stack>
          <s-button slot="secondary-actions" commandFor="${id}-account-modal" command="--hide">Cancel</s-button>
        </s-modal>
      </s-stack>` : ""}
      ${["meta", "google_ads"].includes(id) && providerAvailable ? `<s-modal id="${id}-reporting-modal" heading="${label} reporting account" size="small-100">
        <s-stack gap="base">
          <s-paragraph>Choose the single connected account AdsTable will show in Dashboard, Funnel and Analysis.</s-paragraph>
          <s-paragraph>Changing this preference does not reconnect ${label}, remove connected accounts or delete historical data.</s-paragraph>
          <s-select id="${id}-reporting-choice" label="Reporting account"></s-select>
          <s-paragraph id="${id}-reporting-message" aria-live="polite"></s-paragraph>
        </s-stack>
        <s-button slot="secondary-actions" commandFor="${id}-reporting-modal" command="--hide">Cancel</s-button>
        <s-button id="${id}-reporting-save" slot="primary-action" variant="primary">Save reporting account</s-button>
      </s-modal>` : ""}
    </s-stack>`;
}

function renderEmbeddedPlatforms({clientId, providerOAuthEnabled, providerAvailability = {}}) {
  if (typeof clientId !== "string" || !clientId.trim()) throw new TypeError("clientId is required");
  const providers = [
    {id: "meta", label: "Meta"},
    {id: "google_ads", label: "Google Ads"},
    {id: "klaviyo", label: "Klaviyo"},
  ];
  const sections = providers.map((provider, index) => renderProviderSection(
    {...provider, showDivider: index < providers.length - 1},
    providerOAuthEnabled,
    providerAvailability[provider.id] ?? providerOAuthEnabled,
  )).join("\n    ");
  return `<!doctype html>
<html lang="en">
<head>
  ${documentHead({clientId, title: "Settings — AdsTable"})}
</head>
<body>
  ${appNavigation()}
  <s-page heading="Settings" inlineSize="base">
    <s-banner id="status" heading="Settings" tone="info" hidden></s-banner>
    <s-stack id="r6d4-google-acceptance" display="none">
      <s-section heading="Google Ads acceptance check">
        <s-stack gap="base">
          <s-paragraph>This one-time check reads the selected Google Ads accounts, Standard Ads and Performance Max Asset Groups through the completed E5 contract. It does not write Dataset V2.</s-paragraph>
          <s-paragraph id="r6d4-google-message" aria-live="polite"></s-paragraph>
          <s-button id="r6d4-google-run" variant="primary">Run read-only acceptance</s-button>
          <s-stack id="r6d4-google-dataset-step" gap="base">
              <s-paragraph>This controlled acceptance may write only real provider-verified Google Ads rows to Dataset V2. A verified empty result writes no synthetic rows and does not enable schedules or backfill.</s-paragraph>
              <s-paragraph id="r6d4-google-dataset-message" aria-live="polite"></s-paragraph>
              <s-button id="r6d4-google-dataset-run" tone="critical">Run controlled Dataset V2 acceptance</s-button>
          </s-stack>
        </s-stack>
      </s-section>
    </s-stack>
    <s-stack id="r6d3-meta-acceptance" display="none">
      <s-section heading="Meta acceptance check">
        <s-stack gap="base">
          <s-paragraph>This one-time check reads the selected Meta accounts and daily Insights through the completed E4 contract. It does not write Dataset V2.</s-paragraph>
          <s-paragraph id="r6d3-meta-message" aria-live="polite"></s-paragraph>
          <s-button id="r6d3-meta-run" variant="primary">Run read-only acceptance</s-button>
          <s-stack id="r6d3-meta-dataset-step" gap="base">
              <s-paragraph>This controlled acceptance may write real provider-verified Meta rows to Dataset V2. A verified empty result writes no synthetic rows and does not enable schedules or backfill.</s-paragraph>
              <s-paragraph id="r6d3-meta-dataset-message" aria-live="polite"></s-paragraph>
              <s-button id="r6d3-meta-dataset-run" tone="critical">Run controlled Dataset V2 acceptance</s-button>
          </s-stack>
        </s-stack>
      </s-section>
    </s-stack>
    <s-stack id="r6d2-klaviyo-acceptance" display="none">
      <s-section heading="Klaviyo acceptance check">
        <s-stack gap="base">
          <s-paragraph>This one-time check reads the verified Klaviyo account, Campaign and Flow reporting APIs for one closed provider date. It does not write Dataset V2.</s-paragraph>
          <s-date-field id="r6d2-klaviyo-provider-date" label="Provider date" details="Choose the closed date when the Campaign or Flow message was sent."></s-date-field>
          <s-paragraph id="r6d2-klaviyo-message" aria-live="polite"></s-paragraph>
          <s-button id="r6d2-klaviyo-run" variant="primary">Run read-only acceptance</s-button>
          <s-stack id="r6d2-klaviyo-c6-step" gap="base">
              <s-paragraph>This controlled acceptance may write real verified Klaviyo rows to Dataset V2. It never creates synthetic rows and does not enable scheduled production activation.</s-paragraph>
              <s-paragraph id="r6d2-klaviyo-c6-message" aria-live="polite"></s-paragraph>
              <s-button id="r6d2-klaviyo-c6-run" tone="critical" disabled>Run controlled Dataset V2 acceptance</s-button>
          </s-stack>
          <s-stack id="r6d2-klaviyo-metric-step" display="none" gap="base">
              <s-paragraph>Confirming stores only this workspace account's verified reporting metric. It does not write Dataset V2.</s-paragraph>
              <s-select id="r6d2-klaviyo-metric" label="Placed Order metric"></s-select>
              <s-button id="r6d2-klaviyo-metric-confirm" variant="primary">Confirm metric and continue</s-button>
          </s-stack>
        </s-stack>
      </s-section>
    </s-stack>
    <s-stack id="r6d5-klaviyo-historical-inventory" display="none">
      <s-section heading="Klaviyo historical test-data inventory">
        <s-stack gap="base">
          <s-paragraph>This read-only check finds closed dates for sent Klaviyo campaigns and evaluates only the most recent date through the existing Campaign and Flow mapping. It does not write Dataset V2.</s-paragraph>
          <s-paragraph id="r6d5-klaviyo-historical-message" aria-live="polite"></s-paragraph>
          <s-button id="r6d5-klaviyo-historical-run" variant="primary">Run historical read-only inventory</s-button>
          <s-paragraph>This separate read-only check inventories Flow status and known synthetic Event categories. Events remain diagnostic evidence and are not written as Campaign or Flow performance rows.</s-paragraph>
          <s-paragraph id="r6d5-klaviyo-flow-event-message" aria-live="polite"></s-paragraph>
          <s-button id="r6d5-klaviyo-flow-event-run" variant="primary">Run Flow/Event read-only inventory</s-button>
        </s-stack>
      </s-section>
    </s-stack>
    <s-stack gap="large">
      <s-stack gap="small">
        <s-heading>Reporting Currency</s-heading>
        <s-box padding="base" background="base" borderWidth="base" borderColor="base" borderRadius="base" accessibilityLabel="Reporting Currency">
      <s-stack id="reporting-currency-summary" display="none" gap="small">
          <s-paragraph id="reporting-currency-value"></s-paragraph>
          <s-paragraph>This currency is fixed for the workspace and cannot be changed after confirmation.</s-paragraph>
      </s-stack>
      <s-stack id="currency-setup" display="none" gap="base">
          <s-paragraph>Choose carefully. Reporting Currency cannot be changed after confirmation.</s-paragraph>
          <s-button variant="primary" commandFor="platforms-currency-modal" command="--show">Choose reporting currency</s-button>
      </s-stack>
        </s-box>
      </s-stack>
    <s-modal id="platforms-currency-modal" heading="Choose reporting currency" size="small-100">
      <s-stack id="currency-selection-step" gap="base">
          <s-paragraph>This is independent from Shopify and provider account currencies.</s-paragraph>
          <s-select id="reporting-currency" label="Reporting currency">
            ${["TRY","USD","EUR","GBP","JPY","CNY","AUD","CAD","CHF","SEK","NOK","DKK","PLN"].map(currency => `<s-option value="${currency}">${currency}</s-option>`).join("")}
          </s-select>
          <s-button id="review-reporting-currency" variant="primary">Review selection</s-button>
      </s-stack>
      <s-stack id="currency-confirmation-step" display="none" gap="base">
          <s-paragraph id="reporting-currency-confirmation"></s-paragraph>
          <s-paragraph>This choice is permanent for this workspace. Removing the app does not delete data or reset this currency.</s-paragraph>
          <s-button id="back-to-currency-selection">Back</s-button>
          <s-button id="save-reporting-currency" variant="primary">Confirm reporting currency</s-button>
      </s-stack>
      <s-paragraph id="platforms-currency-message" aria-live="polite"></s-paragraph>
      <s-button slot="secondary-actions" commandFor="platforms-currency-modal" command="--hide">Cancel</s-button>
    </s-modal>
      <s-stack gap="small">
        <s-heading>Platforms</s-heading>
        <s-box id="provider-sections" display="none" padding="base" background="base" borderWidth="base" borderColor="base" borderRadius="base" accessibilityLabel="Platforms">
          <s-stack gap="base">${sections}</s-stack>
        </s-box>
      </s-stack>
    </s-stack>
  </s-page>
  <script>
    (() => {
      "use strict";
      const status = document.getElementById("status");
      const currencySetup = document.getElementById("currency-setup");
      const currencySummary = document.getElementById("reporting-currency-summary");
      const currencyValue = document.getElementById("reporting-currency-value");
      const providerSections = document.getElementById("provider-sections");
      const currency = document.getElementById("reporting-currency");
      const selectionStep = document.getElementById("currency-selection-step");
      const confirmationStep = document.getElementById("currency-confirmation-step");
      const confirmationText = document.getElementById("reporting-currency-confirmation");
      const reviewCurrency = document.getElementById("review-reporting-currency");
      const backToSelection = document.getElementById("back-to-currency-selection");
      const saveCurrency = document.getElementById("save-reporting-currency");
      const currencyMessage = document.getElementById("platforms-currency-message");
      const googleAcceptancePanel = document.getElementById("r6d4-google-acceptance");
      const googleAcceptanceButton = document.getElementById("r6d4-google-run");
      const googleAcceptanceMessage = document.getElementById("r6d4-google-message");
      const googleDatasetAcceptanceButton = document.getElementById("r6d4-google-dataset-run");
      const googleDatasetAcceptanceMessage = document.getElementById("r6d4-google-dataset-message");
      const metaAcceptancePanel = document.getElementById("r6d3-meta-acceptance");
      const metaAcceptanceButton = document.getElementById("r6d3-meta-run");
      const metaAcceptanceMessage = document.getElementById("r6d3-meta-message");
      const metaDatasetAcceptanceButton = document.getElementById("r6d3-meta-dataset-run");
      const metaDatasetAcceptanceMessage = document.getElementById("r6d3-meta-dataset-message");
      const acceptancePanel = document.getElementById("r6d2-klaviyo-acceptance");
      const acceptanceButton = document.getElementById("r6d2-klaviyo-run");
      const acceptanceProviderDate = document.getElementById("r6d2-klaviyo-provider-date");
      const acceptanceMessage = document.getElementById("r6d2-klaviyo-message");
      const metricStep = document.getElementById("r6d2-klaviyo-metric-step");
      const metricSelect = document.getElementById("r6d2-klaviyo-metric");
      const metricConfirm = document.getElementById("r6d2-klaviyo-metric-confirm");
      const datasetAcceptanceButton = document.getElementById("r6d2-klaviyo-c6-run");
      const datasetAcceptanceMessage = document.getElementById("r6d2-klaviyo-c6-message");
      const historicalInventoryPanel = document.getElementById("r6d5-klaviyo-historical-inventory");
      const historicalInventoryButton = document.getElementById("r6d5-klaviyo-historical-run");
      const historicalInventoryMessage = document.getElementById("r6d5-klaviyo-historical-message");
      const flowEventInventoryButton = document.getElementById("r6d5-klaviyo-flow-event-run");
      const flowEventInventoryMessage = document.getElementById("r6d5-klaviyo-flow-event-message");
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
        currencySetup.display = "none";
        currencySummary.display = "auto";
        currencyValue.textContent = "Reporting Currency: " + reportingCurrency;
        providerSections.display = "auto";
        status.hidden = true;
        if (providerSections.dataset.initialized !== "true") {
          providerSections.dataset.initialized = "true";
          (${initializeAdAccounts.toString()})();
          (${initializeKlaviyoAccounts.toString()})();
        }
      };
      const loadSettings = async () => {
        try {
          const settings = await sessionRequest("/api/shopify/workspace/settings");
          if (settings.status === "configured") return showProviders(settings.reporting_currency);
          currencySetup.display = "auto";
          currencySummary.display = "none";
          providerSections.display = "none";
          status.hidden = true;
        } catch {
          currencySetup.display = "none";
          currencySummary.display = "none";
          providerSections.display = "none";
          status.hidden = false;
          status.setAttribute("heading", "Open AdsTable from Shopify Admin");
          status.setAttribute("tone", "critical");
          status.textContent = "";
        }
      };
      reviewCurrency.addEventListener("click", () => {
        confirmationText.textContent = "Confirm " + String(currency.value) + " as the permanent Reporting Currency.";
        selectionStep.display = "none";
        confirmationStep.display = "auto";
        currencyMessage.textContent = "";
      });
      backToSelection.addEventListener("click", () => {
        confirmationStep.display = "none";
        selectionStep.display = "auto";
        currencyMessage.textContent = "";
      });
      saveCurrency.addEventListener("click", async () => {
        saveCurrency.disabled = true;
        saveCurrency.loading = true;
        try {
          const result = await sessionRequest("/api/shopify/workspace/reporting-currency", {method: "POST", body: JSON.stringify({currency: String(currency.value)})});
          showProviders(result.reporting_currency);
          location.assign("/shopify/app/settings");
        } catch (error) {
          currencyMessage.textContent = error.message === "REPORTING_CURRENCY_ALREADY_CONFIGURED" ? "Currency is already configured. Reload Settings." : "Please choose a supported currency and try again.";
        } finally { saveCurrency.disabled = false; saveCurrency.loading = false; }
      });
      if (params.get("acceptance") === "r6d5-klaviyo") {
        historicalInventoryPanel.display = "auto";
        historicalInventoryButton.addEventListener("click", async () => {
          historicalInventoryButton.disabled = true;
          historicalInventoryButton.loading = true;
          historicalInventoryMessage.textContent = "Inspecting closed sent-campaign dates without writing Dataset V2…";
          try {
            const result = await sessionRequest("/api/shopify/providers/klaviyo/runtime/historical-inventory", {method: "POST"});
            if (result.status !== "PASS_R6_D5_A_KLAVIYO_HISTORICAL_INVENTORY") throw new Error("KLAVIYO_HISTORICAL_INVENTORY_FAILED");
            const campaignSummary = result.total_campaign_count + " campaign(s), " + result.sent_campaign_count + " Sent; " +
              result.sent_with_scheduled_at_count + " with scheduled_at, " +
              result.sent_without_scheduled_at_count + " without scheduled_at. ";
            historicalInventoryMessage.textContent = result.checked_date_count === 0
              ? "PASS — " + campaignSummary + "No closed sent date. Dataset V2 writes: 0."
              : "PASS — " + campaignSummary + result.closed_sent_date_count + " closed sent date(s) found; " +
                result.row_count + " verified row(s) mapped on " + result.provider_date + ". Dataset V2 writes: 0.";
          } catch (error) {
            historicalInventoryMessage.textContent = /^[A-Z0-9_]{1,64}$/.test(error.message || "") ? error.message : "KLAVIYO_HISTORICAL_INVENTORY_FAILED";
            historicalInventoryButton.disabled = false;
          } finally { historicalInventoryButton.loading = false; }
        });
        flowEventInventoryButton.addEventListener("click", async () => {
          flowEventInventoryButton.disabled = true;
          flowEventInventoryButton.loading = true;
          flowEventInventoryMessage.textContent = "Inspecting Flow status and Event dates without writing Dataset V2…";
          try {
            const result = await sessionRequest("/api/shopify/providers/klaviyo/runtime/flow-event-inventory", {method: "POST"});
            if (result.status !== "PASS_R6_D5_A2_KLAVIYO_FLOW_EVENT_INVENTORY") throw new Error("KLAVIYO_FLOW_EVENT_INVENTORY_FAILED");
            const counts = result.event_counts;
            const range = result.earliest_event_date
              ? result.earliest_event_date + " to " + result.latest_event_date
              : "no event date";
            flowEventInventoryMessage.textContent =
              "PASS — " + result.flow_count + " flow(s): " + result.flow_status_counts.live + " live, " +
              result.flow_status_counts.manual + " manual, " + result.flow_status_counts.draft + " draft, " +
              result.flow_status_counts.other + " other. " + result.scanned_event_count + " event(s) scanned (" + range + "), " +
              result.attributed_event_count + " attributed; Received Email " + counts.received_email +
              ", Opened Email " + counts.opened_email + ", Clicked Email " + counts.clicked_email +
              ", Added to Cart " + counts.added_to_cart + ", Started Checkout " + counts.started_checkout +
              ", Placed Order " + counts.placed_order + ". Scan truncated: " + (result.event_scan_truncated ? "yes" : "no") +
              ". Dataset V2 writes: 0.";
          } catch (error) {
            flowEventInventoryMessage.textContent = /^[A-Z0-9_]{1,64}$/.test(error.message || "") ? error.message : "KLAVIYO_FLOW_EVENT_INVENTORY_FAILED";
            flowEventInventoryButton.disabled = false;
          } finally { flowEventInventoryButton.loading = false; }
        });
      }
      if (params.get("acceptance") === "r6d2-klaviyo") {
        acceptancePanel.display = "auto";
        const latestClosedDate = new Date(Date.now() - (48 * 60 * 60 * 1000)).toISOString().slice(0, 10);
        acceptanceProviderDate.value = latestClosedDate;
        acceptanceProviderDate.setAttribute("allow", "--" + latestClosedDate);
        const acceptanceRequest = () => JSON.stringify({provider_date: String(acceptanceProviderDate.value || "")});
        const showAcceptanceResult = result => {
          if (result.status !== "PASS_R6_D2_KLAVIYO_READ_ONLY_PREFLIGHT") {
            datasetAcceptanceButton.disabled = true;
            acceptanceMessage.textContent = "The acceptance result could not be verified.";
            return;
          }
          datasetAcceptanceButton.disabled = result.empty_provider_result === true;
          const outcome = result.empty_provider_result ? "VERIFIED EMPTY" : "PASS";
          acceptanceMessage.textContent = outcome + " — " + result.provider_date + ": " +
            result.campaign_row_count + " Campaign row(s), " + result.flow_row_count +
            " Flow row(s). Account, Time and FX checks succeeded. Dataset V2 writes: 0.";
        };
        acceptanceProviderDate.addEventListener("change", () => {
          datasetAcceptanceButton.disabled = true;
          acceptanceMessage.textContent = "Run the read-only acceptance for the selected provider date.";
          datasetAcceptanceMessage.textContent = "";
        });
        acceptanceButton.addEventListener("click", async () => {
          acceptanceButton.disabled = true;
          acceptanceButton.loading = true;
          acceptanceMessage.textContent = "Running the read-only checks…";
          try {
            const result = await sessionRequest("/api/shopify/providers/klaviyo/runtime/preflight", {method: "POST", body: acceptanceRequest()});
            showAcceptanceResult(result);
          } catch (error) {
            if (error.message === "KLAVIYO_PREFLIGHT_METRIC_REQUIRED") {
              try {
                const discovery = await sessionRequest("/api/shopify/providers/klaviyo/runtime/metrics");
                metricSelect.replaceChildren();
                discovery.candidates.forEach(candidate => {
                  const option = document.createElement("s-option");
                  option.value = candidate.id;
                  option.textContent = candidate.name + " — " + candidate.integration_name + (candidate.integration_category ? " (" + candidate.integration_category + ")" : "");
                  metricSelect.appendChild(option);
                });
                metricSelect.value = discovery.candidates[0]?.id || "";
                metricStep.display = "auto";
                acceptanceMessage.textContent = "Select the provider-verified sales source. AdsTable will bind its exact Added to Cart, Checkout and Placed Order metrics for this workspace and Klaviyo account.";
              } catch (discoveryError) {
                acceptanceMessage.textContent = /^[A-Z0-9_]{1,64}$/.test(discoveryError.message || "") ? discoveryError.message : "KLAVIYO_METRIC_DISCOVERY_FAILED";
              }
            } else {
              acceptanceMessage.textContent = /^[A-Z0-9_]{1,64}$/.test(error.message || "") ? error.message : "KLAVIYO_PREFLIGHT_FAILED";
            }
            acceptanceButton.disabled = false;
          } finally { acceptanceButton.loading = false; }
        });
        datasetAcceptanceButton.addEventListener("click", async () => {
          datasetAcceptanceButton.disabled = true;
          datasetAcceptanceButton.loading = true;
          datasetAcceptanceMessage.textContent = "Running one controlled Dataset V2 acceptance…";
          try {
            const result = await sessionRequest("/api/shopify/providers/klaviyo/runtime/acceptance", {
              method: "POST",
              body: JSON.stringify({confirmation: "RUN_R6_D2_C6_KLAVIYO_WRITE", provider_date: String(acceptanceProviderDate.value || "")}),
            });
            datasetAcceptanceMessage.textContent = result.status === "PASS_R6_D2_C6_KLAVIYO_DATASET_WRITE"
              ? "PASS — " + result.provider_date + ": attempted " + result.attempted + ", persisted " + result.persisted +
                "; Campaign rows " + result.campaign_row_count + ", Flow rows " + result.flow_row_count + "."
              : "The Dataset V2 acceptance result could not be verified.";
          } catch (error) {
            datasetAcceptanceMessage.textContent = (/^[A-Z0-9_]{1,64}$/.test(error.message || "") ? error.message : "KLAVIYO_DATASET_ACCEPTANCE_FAILED") + ". Do not retry; review runtime evidence.";
          } finally { datasetAcceptanceButton.loading = false; }
        });
        metricConfirm.addEventListener("click", async () => {
          metricConfirm.disabled = true;
          metricConfirm.loading = true;
            acceptanceMessage.textContent = "Verifying and binding the Klaviyo commerce metrics…";
          try {
            await sessionRequest("/api/shopify/providers/klaviyo/runtime/metrics/select", {method: "POST", body: JSON.stringify({metric_id: String(metricSelect.value || "")})});
            metricStep.display = "none";
            acceptanceMessage.textContent = "Metric confirmed. Running the read-only acceptance…";
            showAcceptanceResult(await sessionRequest("/api/shopify/providers/klaviyo/runtime/preflight", {method: "POST", body: acceptanceRequest()}));
          } catch (error) {
            acceptanceMessage.textContent = /^[A-Z0-9_]{1,64}$/.test(error.message || "") ? error.message : "KLAVIYO_PREFLIGHT_FAILED";
            metricConfirm.disabled = false;
          } finally { metricConfirm.loading = false; }
        });
      }
      if (params.get("acceptance") === "r6d3-meta") {
        metaAcceptancePanel.display = "auto";
        metaAcceptanceButton.addEventListener("click", async () => {
          metaAcceptanceButton.disabled = true;
          metaAcceptanceButton.loading = true;
          metaAcceptanceMessage.textContent = "Running the read-only checks…";
          try {
            const result = await sessionRequest("/api/shopify/providers/meta/runtime/preflight", {method: "POST"});
            metaAcceptanceMessage.textContent = result.status === "PASS_R6_D3_D_META_READ_ONLY_PREFLIGHT"
              ? "PASS — " + result.selected_account_count + " account(s), " + result.row_count + " verified row(s), Time and FX checks succeeded. Dataset V2 writes: 0."
              : "The acceptance result could not be verified.";
          } catch (error) {
            metaAcceptanceMessage.textContent = /^[A-Z0-9_]{1,64}$/.test(error.message || "") ? error.message : "META_PREFLIGHT_FAILED";
            metaAcceptanceButton.disabled = false;
          } finally { metaAcceptanceButton.loading = false; }
        });
        metaDatasetAcceptanceButton.addEventListener("click", async () => {
          metaDatasetAcceptanceButton.disabled = true;
          metaDatasetAcceptanceButton.loading = true;
          metaDatasetAcceptanceMessage.textContent = "Running one controlled Dataset V2 acceptance…";
          try {
            const result = await sessionRequest("/api/shopify/providers/meta/runtime/acceptance", {
              method: "POST",
              body: JSON.stringify({confirmation: "RUN_R6_D3_E_META_WRITE"}),
            });
            metaDatasetAcceptanceMessage.textContent = result.status === "PASS_R6_D3_E_META_DATASET_WRITE"
              ? "PASS — attempted: " + result.attempted + ", persisted: " + result.persisted + ", verified empty: " + result.empty_provider_result + "."
              : "The Dataset V2 acceptance result could not be verified.";
          } catch (error) {
            metaDatasetAcceptanceMessage.textContent = (/^[A-Z0-9_]{1,64}$/.test(error.message || "") ? error.message : "META_DATASET_ACCEPTANCE_FAILED") + ". Do not retry; review runtime evidence.";
          } finally { metaDatasetAcceptanceButton.loading = false; }
        });
      }
      if (params.get("acceptance") === "r6d4-google") {
        googleAcceptancePanel.display = "auto";
        googleAcceptanceButton.addEventListener("click", async () => {
          googleAcceptanceButton.disabled = true;
          googleAcceptanceButton.loading = true;
          googleAcceptanceMessage.textContent = "Running the read-only checks…";
          try {
            const result = await sessionRequest("/api/shopify/providers/google_ads/runtime/preflight", {method: "POST"});
            googleAcceptanceMessage.textContent = result.status === "PASS_R6_D4_D_GOOGLE_READ_ONLY_PREFLIGHT"
              ? "PASS — " + result.selected_account_count + " account(s), " + result.row_count + " verified row(s) across Standard and Performance Max. Time and FX checks succeeded. Dataset V2 writes: 0."
              : "The acceptance result could not be verified.";
          } catch (error) {
            googleAcceptanceMessage.textContent = /^[A-Z0-9_]{1,64}$/.test(error.message || "") ? error.message : "GOOGLE_PREFLIGHT_FAILED";
            googleAcceptanceButton.disabled = false;
          } finally { googleAcceptanceButton.loading = false; }
        });
        googleDatasetAcceptanceButton.addEventListener("click", async () => {
          googleDatasetAcceptanceButton.disabled = true;
          googleDatasetAcceptanceButton.loading = true;
          googleDatasetAcceptanceMessage.textContent = "Running one controlled Dataset V2 acceptance…";
          try {
            const result = await sessionRequest("/api/shopify/providers/google_ads/runtime/acceptance", {
              method: "POST",
              body: JSON.stringify({confirmation: "RUN_R6_D4_E_GOOGLE_WRITE"}),
            });
            googleDatasetAcceptanceMessage.textContent = result.status === "PASS_R6_D4_E_GOOGLE_DATASET_WRITE"
              ? "PASS — attempted: " + result.attempted + ", persisted: " + result.persisted + ", verified empty: " + result.empty_provider_result + "."
              : "The Dataset V2 acceptance result could not be verified.";
          } catch (error) {
            googleDatasetAcceptanceMessage.textContent = (/^[A-Z0-9_]{1,64}$/.test(error.message || "") ? error.message : "GOOGLE_DATASET_ACCEPTANCE_FAILED") + ". Do not retry; review runtime evidence.";
          } finally { googleDatasetAcceptanceButton.loading = false; }
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
      if (params.get("oauth_error") === "reauthorization_required" && params.get("provider") === "meta") {
        const reconnect = document.getElementById("meta-connect-action");
        const message = document.getElementById("meta-message");
        if (reconnect) reconnect.textContent = "Reconnect Meta";
        if (message) message.textContent = "Meta authorization must be renewed before account selection.";
        status.setAttribute("heading", "Reconnect Meta");
        status.setAttribute("tone", "critical");
        status.textContent = "AdsTable could not verify a valid Meta authorization.";
        status.hidden = false;
      } else if (params.has("oauth_connected")) {
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

function renderEmbeddedPlaceholder({clientId, page}) {
  if (typeof clientId !== "string" || !clientId.trim()) throw new TypeError("clientId is required");
  if (!["Funnel", "Analysis"].includes(page)) throw new TypeError("page is required");
  return `<!doctype html>
<html lang="en">
<head>
  ${documentHead({clientId, title: page + " — AdsTable"})}
</head>
<body>
  ${appNavigation()}
  <s-page heading="${page}">
    <s-banner heading="${page}" tone="info">This Shopify-native workspace surface is reserved for the planned ${page} package.</s-banner>
  </s-page>
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
  const settingsHandler = (_req, res) => {
    setEmbeddedHeaders(res);
    return res.type("html").send(html);
  };
  app.get("/shopify/app/settings", settingsHandler);
  app.get("/shopify/app/platforms", settingsHandler);
  for (const page of ["Funnel", "Analysis"]) {
    app.get("/shopify/app/" + page.toLowerCase(), (_req, res) => {
      setEmbeddedHeaders(res);
      return res.type("html").send(renderEmbeddedPlaceholder({clientId, page}));
    });
  }
}

module.exports = Object.freeze({EMBEDDED_HOME_RELEASE, registerEmbeddedAppHome, renderEmbeddedAppHome, registerEmbeddedPlatforms, renderEmbeddedPlatforms, renderEmbeddedPlaceholder});
