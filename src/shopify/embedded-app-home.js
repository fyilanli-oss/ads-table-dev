"use strict";

const EMBEDDED_HOME_RELEASE = "e10-t6c2j";

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
  <meta http-equiv="Cache-Control" content="no-store, no-cache, must-revalidate">
  <style>body{font:16px system-ui;margin:0;background:#f6f6f7;color:#202223}main{max-width:760px;margin:auto;padding:32px 20px}.platforms-card{margin-top:20px;padding:24px;border:1px solid #c9cccf;border-radius:12px;background:#fff;box-shadow:0 1px 2px rgba(0,0,0,.08)}.platforms-card h2{margin:0 0 8px;font-size:22px}.platforms-card p{margin:0}.primary-action{display:block;margin-top:18px;padding:14px 20px;border-radius:8px;background:#008060;color:#fff;text-align:center;text-decoration:none;font-size:17px;font-weight:700}.release{margin-top:12px;color:#6d7175;font-size:12px}</style>
</head>
<body>
  <main>
    <h1>AdsTable</h1>
    <p id="status" role="status" aria-live="polite">Securing your development-store connection…</p>
    <section class="platforms-card" aria-labelledby="platforms-heading" data-release="${EMBEDDED_HOME_RELEASE}">
      <h2 id="platforms-heading">Connect your advertising platforms</h2>
      <p>Connect Meta, Google Ads, TikTok, or Klaviyo to this Shopify workspace.</p>
      <a id="platforms" class="primary-action" href="/shopify/app/platforms?release=${EMBEDDED_HOME_RELEASE}">Open Data Sources / Platforms</a>
      <p class="release">AdsTable release ${EMBEDDED_HOME_RELEASE}</p>
    </section>
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
        status.textContent = "Development store connected securely.";
        document.documentElement.dataset.smoke = "pass";
      };
      run().catch((error) => {
        const code = /^[A-Z0-9_]{1,64}$/.test(error.message || "") ? error.message : "EMBEDDED_BOOTSTRAP_FAILED";
        const reference = /^[A-Za-z0-9._:-]{1,128}$/.test(error.requestId || "") ? " Reference: " + error.requestId : "";
        status.textContent = "Connection could not be verified (" + code + ")." + reference;
        document.documentElement.dataset.smoke = "fail";
      });
    })();
  </script>
</body>
</html>`;
}

function renderEmbeddedPlatforms({clientId, providerOAuthEnabled}) {
  if (typeof clientId !== "string" || !clientId.trim()) throw new TypeError("clientId is required");
  const providers = [
    ["meta", "Meta"], ["google_ads", "Google Ads"], ["klaviyo", "Klaviyo"],
    ["tiktok", "TikTok"], ["pinterest", "Pinterest"],
  ];
  const cards = providers.map(([id, label]) => `<li><span>${label}</span><button type="button" data-provider="${id}"${providerOAuthEnabled ? "" : " disabled"}>Connect</button></li>`).join("");
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="shopify-api-key" content="${escapeAttribute(clientId.trim())}"><script src="https://cdn.shopify.com/shopifycloud/app-bridge.js"></script><title>Platforms — AdsTable</title><style>body{font:16px system-ui;margin:0;background:#f6f6f7;color:#202223}main{max-width:760px;margin:auto;padding:32px 20px}ul{list-style:none;padding:0;display:grid;gap:12px}li{display:flex;justify-content:space-between;align-items:center;background:#fff;border:1px solid #ddd;border-radius:12px;padding:18px}button{background:#008060;color:#fff;border:0;border-radius:8px;padding:10px 18px;font-weight:600}button:disabled{background:#aaa}</style></head><body><main><h1>Platforms</h1><p>Connect a provider to AdsTable for this Shopify workspace.</p><p id="status" role="status" aria-live="polite"></p><ul>${cards}</ul></main><script>(()=>{"use strict";const status=document.getElementById("status");document.querySelectorAll("button[data-provider]").forEach(button=>button.addEventListener("click",async()=>{button.disabled=true;status.textContent="Opening secure connection…";try{if(!window.shopify||typeof window.shopify.idToken!=="function")throw new Error();const token=await window.shopify.idToken();const response=await fetch("/api/shopify/providers/"+encodeURIComponent(button.dataset.provider)+"/oauth/start",{method:"POST",headers:{Authorization:"Bearer "+token}});const body=await response.json().catch(()=>({}));if(!response.ok||body.navigation!=="top_level"||typeof body.authorization_url!=="string")throw new Error();window.open(body.authorization_url,"_top");}catch{status.textContent="Connection could not be started. Please try again.";button.disabled=false;}}));const params=new URLSearchParams(location.search);if(params.has("oauth_connected"))status.textContent="Provider authorized. Account selection is next.";else if(params.has("oauth_error"))status.textContent="Connection was not completed. Please try again.";})();</script></body></html>`;
}

function registerEmbeddedAppHome(app, {clientId}) {
  if (!app || typeof app.get !== "function") throw new TypeError("app.get is required");
  const html = renderEmbeddedAppHome({clientId});
  const handler = (_req, res) => {
    res.set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0");
    res.set("CDN-Cache-Control", "no-store");
    res.set("Vercel-CDN-Cache-Control", "no-store");
    res.set("Surrogate-Control", "no-store");
    res.set("X-AdsTable-Release", EMBEDDED_HOME_RELEASE);
    res.set("Content-Security-Policy", "frame-ancestors https://admin.shopify.com https://*.myshopify.com");
    return res.type("html").send(html);
  };
  app.get("/", handler);
  app.get("/shopify/app", handler);
}


function registerEmbeddedPlatforms(app, {clientId, providerOAuthEnabled = false}) {
  if (!app || typeof app.get !== "function") throw new TypeError("app.get is required");
  const html = renderEmbeddedPlatforms({clientId, providerOAuthEnabled});
  app.get("/shopify/app/platforms", (_req, res) => {
    res.set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0");
    res.set("CDN-Cache-Control", "no-store");
    res.set("Vercel-CDN-Cache-Control", "no-store");
    res.set("Surrogate-Control", "no-store");
    res.set("X-AdsTable-Release", EMBEDDED_HOME_RELEASE);
    res.set("Content-Security-Policy", "frame-ancestors https://admin.shopify.com https://*.myshopify.com");
    return res.type("html").send(html);
  });
}

module.exports = Object.freeze({EMBEDDED_HOME_RELEASE, registerEmbeddedAppHome, renderEmbeddedAppHome, registerEmbeddedPlatforms, renderEmbeddedPlatforms});
