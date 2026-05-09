
const express = require("express");
const session = require("express-session");
const path = require("path");
const { google } = require("googleapis");
const { createClient } = require("@supabase/supabase-js");

const app = express();
const PORT = process.env.PORT || 3000;

app.set("trust proxy", 1);
app.use(express.json());
app.use(session({
  secret: process.env.SESSION_SECRET || "dev_secret_change_me",
  resave: false,
  saveUninitialized: false,
  cookie: { httpOnly: true, sameSite: "lax", secure: true }
}));

app.use(express.static(path.join(__dirname, "public")));
app.get("/", (req, res) => res.sendFile(path.join(__dirname, "public", "index.html")));
app.get("/privacy", (req, res) => res.sendFile(path.join(__dirname, "public", "privacy.html")));
app.get("/privacy.html", (req, res) => res.sendFile(path.join(__dirname, "public", "privacy.html")));

const META_GRAPH_VERSION = "v20.0";
const PINTEREST_API_BASE = "https://api.pinterest.com/v5";
const GOOGLE_ADS_API_VERSION = process.env.GOOGLE_ADS_API_VERSION || "v24";
const DEV_USER_ID = "00000000-0000-0000-0000-000000000001";

const supabase = (process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY)
  ? createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false }})
  : null;

async function ensureUser() {
  if (!supabase) return;
  await supabase.from("users").upsert({ id: DEV_USER_ID }, { onConflict: "id" });
}
async function saveConnection(platform, payload) {
  if (!supabase) return;
  await ensureUser();
  const row = {
    user_id: DEV_USER_ID,
    platform,
    access_token: payload.accessToken || null,
    refresh_token: payload.refreshToken || null,
    account_id: payload.accountId || null,
    account_name: payload.accountName || null,
    metadata: payload.metadata || {},
    connected: true,
    updated_at: new Date().toISOString()
  };
  const existing = await supabase.from("platform_connections").select("id").eq("user_id", DEV_USER_ID).eq("platform", platform).maybeSingle();
  if (existing.error) throw new Error(existing.error.message);
  if (existing.data?.id) {
    const { error } = await supabase.from("platform_connections").update(row).eq("id", existing.data.id);
    if (error) throw new Error(error.message);
  } else {
    const { error } = await supabase.from("platform_connections").insert(row);
    if (error) throw new Error(error.message);
  }
}
async function getConnection(platform) {
  if (!supabase) return null;
  const { data, error } = await supabase.from("platform_connections").select("*").eq("user_id", DEV_USER_ID).eq("platform", platform).eq("connected", true).maybeSingle();
  if (error) throw new Error(error.message);
  return data;
}
async function connectionStatus(platform, sessionConnected, connectedAt) {
  const db = await getConnection(platform).catch(() => null);
  return {
    connected: Boolean(sessionConnected || db?.access_token || db?.refresh_token),
    connectedAt: connectedAt || db?.updated_at || null,
    source: db ? "database" : (sessionConnected ? "session" : "none")
  };
}
async function tokenFor(req, platform) {
  if (platform === "meta" && req.session?.meta?.accessToken) return req.session.meta.accessToken;
  if (platform === "pinterest" && req.session?.pinterest?.accessToken) return req.session.pinterest.accessToken;
  const db = await getConnection(platform);
  if (!db?.access_token) throw new Error(`${platform} not connected`);
  return db.access_token;
}

/* META */
function requireMeta() {
  if (!process.env.META_APP_ID || !process.env.META_APP_SECRET || !process.env.META_REDIRECT_URI) throw new Error("Missing Meta env");
}
app.get("/auth/meta", (req, res) => {
  try {
    requireMeta();
    const state = Math.random().toString(36).slice(2);
    req.session.metaOAuthState = state;
    const p = new URLSearchParams({ client_id: process.env.META_APP_ID, redirect_uri: process.env.META_REDIRECT_URI, state, response_type: "code", scope: "ads_read" });
    res.redirect(`https://www.facebook.com/${META_GRAPH_VERSION}/dialog/oauth?${p.toString()}`);
  } catch(e) { res.status(500).send(e.message); }
});
app.get("/auth/meta/callback", async (req, res) => {
  try {
    requireMeta();
    const { code, state, error, error_description } = req.query;
    if (error) return res.redirect(`/?meta_error=${encodeURIComponent(error_description || error)}`);
    if (!code) return res.redirect("/?meta_error=Missing authorization code");
    if (!state || state !== req.session.metaOAuthState) return res.redirect("/?meta_error=Invalid OAuth state");
    const url = new URL(`https://graph.facebook.com/${META_GRAPH_VERSION}/oauth/access_token`);
    url.searchParams.set("client_id", process.env.META_APP_ID);
    url.searchParams.set("redirect_uri", process.env.META_REDIRECT_URI);
    url.searchParams.set("client_secret", process.env.META_APP_SECRET);
    url.searchParams.set("code", code);
    const r = await fetch(url.toString());
    const data = await r.json();
    if (!r.ok || !data.access_token) throw new Error(data.error?.message || "Meta token exchange failed");
    req.session.meta = { accessToken: data.access_token, connectedAt: new Date().toISOString() };
    await saveConnection("meta", { accessToken: data.access_token, metadata: { expiresIn: data.expires_in || null }});
    res.redirect("/?meta_connected=1");
  } catch(e) { res.redirect(`/?meta_error=${encodeURIComponent(e.message)}`); }
});
async function metaGet(pathname, params, token) {
  const url = new URL(`https://graph.facebook.com/${META_GRAPH_VERSION}${pathname}`);
  Object.entries(params || {}).forEach(([k,v]) => url.searchParams.set(k, v));
  url.searchParams.set("access_token", token);
  const r = await fetch(url.toString());
  const data = await r.json();
  if (!r.ok) throw new Error(data.error?.message || JSON.stringify(data));
  return data;
}
app.get("/api/meta/status", async (req, res) => res.json(await connectionStatus("meta", Boolean(req.session?.meta?.accessToken), req.session?.meta?.connectedAt)));
app.get("/api/meta/adaccounts", async (req, res) => {
  try {
    const token = await tokenFor(req, "meta");
    const data = await metaGet("/me/adaccounts", { fields: "id,name,account_status,currency,timezone_name", limit: "50" }, token);
    res.json({ accounts: data.data || [] });
  } catch(e) { res.status(500).json({ error: e.message }); }
});
function av(actions, type) {
  if (!Array.isArray(actions)) return 0;
  const f = actions.find(a => a.action_type === type);
  return f ? Number(f.value || 0) : 0;
}
app.get("/api/meta/insights", async (req, res) => {
  try {
    const token = await tokenFor(req, "meta");
    const adAccountId = req.query.adAccountId;
    if (!adAccountId) return res.status(400).json({ error: "Missing adAccountId" });
    const preset = req.query.range === "7d" ? "last_7d" : req.query.range === "month" ? "this_month" : (req.query.range || "today");
    const data = await metaGet(`/${adAccountId}/insights`, { level: "campaign", fields: "campaign_id,campaign_name,impressions,clicks,spend,actions,action_values", date_preset: preset, limit: "100" }, token);
    const campaigns = (data.data || []).map(row => {
      const spend = Number(row.spend || 0), clicks = Number(row.clicks || 0), impressions = Number(row.impressions || 0);
      const sales = av(row.action_values, "purchase") || av(row.action_values, "offsite_conversion.fb_pixel_purchase");
      return { platform: "Meta", id: row.campaign_id || "", name: row.campaign_name || "Unknown campaign", spend, sales, impressions, clicks, ctr: impressions ? clicks/impressions : 0, cpc: clicks ? spend/clicks : 0, roas: spend ? sales/spend : 0, acos: sales ? spend/sales : 0 };
    });
    res.json({ platform: "Meta", campaigns });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* GOOGLE */
function requireGoogle() {
  if (!process.env.GOOGLE_CLIENT_ID || !process.env.GOOGLE_CLIENT_SECRET || !process.env.GOOGLE_REDIRECT_URI || !process.env.GOOGLE_DEVELOPER_TOKEN) throw new Error("Missing Google env");
}
function googleClient() { return new google.auth.OAuth2(process.env.GOOGLE_CLIENT_ID, process.env.GOOGLE_CLIENT_SECRET, process.env.GOOGLE_REDIRECT_URI); }
app.get("/auth/google", (req, res) => {
  try {
    requireGoogle();
    const state = Math.random().toString(36).slice(2);
    req.session.googleOAuthState = state;
    const url = googleClient().generateAuthUrl({ access_type: "offline", prompt: "consent", state, scope: ["https://www.googleapis.com/auth/adwords"] });
    res.redirect(url);
  } catch(e) { res.status(500).send(e.message); }
});
app.get("/auth/google/callback", async (req, res) => {
  try {
    requireGoogle();
    const { code, state, error } = req.query;
    if (error) return res.redirect(`/?google_error=${encodeURIComponent(error)}`);
    if (!code) return res.redirect("/?google_error=Missing authorization code");
    if (!state || state !== req.session.googleOAuthState) return res.redirect("/?google_error=Invalid OAuth state");
    const client = googleClient();
    const { tokens } = await client.getToken(code);
    req.session.google = { tokens: { ...req.session?.google?.tokens, ...tokens }, connectedAt: new Date().toISOString() };
    await saveConnection("google", { accessToken: req.session.google.tokens.access_token, refreshToken: req.session.google.tokens.refresh_token || null, metadata: { scope: req.session.google.tokens.scope || null, expiryDate: req.session.google.tokens.expiry_date || null }});
    res.redirect("/?google_connected=1");
  } catch(e) { res.redirect(`/?google_error=${encodeURIComponent(e.message)}`); }
});
app.get("/api/google/status", async (req, res) => res.json(await connectionStatus("google", Boolean(req.session?.google?.tokens?.refresh_token || req.session?.google?.tokens?.access_token), req.session?.google?.connectedAt)));
async function getGoogleAccessToken(req) {
  let tokens = req.session?.google?.tokens;
  if (!tokens) {
    const db = await getConnection("google");
    if (!db?.refresh_token && !db?.access_token) throw new Error("Google not connected");
    tokens = { access_token: db.access_token, refresh_token: db.refresh_token };
  }
  const client = googleClient();
  client.setCredentials(tokens);
  const result = await client.getAccessToken();
  const token = result?.token || client.credentials.access_token || tokens.access_token;
  req.session.google = req.session.google || { tokens: {}, connectedAt: new Date().toISOString() };
  req.session.google.tokens = { ...tokens, ...client.credentials };
  await saveConnection("google", { accessToken: req.session.google.tokens.access_token, refreshToken: req.session.google.tokens.refresh_token || null, metadata: { expiryDate: req.session.google.tokens.expiry_date || null }});
  if (!token) throw new Error("Could not get Google access token");
  return token;
}
async function googleAdsFetch(req, url, options={}) {
  const headers = { Authorization: `Bearer ${await getGoogleAccessToken(req)}`, "developer-token": process.env.GOOGLE_DEVELOPER_TOKEN, "Content-Type": "application/json", ...(options.headers || {}) };
  if (process.env.GOOGLE_LOGIN_CUSTOMER_ID) headers["login-customer-id"] = String(process.env.GOOGLE_LOGIN_CUSTOMER_ID).replace(/-/g, "");
  const r = await fetch(url, { ...options, headers });
  const text = await r.text();
  let data; try { data = text ? JSON.parse(text) : {}; } catch { data = { raw: text }; }
  if (!r.ok) throw new Error(data.error?.message || text || `Google Ads API error ${r.status}`);
  return data;
}
app.get("/api/google/customers", async (req, res) => {
  try {
    requireGoogle();
    const data = await googleAdsFetch(req, `https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers:listAccessibleCustomers`, { method: "GET" });
    res.json({ customers: (data.resourceNames || []).map(r => ({ resourceName: r, customerId: r.replace("customers/","") })) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});
app.get("/api/google/insights", async (req, res) => {
  try {
    requireGoogle();
    const customerId = String(req.query.customerId || "").replace(/-/g, "");
    if (!customerId) return res.status(400).json({ error: "Missing customerId" });
    const query = `SELECT campaign.id, campaign.name, campaign.status, metrics.impressions, metrics.clicks, metrics.ctr, metrics.average_cpc, metrics.cost_micros, metrics.conversions, metrics.conversions_value FROM campaign WHERE segments.date DURING LAST_30_DAYS LIMIT 100`;
    const data = await googleAdsFetch(req, `https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers/${customerId}/googleAds:searchStream`, { method: "POST", body: JSON.stringify({ query }) });
    const rows = Array.isArray(data) ? data.flatMap(x => x.results || []) : (data.results || []);
    const campaigns = rows.map(row => {
      const m = row.metrics || {}, c = row.campaign || {};
      const spend = Number(m.costMicros || 0)/1000000, clicks = Number(m.clicks || 0), impressions = Number(m.impressions || 0), sales = Number(m.conversionsValue || 0);
      return { platform: "Google", id: c.id || "", name: c.name || "Unknown campaign", status: c.status || "", spend, sales, impressions, clicks, ctr: Number(m.ctr || 0), cpc: Number(m.averageCpc || 0)/1000000, roas: spend ? sales/spend : 0, acos: sales ? spend/sales : 0, conversions: Number(m.conversions || 0) };
    });
    res.json({ platform: "Google", campaigns });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* PINTEREST */
function requirePinterest() {
  if (!process.env.PINTEREST_CLIENT_ID || !process.env.PINTEREST_CLIENT_SECRET || !process.env.PINTEREST_REDIRECT_URI) throw new Error("Missing Pinterest env");
}
function pinBasic() { return Buffer.from(`${process.env.PINTEREST_CLIENT_ID}:${process.env.PINTEREST_CLIENT_SECRET}`).toString("base64"); }
app.get("/auth/pinterest", (req, res) => {
  try {
    requirePinterest();
    const state = Math.random().toString(36).slice(2);
    req.session.pinterestOAuthState = state;
    const p = new URLSearchParams({ response_type: "code", client_id: process.env.PINTEREST_CLIENT_ID, redirect_uri: process.env.PINTEREST_REDIRECT_URI, scope: "ads:read", state });
    res.redirect(`https://www.pinterest.com/oauth/?${p.toString()}`);
  } catch(e) { res.status(500).send(e.message); }
});
app.get("/auth/pinterest/callback", async (req, res) => {
  try {
    requirePinterest();
    const { code, state, error, error_description } = req.query;
    if (error) return res.redirect(`/?pinterest_error=${encodeURIComponent(error_description || error)}`);
    if (!code) return res.redirect("/?pinterest_error=Missing authorization code");
    if (!state || state !== req.session.pinterestOAuthState) return res.redirect("/?pinterest_error=Invalid OAuth state");
    const body = new URLSearchParams({ grant_type: "authorization_code", code, redirect_uri: process.env.PINTEREST_REDIRECT_URI });
    const r = await fetch(`${PINTEREST_API_BASE}/oauth/token`, { method: "POST", headers: { Authorization: `Basic ${pinBasic()}`, "Content-Type": "application/x-www-form-urlencoded" }, body: body.toString() });
    const data = await r.json();
    if (!r.ok || !data.access_token) throw new Error(data.message || data.error_description || data.error || "Pinterest token exchange failed");
    req.session.pinterest = { accessToken: data.access_token, refreshToken: data.refresh_token || null, connectedAt: new Date().toISOString() };
    await saveConnection("pinterest", { accessToken: data.access_token, refreshToken: data.refresh_token || null, metadata: { scope: data.scope || null, expiresIn: data.expires_in || null }});
    res.redirect("/?pinterest_connected=1");
  } catch(e) { res.redirect(`/?pinterest_error=${encodeURIComponent(e.message)}`); }
});
app.get("/api/pinterest/status", async (req, res) => res.json(await connectionStatus("pinterest", Boolean(req.session?.pinterest?.accessToken), req.session?.pinterest?.connectedAt)));
async function pinterestFetch(req, endpoint, options={}) {
  const token = await tokenFor(req, "pinterest");
  const r = await fetch(`${PINTEREST_API_BASE}${endpoint}`, { ...options, headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json", ...(options.headers || {}) }});
  const text = await r.text();
  let data; try { data = text ? JSON.parse(text) : {}; } catch { data = { raw: text }; }
  if (!r.ok) throw new Error(data.message || text || `Pinterest API error ${r.status}`);
  return data;
}
app.get("/api/pinterest/adaccounts", async (req, res) => {
  try { requirePinterest(); res.json(await pinterestFetch(req, "/ad_accounts")); }
  catch(e) { res.status(500).json({ error: e.message }); }
});
app.get("/api/pinterest/campaigns", async (req, res) => {
  try {
    requirePinterest();
    const adAccountId = req.query.adAccountId;
    if (!adAccountId) return res.status(400).json({ error: "Missing adAccountId" });
    res.json(await pinterestFetch(req, `/ad_accounts/${adAccountId}/campaigns`));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* TIKTOK SKELETON */
app.get("/api/tiktok/status", (req, res) => res.json({ connected: false, status: "pending_verification" }));
app.get("/auth/tiktok", (req, res) => res.status(501).json({ status: "pending", message: "TikTok verification pending" }));

/* DEBUG + UNIFIED */
app.get("/api/debug/connections", async (req, res) => {
  try {
    if (!supabase) return res.status(500).json({ error: "Supabase not configured" });
    const { data, error } = await supabase.from("platform_connections").select("platform,connected,account_id,account_name,metadata,updated_at").eq("user_id", DEV_USER_ID).order("updated_at", { ascending: false });
    if (error) throw error;
    res.json({ connections: data || [] });
  } catch(e) { res.status(500).json({ error: e.message }); }
});
app.get("/api/unified/status", async (req, res) => {
  const meta = await connectionStatus("meta", Boolean(req.session?.meta?.accessToken), req.session?.meta?.connectedAt);
  const google = await connectionStatus("google", Boolean(req.session?.google?.tokens?.refresh_token || req.session?.google?.tokens?.access_token), req.session?.google?.connectedAt);
  const pinterest = await connectionStatus("pinterest", Boolean(req.session?.pinterest?.accessToken), req.session?.pinterest?.connectedAt);
  res.json({ meta: meta.connected, google: google.connected, pinterest: pinterest.connected, tiktok: false, tiktokStatus: "pending_verification", sources: { meta: meta.source, google: google.source, pinterest: pinterest.source }});
});

if (process.env.VERCEL !== "1") app.listen(PORT, () => console.log(`AdsTable dev DB running on ${PORT}`));
module.exports = app;
