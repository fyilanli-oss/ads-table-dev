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
  cookie: {
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production" || process.env.VERCEL === "1"
  }
}));

app.use(express.static(path.join(__dirname, "public")));

const META_GRAPH_VERSION = process.env.META_GRAPH_VERSION || "v20.0";
const PINTEREST_API_BASE = "https://api.pinterest.com/v5";
const GOOGLE_ADS_API_VERSION = process.env.GOOGLE_ADS_API_VERSION || "v24";

const supabaseAdmin = (process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY)
  ? createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false }})
  : null;

function publicConfig() {
  return {
    supabaseUrl: process.env.SUPABASE_URL || "",
    supabaseAnonKey: process.env.SUPABASE_ANON_KEY || process.env.SUPABASE_PUBLISHABLE_KEY || ""
  };
}

app.get("/", (req, res) => res.sendFile(path.join(__dirname, "public", "landing.html")));
app.get("/login", (req, res) => res.sendFile(path.join(__dirname, "public", "login.html")));
app.get("/signup", (req, res) => res.sendFile(path.join(__dirname, "public", "signup.html")));
app.get("/dashboard", (req, res) => res.sendFile(path.join(__dirname, "public", "dashboard.html")));
app.get("/demo", (req, res) => res.sendFile(path.join(__dirname, "public", "dashboard-demo.html")));
app.get("/privacy", (req, res) => res.sendFile(path.join(__dirname, "public", "privacy.html")));
app.get("/terms", (req, res) => res.sendFile(path.join(__dirname, "public", "terms.html")));
app.get("/data-deletion", (req, res) => res.sendFile(path.join(__dirname, "public", "data-deletion.html")));
app.get("/api/public-config", (req, res) => res.json(publicConfig()));

async function getUserFromRequest(req) {
  const auth = req.headers.authorization || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : null;
  if (!token || !supabaseAdmin) return null;
  const { data, error } = await supabaseAdmin.auth.getUser(token);
  if (error || !data?.user?.id) return null;
  return data.user;
}

async function requireUser(req, res) {
  const user = await getUserFromRequest(req);
  if (!user) {
    res.status(401).json({ error: "Not authenticated" });
    return null;
  }
  return user;
}

function parseExpiry(seconds) {
  if (!seconds) return null;
  return new Date(Date.now() + Number(seconds) * 1000).toISOString();
}

async function saveConnection(userId, platform, payload) {
  if (!supabaseAdmin || !userId) throw new Error("Supabase not configured or user missing");
  const row = {
    user_id: userId,
    platform,
    access_token: payload.accessToken || null,
    refresh_token: payload.refreshToken || null,
    token_expires_at: payload.tokenExpiresAt || null,
    account_id: payload.accountId || null,
    account_name: payload.accountName || null,
    metadata: payload.metadata || {},
    connected: true,
    updated_at: new Date().toISOString()
  };
  const { error } = await supabaseAdmin
    .from("platform_connections")
    .upsert(row, { onConflict: "user_id,platform" });
  if (error) throw new Error(error.message);
}

async function getConnection(userId, platform) {
  if (!supabaseAdmin || !userId) return null;
  const { data, error } = await supabaseAdmin
    .from("platform_connections")
    .select("*")
    .eq("user_id", userId)
    .eq("platform", platform)
    .eq("connected", true)
    .maybeSingle();
  if (error) throw new Error(error.message);
  return data;
}

async function connectionStatus(userId, platform) {
  const row = await getConnection(userId, platform).catch(() => null);
  return {
    connected: Boolean(row && (row.access_token || row.refresh_token)),
    source: row ? "database" : "none",
    updatedAt: row?.updated_at || null
  };
}

async function requireConnection(req, res, platform) {
  const user = await requireUser(req, res);
  if (!user) return null;
  const conn = await getConnection(user.id, platform);
  if (!conn) {
    res.status(404).json({ error: `${platform} not connected` });
    return null;
  }
  return { user, conn };
}

function requireGoogleEnv() {
  if (!process.env.GOOGLE_CLIENT_ID || !process.env.GOOGLE_CLIENT_SECRET || !process.env.GOOGLE_REDIRECT_URI) {
    throw new Error("Missing Google OAuth env");
  }
}

function googleOAuthClient() {
  requireGoogleEnv();
  return new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
    process.env.GOOGLE_REDIRECT_URI
  );
}

async function getFreshGoogleAccessToken(userId) {
  const conn = await getConnection(userId, "google");
  if (!conn) throw new Error("Google not connected");
  const expiresAt = conn.token_expires_at ? new Date(conn.token_expires_at).getTime() : 0;
  const accessStillValid = conn.access_token && expiresAt && expiresAt > Date.now() + 120000;
  if (accessStillValid) return conn.access_token;
  if (!conn.refresh_token) {
    if (conn.access_token) return conn.access_token;
    throw new Error("Google refresh token missing. Please reconnect Google.");
  }
  const client = googleOAuthClient();
  client.setCredentials({ refresh_token: conn.refresh_token });
  const { credentials } = await client.refreshAccessToken();
  const newAccessToken = credentials.access_token;
  const expiryDate = credentials.expiry_date || (Date.now() + 3600 * 1000);
  await saveConnection(userId, "google", {
    accessToken: newAccessToken,
    refreshToken: conn.refresh_token,
    tokenExpiresAt: new Date(expiryDate).toISOString(),
    metadata: { ...(conn.metadata || {}), refreshedAt: new Date().toISOString(), expiryDate }
  });
  return newAccessToken;
}

app.get("/auth/meta", async (req, res) => {
  try {
    const userId = req.query.user_id;
    if (!userId) return res.redirect("/dashboard?error=missing_user_id");
    if (!process.env.META_APP_ID || !process.env.META_REDIRECT_URI) throw new Error("Missing Meta env");
    const state = Math.random().toString(36).slice(2);
    req.session.metaOAuthState = state;
    req.session.oauthUserId = userId;
    const p = new URLSearchParams({
      client_id: process.env.META_APP_ID,
      redirect_uri: process.env.META_REDIRECT_URI,
      state,
      response_type: "code",
      scope: "ads_read"
    });
    res.redirect(`https://www.facebook.com/${META_GRAPH_VERSION}/dialog/oauth?${p.toString()}`);
  } catch (e) {
    res.status(500).send(e.message);
  }
});

app.get("/auth/meta/callback", async (req, res) => {
  try {
    const { code, state, error, error_description } = req.query;
    if (error) return res.redirect(`/dashboard?meta_error=${encodeURIComponent(error_description || error)}`);
    if (!code) return res.redirect("/dashboard?meta_error=missing_code");
    if (!state || state !== req.session.metaOAuthState) return res.redirect("/dashboard?meta_error=invalid_state");
    const userId = req.session.oauthUserId;
    if (!userId) return res.redirect("/dashboard?meta_error=missing_user_id");
    const url = new URL(`https://graph.facebook.com/${META_GRAPH_VERSION}/oauth/access_token`);
    url.searchParams.set("client_id", process.env.META_APP_ID);
    url.searchParams.set("redirect_uri", process.env.META_REDIRECT_URI);
    url.searchParams.set("client_secret", process.env.META_APP_SECRET);
    url.searchParams.set("code", code);
    const r = await fetch(url.toString());
    const data = await r.json();
    if (!r.ok || !data.access_token) throw new Error(data.error?.message || "Meta token exchange failed");
    await saveConnection(userId, "meta", {
      accessToken: data.access_token,
      tokenExpiresAt: parseExpiry(data.expires_in),
      metadata: { expiresIn: data.expires_in || null }
    });
    req.session.metaOAuthState = null;
    res.redirect("/dashboard?meta_connected=1");
  } catch (e) {
    res.redirect(`/dashboard?meta_error=${encodeURIComponent(e.message)}`);
  }
});

app.get("/auth/google", async (req, res) => {
  try {
    const userId = req.query.user_id;
    if (!userId) return res.redirect("/dashboard?error=missing_user_id");
    const state = Math.random().toString(36).slice(2);
    req.session.googleOAuthState = state;
    req.session.oauthUserId = userId;
    const url = googleOAuthClient().generateAuthUrl({
      access_type: "offline",
      prompt: "consent",
      state,
      scope: ["https://www.googleapis.com/auth/adwords"]
    });
    res.redirect(url);
  } catch (e) {
    res.status(500).send(e.message);
  }
});

app.get("/auth/google/callback", async (req, res) => {
  try {
    const { code, state, error } = req.query;
    if (error) return res.redirect(`/dashboard?google_error=${encodeURIComponent(error)}`);
    if (!code) return res.redirect("/dashboard?google_error=missing_code");
    if (!state || state !== req.session.googleOAuthState) return res.redirect("/dashboard?google_error=invalid_state");
    const userId = req.session.oauthUserId;
    if (!userId) return res.redirect("/dashboard?google_error=missing_user_id");
    const client = googleOAuthClient();
    const { tokens } = await client.getToken(code);
    await saveConnection(userId, "google", {
      accessToken: tokens.access_token,
      refreshToken: tokens.refresh_token || null,
      tokenExpiresAt: tokens.expiry_date ? new Date(tokens.expiry_date).toISOString() : null,
      metadata: { scope: tokens.scope || null, expiryDate: tokens.expiry_date || null, tokenType: tokens.token_type || null }
    });
    req.session.googleOAuthState = null;
    res.redirect("/dashboard?google_connected=1");
  } catch (e) {
    res.redirect(`/dashboard?google_error=${encodeURIComponent(e.message)}`);
  }
});

function pinterestBasic() {
  return Buffer.from(`${process.env.PINTEREST_CLIENT_ID}:${process.env.PINTEREST_CLIENT_SECRET}`).toString("base64");
}

app.get("/auth/pinterest", (req, res) => {
  try {
    const userId = req.query.user_id;
    if (!userId) return res.redirect("/dashboard?error=missing_user_id");
    if (!process.env.PINTEREST_CLIENT_ID || !process.env.PINTEREST_REDIRECT_URI) throw new Error("Missing Pinterest env");
    const state = Math.random().toString(36).slice(2);
    req.session.pinterestOAuthState = state;
    req.session.oauthUserId = userId;
    const p = new URLSearchParams({
      response_type: "code",
      client_id: process.env.PINTEREST_CLIENT_ID,
      redirect_uri: process.env.PINTEREST_REDIRECT_URI,
      scope: "ads:read",
      state
    });
    res.redirect(`https://www.pinterest.com/oauth/?${p.toString()}`);
  } catch (e) {
    res.status(500).send(e.message);
  }
});

app.get("/auth/pinterest/callback", async (req, res) => {
  try {
    const { code, state, error, error_description } = req.query;
    if (error) return res.redirect(`/dashboard?pinterest_error=${encodeURIComponent(error_description || error)}`);
    if (!code) return res.redirect("/dashboard?pinterest_error=missing_code");
    if (!state || state !== req.session.pinterestOAuthState) return res.redirect("/dashboard?pinterest_error=invalid_state");
    const userId = req.session.oauthUserId;
    if (!userId) return res.redirect("/dashboard?pinterest_error=missing_user_id");
    const body = new URLSearchParams({
      grant_type: "authorization_code",
      code,
      redirect_uri: process.env.PINTEREST_REDIRECT_URI
    });
    const r = await fetch(`${PINTEREST_API_BASE}/oauth/token`, {
      method: "POST",
      headers: { Authorization: `Basic ${pinterestBasic()}`, "Content-Type": "application/x-www-form-urlencoded" },
      body: body.toString()
    });
    const data = await r.json();
    if (!r.ok || !data.access_token) throw new Error(data.message || data.error_description || data.error || "Pinterest token exchange failed");
    await saveConnection(userId, "pinterest", {
      accessToken: data.access_token,
      refreshToken: data.refresh_token || null,
      tokenExpiresAt: parseExpiry(data.expires_in),
      metadata: { scope: data.scope || null, expiresIn: data.expires_in || null }
    });
    req.session.pinterestOAuthState = null;
    res.redirect("/dashboard?pinterest_connected=1");
  } catch (e) {
    res.redirect(`/dashboard?pinterest_error=${encodeURIComponent(e.message)}`);
  }
});

app.get("/api/unified/status", async (req, res) => {
  const user = await requireUser(req, res);
  if (!user) return;
  const meta = await connectionStatus(user.id, "meta");
  const google = await connectionStatus(user.id, "google");
  const pinterest = await connectionStatus(user.id, "pinterest");
  res.json({
    meta: meta.connected,
    google: google.connected,
    pinterest: pinterest.connected,
    tiktok: false,
    tiktokStatus: "pending_verification",
    sources: { meta: meta.source, google: google.source, pinterest: pinterest.source },
    updatedAt: { meta: meta.updatedAt, google: google.updatedAt, pinterest: pinterest.updatedAt }
  });
});

app.get("/api/debug/connections", async (req, res) => {
  try {
    const user = await requireUser(req, res);
    if (!user) return;
    const { data, error } = await supabaseAdmin
      .from("platform_connections")
      .select("platform,connected,account_id,account_name,token_expires_at,metadata,updated_at")
      .eq("user_id", user.id)
      .order("updated_at", { ascending: false });
    if (error) throw error;
    res.json({ connections: data || [] });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post("/api/connections/:platform/disconnect", async (req, res) => {
  try {
    const user = await requireUser(req, res);
    if (!user) return;
    const platform = req.params.platform;
    const allowed = ["meta", "google", "pinterest"];
    if (!allowed.includes(platform)) return res.status(400).json({ error: "Unsupported platform" });
    const { error } = await supabaseAdmin
      .from("platform_connections")
      .update({ connected: false, updated_at: new Date().toISOString() })
      .eq("user_id", user.id)
      .eq("platform", platform);
    if (error) throw error;
    res.json({ ok: true, platform, connected: false });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

async function upsertAdAccount(userId, platform, account) {
  if (!supabaseAdmin || !userId) return;
  const row = {
    user_id: userId,
    platform,
    platform_business_id: account.business_id || null,
    platform_account_id: account.id || account.customerId || account.account_id,
    account_name: account.name || account.descriptiveName || account.account_name || null,
    currency: account.currency || null,
    timezone: account.timezone_name || account.timezone || null,
    status: String(account.account_status || account.status || ""),
    metadata: account,
    updated_at: new Date().toISOString()
  };
  if (!row.platform_account_id) return;
  await supabaseAdmin.from("platform_ad_accounts").upsert(row, { onConflict: "user_id,platform,platform_account_id" });
}

async function metaGraph(pathname, params, token) {
  const url = new URL(`https://graph.facebook.com/${META_GRAPH_VERSION}${pathname}`);
  Object.entries(params || {}).forEach(([k, v]) => url.searchParams.set(k, v));
  url.searchParams.set("access_token", token);
  const r = await fetch(url.toString());
  const data = await r.json();
  if (!r.ok) throw new Error(data.error?.message || JSON.stringify(data));
  return data;
}

app.get("/api/meta/adaccounts", async (req, res) => {
  try {
    const result = await requireConnection(req, res, "meta");
    if (!result) return;
    const { user, conn } = result;
    const data = await metaGraph("/me/adaccounts", {
      fields: "id,name,account_status,currency,timezone_name",
      limit: "100"
    }, conn.access_token);
    const accounts = data.data || [];
    for (const account of accounts) await upsertAdAccount(user.id, "meta", account);
    res.json({ platform: "meta", accounts });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});



function metaActionValue(list, actionType) {
  const found = Array.isArray(list) ? list.find(x => x.action_type === actionType) : null;
  return found ? Number(found.value || 0) : null;
}

function normalizeMetaInsight(row, level) {
  const actions = row.actions || [];
  const costs = row.cost_per_action_type || [];
  return {
    platform: "Meta",
    level,
    campaign_id: row.campaign_id || null,
    campaign_name: row.campaign_name || null,
    adset_id: row.adset_id || null,
    adset_name: row.adset_name || null,
    ad_id: row.ad_id || null,
    ad_name: row.ad_name || null,
    currency: row.account_currency || null,
    impressions: Number(row.impressions || 0),
    reach: Number(row.reach || 0),
    clicks: Number(row.clicks || 0),
    ctr: row.ctr !== undefined ? Number(row.ctr) : null,
    cpc: row.cpc !== undefined ? Number(row.cpc) : null,
    spend: Number(row.spend || 0),
    link_clicks: metaActionValue(actions, "link_click"),
    landing_page_views: metaActionValue(actions, "landing_page_view"),
    omni_landing_page_views: metaActionValue(actions, "omni_landing_page_view"),
    page_engagement: metaActionValue(actions, "page_engagement"),
    post_engagement: metaActionValue(actions, "post_engagement"),
    video_views: metaActionValue(actions, "video_view"),
    purchases: metaActionValue(actions, "purchase"),
    cost_per_link_click: metaActionValue(costs, "link_click"),
    cost_per_landing_page_view: metaActionValue(costs, "landing_page_view"),
    cost_per_page_engagement: metaActionValue(costs, "page_engagement"),
    cost_per_video_view: metaActionValue(costs, "video_view"),
    conversion_rate_ranking: row.conversion_rate_ranking || null,
    sales: null,
    revenue: null,
    roas: null,
    date_start: row.date_start || null,
    date_stop: row.date_stop || null,
    raw: row
  };
}

app.get("/api/meta/insights", async (req, res) => {
  try {
    const result = await requireConnection(req, res, "meta");
    if (!result) return;
    const { conn } = result;
    const adAccountId = req.query.adAccountId || req.query.ad_account_id;
    if (!adAccountId) return res.status(400).json({ error: "Missing adAccountId" });
    const level = ["campaign", "adset", "ad"].includes(String(req.query.level || "campaign")) ? String(req.query.level || "campaign") : "campaign";
    const fields = ["campaign_id", "campaign_name", "account_currency", "impressions", "reach", "clicks", "ctr", "cpc", "spend", "actions", "cost_per_action_type", "conversion_rate_ranking"];
    if (level === "adset") fields.splice(2, 0, "adset_id", "adset_name");
    if (level === "ad") fields.splice(2, 0, "adset_id", "adset_name", "ad_id", "ad_name");
    const data = await metaGraph(`/${adAccountId}/insights`, {
      level,
      date_preset: req.query.date_preset || "last_7d",
      fields: fields.join(","),
      limit: req.query.limit || "100"
    }, conn.access_token);
    const rows = (data.data || []).map(row => normalizeMetaInsight(row, level));
    res.json({ platform: "Meta", level, date_preset: req.query.date_preset || "last_7d", rows, paging: data.paging || null });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get("/api/google/customers", async (req, res) => {
  try {
    const user = await requireUser(req, res);
    if (!user) return;
    const accessToken = await getFreshGoogleAccessToken(user.id);
    const r = await fetch(`https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers:listAccessibleCustomers`, {
      method: "GET",
      headers: { Authorization: `Bearer ${accessToken}`, "developer-token": process.env.GOOGLE_DEVELOPER_TOKEN || "" }
    });
    const data = await r.json();
    if (!r.ok) return res.status(r.status).json({ error: JSON.stringify(data), status: r.status });
    const customers = (data.resourceNames || []).map(resourceName => {
      const customerId = String(resourceName).replace("customers/", "");
      return { resourceName, customerId };
    });
    for (const c of customers) await upsertAdAccount(user.id, "google", { id: c.customerId, customerId: c.customerId, name: c.customerId, status: "accessible" });
    res.json({ customers });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

async function pinterestFetch(conn, endpoint, options = {}) {
  const r = await fetch(`${PINTEREST_API_BASE}${endpoint}`, {
    ...options,
    headers: { Authorization: `Bearer ${conn.access_token}`, "Content-Type": "application/json", ...(options.headers || {}) }
  });
  const text = await r.text();
  let data;
  try { data = text ? JSON.parse(text) : {}; } catch { data = { raw: text }; }
  if (!r.ok) throw new Error(data.message || text || `Pinterest API error ${r.status}`);
  return data;
}

app.get("/api/pinterest/adaccounts", async (req, res) => {
  try {
    const result = await requireConnection(req, res, "pinterest");
    if (!result) return;
    const { user, conn } = result;
    const data = await pinterestFetch(conn, "/ad_accounts");
    const accounts = data.items || [];
    for (const account of accounts) await upsertAdAccount(user.id, "pinterest", { id: account.id, name: account.name, currency: account.currency, status: "accessible", ...account });
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get("/api/accounts", async (req, res) => {
  try {
    const user = await requireUser(req, res);
    if (!user) return;
    const { data, error } = await supabaseAdmin
      .from("platform_ad_accounts")
      .select("*")
      .eq("user_id", user.id)
      .order("platform", { ascending: true });
    if (error) throw error;
    res.json({ accounts: data || [] });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get("/api/tiktok/status", (req, res) => {
  res.json({ connected: false, status: "pending_verification" });
});

if (process.env.VERCEL !== "1") {
  app.listen(PORT, () => console.log(`AdsTable server running on ${PORT}`));
}

module.exports = app;
