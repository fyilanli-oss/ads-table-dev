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
  cookie: { httpOnly: true, sameSite: "lax", secure: process.env.VERCEL === "1" || process.env.NODE_ENV === "production" }
}));
app.use(express.static(path.join(__dirname, "public")));

const META_GRAPH_VERSION = process.env.META_GRAPH_VERSION || "v20.0";
const GOOGLE_ADS_API_VERSION = process.env.GOOGLE_ADS_API_VERSION || "v24";
const PINTEREST_API_BASE = "https://api.pinterest.com/v5";

const supabaseAdmin = (process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY)
  ? createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } })
  : null;

app.get("/", (req, res) => res.sendFile(path.join(__dirname, "public", "landing.html")));
app.get("/login", (req, res) => res.sendFile(path.join(__dirname, "public", "login.html")));
app.get("/signup", (req, res) => res.sendFile(path.join(__dirname, "public", "signup.html")));
app.get("/dashboard", (req, res) => res.sendFile(path.join(__dirname, "public", "dashboard.html")));
app.get("/demo", (req, res) => res.sendFile(path.join(__dirname, "public", "dashboard-demo.html")));
app.get("/privacy", (req, res) => res.sendFile(path.join(__dirname, "public", "privacy.html")));
app.get("/terms", (req, res) => res.sendFile(path.join(__dirname, "public", "terms.html")));
app.get("/data-deletion", (req, res) => res.sendFile(path.join(__dirname, "public", "data-deletion.html")));
app.get("/api/public-config", (req, res) => res.json({
  supabaseUrl: process.env.SUPABASE_URL || "",
  supabaseAnonKey: process.env.SUPABASE_ANON_KEY || process.env.SUPABASE_PUBLISHABLE_KEY || ""
}));

async function getUser(req) {
  const auth = req.headers.authorization || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : null;
  if (!token || !supabaseAdmin) return null;
  const { data, error } = await supabaseAdmin.auth.getUser(token);
  if (error || !data?.user) return null;
  return data.user;
}
async function requireUser(req, res) {
  const user = await getUser(req);
  if (!user) {
    res.status(401).json({ error: "Not authenticated" });
    return null;
  }
  return user;
}
function expiresIn(seconds) {
  if (!seconds) return null;
  return new Date(Date.now() + Number(seconds) * 1000).toISOString();
}
async function saveConnection(userId, platform, p) {
  const row = {
    user_id: userId,
    platform,
    access_token: p.accessToken || null,
    refresh_token: p.refreshToken || null,
    token_expires_at: p.tokenExpiresAt || null,
    account_id: p.accountId || null,
    account_name: p.accountName || null,
    metadata: p.metadata || {},
    connected: true,
    updated_at: new Date().toISOString()
  };
  const { error } = await supabaseAdmin.from("platform_connections").upsert(row, { onConflict: "user_id,platform" });
  if (error) throw new Error(error.message);
}
async function getConnection(userId, platform) {
  const { data, error } = await supabaseAdmin.from("platform_connections")
    .select("*").eq("user_id", userId).eq("platform", platform).eq("connected", true).maybeSingle();
  if (error) throw new Error(error.message);
  return data;
}
async function connectionStatus(userId, platform) {
  const row = await getConnection(userId, platform).catch(() => null);
  return { connected: Boolean(row && (row.access_token || row.refresh_token)), source: row ? "database" : "none", updatedAt: row?.updated_at || null };
}
async function disconnect(userId, platform) {
  const { error } = await supabaseAdmin.from("platform_connections").update({ connected:false, updated_at:new Date().toISOString() }).eq("user_id", userId).eq("platform", platform);
  if (error) throw new Error(error.message);
}
function googleClient() {
  return new google.auth.OAuth2(process.env.GOOGLE_CLIENT_ID, process.env.GOOGLE_CLIENT_SECRET, process.env.GOOGLE_REDIRECT_URI);
}
async function getFreshGoogleAccessToken(userId) {
  const conn = await getConnection(userId, "google");
  if (!conn) throw new Error("Google not connected");
  const exp = conn.token_expires_at ? new Date(conn.token_expires_at).getTime() : 0;
  if (conn.access_token && exp && exp > Date.now() + 120000) return conn.access_token;
  if (!conn.refresh_token) {
    if (conn.access_token) return conn.access_token;
    throw new Error("Google refresh token missing. Reconnect Google.");
  }
  const client = googleClient();
  client.setCredentials({ refresh_token: conn.refresh_token });
  const { credentials } = await client.refreshAccessToken();
  const expiryDate = credentials.expiry_date || (Date.now() + 3600*1000);
  await saveConnection(userId, "google", {
    accessToken: credentials.access_token,
    refreshToken: conn.refresh_token,
    tokenExpiresAt: new Date(expiryDate).toISOString(),
    metadata: { ...(conn.metadata || {}), refreshedAt: new Date().toISOString(), expiryDate }
  });
  return credentials.access_token;
}

app.get("/auth/meta", (req, res) => {
  const userId = req.query.user_id;
  if (!userId) return res.redirect("/dashboard?error=missing_user_id");
  const state = Math.random().toString(36).slice(2);
  req.session.metaOAuthState = state;
  req.session.oauthUserId = userId;
  const p = new URLSearchParams({ client_id: process.env.META_APP_ID, redirect_uri: process.env.META_REDIRECT_URI, state, response_type:"code", scope:"ads_read" });
  res.redirect(`https://www.facebook.com/${META_GRAPH_VERSION}/dialog/oauth?${p.toString()}`);
});
app.get("/auth/meta/callback", async (req, res) => {
  try {
    const { code, state } = req.query;
    if (!code || state !== req.session.metaOAuthState) throw new Error("Invalid Meta OAuth callback");
    const userId = req.session.oauthUserId;
    const u = new URL(`https://graph.facebook.com/${META_GRAPH_VERSION}/oauth/access_token`);
    u.searchParams.set("client_id", process.env.META_APP_ID);
    u.searchParams.set("redirect_uri", process.env.META_REDIRECT_URI);
    u.searchParams.set("client_secret", process.env.META_APP_SECRET);
    u.searchParams.set("code", code);
    const r = await fetch(u.toString());
    const data = await r.json();
    if (!r.ok || !data.access_token) throw new Error(data.error?.message || "Meta token failed");
    await saveConnection(userId, "meta", { accessToken:data.access_token, tokenExpiresAt:expiresIn(data.expires_in), metadata:{ expiresIn:data.expires_in || null }});
    res.redirect("/dashboard?meta_connected=1");
  } catch(e) { res.redirect(`/dashboard?meta_error=${encodeURIComponent(e.message)}`); }
});
app.get("/auth/google", (req, res) => {
  const userId = req.query.user_id;
  if (!userId) return res.redirect("/dashboard?error=missing_user_id");
  const state = Math.random().toString(36).slice(2);
  req.session.googleOAuthState = state;
  req.session.oauthUserId = userId;
  const url = googleClient().generateAuthUrl({ access_type:"offline", prompt:"consent", state, scope:["https://www.googleapis.com/auth/adwords"] });
  res.redirect(url);
});
app.get("/auth/google/callback", async (req, res) => {
  try {
    const { code, state } = req.query;
    if (!code || state !== req.session.googleOAuthState) throw new Error("Invalid Google OAuth callback");
    const userId = req.session.oauthUserId;
    const { tokens } = await googleClient().getToken(code);
    await saveConnection(userId, "google", {
      accessToken: tokens.access_token,
      refreshToken: tokens.refresh_token || null,
      tokenExpiresAt: tokens.expiry_date ? new Date(tokens.expiry_date).toISOString() : null,
      metadata: { scope:tokens.scope || null, expiryDate:tokens.expiry_date || null }
    });
    res.redirect("/dashboard?google_connected=1");
  } catch(e) { res.redirect(`/dashboard?google_error=${encodeURIComponent(e.message)}`); }
});
function pinterestBasic() {
  return Buffer.from(`${process.env.PINTEREST_CLIENT_ID}:${process.env.PINTEREST_CLIENT_SECRET}`).toString("base64");
}
app.get("/auth/pinterest", (req, res) => {
  const userId = req.query.user_id;
  if (!userId) return res.redirect("/dashboard?error=missing_user_id");
  const state = Math.random().toString(36).slice(2);
  req.session.pinterestOAuthState = state;
  req.session.oauthUserId = userId;
  const p = new URLSearchParams({ response_type:"code", client_id:process.env.PINTEREST_CLIENT_ID, redirect_uri:process.env.PINTEREST_REDIRECT_URI, scope:"ads:read", state });
  res.redirect(`https://www.pinterest.com/oauth/?${p.toString()}`);
});
app.get("/auth/pinterest/callback", async (req, res) => {
  try {
    const { code, state } = req.query;
    if (!code || state !== req.session.pinterestOAuthState) throw new Error("Invalid Pinterest OAuth callback");
    const userId = req.session.oauthUserId;
    const body = new URLSearchParams({ grant_type:"authorization_code", code, redirect_uri:process.env.PINTEREST_REDIRECT_URI });
    const r = await fetch(`${PINTEREST_API_BASE}/oauth/token`, { method:"POST", headers:{ Authorization:`Basic ${pinterestBasic()}`, "Content-Type":"application/x-www-form-urlencoded" }, body:body.toString() });
    const data = await r.json();
    if (!r.ok || !data.access_token) throw new Error(data.message || data.error || "Pinterest token failed");
    await saveConnection(userId, "pinterest", { accessToken:data.access_token, refreshToken:data.refresh_token || null, tokenExpiresAt:expiresIn(data.expires_in), metadata:{ scope:data.scope || null, expiresIn:data.expires_in || null }});
    res.redirect("/dashboard?pinterest_connected=1");
  } catch(e) { res.redirect(`/dashboard?pinterest_error=${encodeURIComponent(e.message)}`); }
});
app.get("/api/unified/status", async (req, res) => {
  const user = await requireUser(req, res); if (!user) return;
  const meta = await connectionStatus(user.id, "meta");
  const google = await connectionStatus(user.id, "google");
  const pinterest = await connectionStatus(user.id, "pinterest");
  res.json({ meta:meta.connected, google:google.connected, pinterest:pinterest.connected, tiktok:false, tiktokStatus:"pending_verification", sources:{ meta:meta.source, google:google.source, pinterest:pinterest.source }, updatedAt:{ meta:meta.updatedAt, google:google.updatedAt, pinterest:pinterest.updatedAt }});
});
app.get("/api/debug/connections", async (req, res) => {
  const user = await requireUser(req, res); if (!user) return;
  const { data, error } = await supabaseAdmin.from("platform_connections").select("platform,connected,account_id,account_name,token_expires_at,metadata,updated_at").eq("user_id", user.id).order("updated_at", { ascending:false });
  if (error) return res.status(500).json({ error:error.message });
  res.json({ connections:data || [] });
});
app.post("/api/connections/:platform/disconnect", async (req, res) => {
  const user = await requireUser(req, res); if (!user) return;
  await disconnect(user.id, req.params.platform);
  res.json({ ok:true });
});
async function upsertAdAccount(userId, platform, account) {
  const row = { user_id:userId, platform, platform_business_id:account.business_id || null, platform_account_id:String(account.id || account.customerId || account.account_id), account_name:account.name || account.customerId || null, currency:account.currency || null, timezone:account.timezone_name || account.timezone || null, status:String(account.account_status || account.status || ""), metadata:account, updated_at:new Date().toISOString() };
  if (!row.platform_account_id) return;
  await supabaseAdmin.from("platform_ad_accounts").upsert(row, { onConflict:"user_id,platform,platform_account_id" });
}
async function metaGraph(pathname, params, token) {
  const u = new URL(`https://graph.facebook.com/${META_GRAPH_VERSION}${pathname}`);
  Object.entries(params || {}).forEach(([k,v]) => u.searchParams.set(k,v));
  u.searchParams.set("access_token", token);
  const r = await fetch(u.toString());
  const data = await r.json();
  if (!r.ok) throw new Error(data.error?.message || JSON.stringify(data));
  return data;
}
app.get("/api/meta/adaccounts", async (req, res) => {
  try {
    const user = await requireUser(req, res); if (!user) return;
    const conn = await getConnection(user.id, "meta"); if (!conn) return res.status(404).json({ error:"Meta not connected" });
    const data = await metaGraph("/me/adaccounts", { fields:"id,name,account_status,currency,timezone_name", limit:"100" }, conn.access_token);
    for (const a of (data.data || [])) await upsertAdAccount(user.id, "meta", a);
    res.json({ platform:"meta", accounts:data.data || [] });
  } catch(e) { res.status(500).json({ error:e.message }); }
});
app.get("/api/google/customers", async (req, res) => {
  try {
    const user = await requireUser(req, res); if (!user) return;
    const token = await getFreshGoogleAccessToken(user.id);
    const r = await fetch(`https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers:listAccessibleCustomers`, { headers:{ Authorization:`Bearer ${token}`, "developer-token":process.env.GOOGLE_DEVELOPER_TOKEN || "" }});
    const data = await r.json();
    if (!r.ok) return res.status(r.status).json({ status:r.status, error:JSON.stringify(data) });
    const customers = (data.resourceNames || []).map(resourceName => ({ resourceName, customerId:String(resourceName).replace("customers/","") }));
    for (const c of customers) await upsertAdAccount(user.id, "google", { id:c.customerId, customerId:c.customerId, name:c.customerId, status:"accessible" });
    res.json({ customers });
  } catch(e) { res.status(500).json({ error:e.message }); }
});
app.get("/api/pinterest/adaccounts", async (req, res) => {
  try {
    const user = await requireUser(req, res); if (!user) return;
    const conn = await getConnection(user.id, "pinterest"); if (!conn) return res.status(404).json({ error:"Pinterest not connected" });
    const r = await fetch(`${PINTEREST_API_BASE}/ad_accounts`, { headers:{ Authorization:`Bearer ${conn.access_token}`, "Content-Type":"application/json" }});
    const data = await r.json();
    if (!r.ok) throw new Error(data.message || JSON.stringify(data));
    for (const a of (data.items || [])) await upsertAdAccount(user.id, "pinterest", { id:a.id, name:a.name, currency:a.currency, status:"accessible", ...a });
    res.json(data);
  } catch(e) { res.status(500).json({ error:e.message }); }
});
app.get("/api/accounts", async (req, res) => {
  const user = await requireUser(req, res); if (!user) return;
  const { data, error } = await supabaseAdmin.from("platform_ad_accounts").select("*").eq("user_id", user.id).order("platform", { ascending:true });
  if (error) return res.status(500).json({ error:error.message });
  res.json({ accounts:data || [] });
});

if (process.env.VERCEL !== "1") app.listen(PORT, () => console.log(`AdsTable stabilization running on ${PORT}`));
module.exports = app;
