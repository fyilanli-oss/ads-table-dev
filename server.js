const express = require("express");
const session = require("express-session");
const path = require("path");
const { google } = require("googleapis");

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

/* META */
const META_GRAPH_VERSION = "v20.0";
function requireMetaConfig() {
  if (!process.env.META_APP_ID || !process.env.META_APP_SECRET || !process.env.META_REDIRECT_URI) throw new Error("Missing Meta env");
}
app.get("/auth/meta", (req, res) => {
  try {
    requireMetaConfig();
    const state = Math.random().toString(36).slice(2);
    req.session.metaOAuthState = state;
    const params = new URLSearchParams({ client_id: process.env.META_APP_ID, redirect_uri: process.env.META_REDIRECT_URI, state, response_type: "code", scope: "ads_read" });
    res.redirect(`https://www.facebook.com/${META_GRAPH_VERSION}/dialog/oauth?${params.toString()}`);
  } catch (e) { res.status(500).send(e.message); }
});
app.get("/auth/meta/callback", async (req, res) => {
  try {
    requireMetaConfig();
    const { code, state, error, error_description } = req.query;
    if (error) return res.redirect(`/?meta_error=${encodeURIComponent(error_description || error)}`);
    if (!code) return res.redirect("/?meta_error=Missing authorization code");
    if (!state || state !== req.session.metaOAuthState) return res.redirect("/?meta_error=Invalid OAuth state");
    const tokenUrl = new URL(`https://graph.facebook.com/${META_GRAPH_VERSION}/oauth/access_token`);
    tokenUrl.searchParams.set("client_id", process.env.META_APP_ID);
    tokenUrl.searchParams.set("redirect_uri", process.env.META_REDIRECT_URI);
    tokenUrl.searchParams.set("client_secret", process.env.META_APP_SECRET);
    tokenUrl.searchParams.set("code", code);
    const r = await fetch(tokenUrl.toString());
    const data = await r.json();
    if (!r.ok || !data.access_token) throw new Error(data.error?.message || "Meta token exchange failed");
    req.session.meta = { accessToken: data.access_token, connectedAt: new Date().toISOString() };
    res.redirect("/?meta_connected=1");
  } catch (e) { res.redirect(`/?meta_error=${encodeURIComponent(e.message)}`); }
});
async function metaGet(pathname, params, token) {
  const url = new URL(`https://graph.facebook.com/${META_GRAPH_VERSION}${pathname}`);
  Object.entries(params || {}).forEach(([k,v]) => url.searchParams.set(k,v));
  url.searchParams.set("access_token", token);
  const r = await fetch(url.toString());
  const data = await r.json();
  if (!r.ok) throw new Error(data.error?.message || JSON.stringify(data));
  return data;
}
app.get("/api/meta/status", (req,res)=>res.json({connected:!!req.session?.meta?.accessToken,connectedAt:req.session?.meta?.connectedAt||null}));
app.get("/api/meta/adaccounts", async (req,res)=>{try{if(!req.session?.meta?.accessToken)return res.status(401).json({error:"Meta not connected"});const data=await metaGet("/me/adaccounts",{fields:"id,name,account_status,currency,timezone_name",limit:"50"},req.session.meta.accessToken);res.json({accounts:data.data||[]});}catch(e){res.status(500).json({error:e.message});}});
function av(actions,type){if(!Array.isArray(actions))return 0;const f=actions.find(a=>a.action_type===type);return f?Number(f.value||0):0;}
app.get("/api/meta/insights", async (req,res)=>{try{if(!req.session?.meta?.accessToken)return res.status(401).json({error:"Meta not connected"});const adAccountId=req.query.adAccountId;if(!adAccountId)return res.status(400).json({error:"Missing adAccountId"});const range=req.query.range||"today";const preset=range==="7d"?"last_7d":range==="month"?"this_month":range;const data=await metaGet(`/${adAccountId}/insights`,{level:"campaign",fields:"campaign_id,campaign_name,impressions,clicks,spend,actions,action_values",date_preset:preset,limit:"100"},req.session.meta.accessToken);const campaigns=(data.data||[]).map(row=>{const spend=Number(row.spend||0),clicks=Number(row.clicks||0),impressions=Number(row.impressions||0),sales=av(row.action_values,"purchase")||av(row.action_values,"offsite_conversion.fb_pixel_purchase");return{platform:"Meta",id:row.campaign_id||"",name:row.campaign_name||"Unknown campaign",spend,sales,impressions,clicks,ctr:impressions?clicks/impressions:0,cpc:clicks?spend/clicks:0,roas:spend?sales/spend:0,acos:sales?spend/sales:0,raw:row};});res.json({platform:"Meta",range,campaigns});}catch(e){res.status(500).json({error:e.message});}});

/* GOOGLE */
const GOOGLE_ADS_API_VERSION = process.env.GOOGLE_ADS_API_VERSION || "v24";
function requireGoogleConfig(){if(!process.env.GOOGLE_CLIENT_ID||!process.env.GOOGLE_CLIENT_SECRET||!process.env.GOOGLE_REDIRECT_URI||!process.env.GOOGLE_DEVELOPER_TOKEN)throw new Error("Missing Google env");}
function googleOAuthClient(){return new google.auth.OAuth2(process.env.GOOGLE_CLIENT_ID,process.env.GOOGLE_CLIENT_SECRET,process.env.GOOGLE_REDIRECT_URI);}
app.get("/auth/google",(req,res)=>{try{requireGoogleConfig();const state=Math.random().toString(36).slice(2);req.session.googleOAuthState=state;const url=googleOAuthClient().generateAuthUrl({access_type:"offline",prompt:"consent",state,scope:["https://www.googleapis.com/auth/adwords"]});res.redirect(url);}catch(e){res.status(500).send(e.message);}});
app.get("/auth/google/callback",async(req,res)=>{try{requireGoogleConfig();const{code,state,error}=req.query;if(error)return res.redirect(`/?google_error=${encodeURIComponent(error)}`);if(!code)return res.redirect("/?google_error=Missing authorization code");if(!state||state!==req.session.googleOAuthState)return res.redirect("/?google_error=Invalid OAuth state");const client=googleOAuthClient();const{tokens}=await client.getToken(code);req.session.google={tokens:{...req.session?.google?.tokens,...tokens},connectedAt:new Date().toISOString()};res.redirect("/?google_connected=1");}catch(e){res.redirect(`/?google_error=${encodeURIComponent(e.message)}`);}});
app.get("/api/google/status",(req,res)=>res.json({connected:!!(req.session?.google?.tokens?.refresh_token||req.session?.google?.tokens?.access_token),connectedAt:req.session?.google?.connectedAt||null}));
async function googleAccessToken(req){if(!req.session?.google?.tokens)throw new Error("Google not connected");const client=googleOAuthClient();client.setCredentials(req.session.google.tokens);const result=await client.getAccessToken();const token=result?.token||client.credentials.access_token||req.session.google.tokens.access_token;req.session.google.tokens={...req.session.google.tokens,...client.credentials};if(!token)throw new Error("Could not get Google access token");return token;}
async function googleAdsFetch(req,url,options={}){const headers={Authorization:`Bearer ${await googleAccessToken(req)}`,"developer-token":process.env.GOOGLE_DEVELOPER_TOKEN,"Content-Type":"application/json",...(options.headers||{})};if(process.env.GOOGLE_LOGIN_CUSTOMER_ID)headers["login-customer-id"]=String(process.env.GOOGLE_LOGIN_CUSTOMER_ID).replace(/-/g,"");const r=await fetch(url,{...options,headers});const text=await r.text();let data;try{data=text?JSON.parse(text):{};}catch{data={raw:text};}if(!r.ok)throw new Error(data.error?.message||text||`Google Ads API error ${r.status}`);return data;}
app.get("/api/google/customers",async(req,res)=>{try{requireGoogleConfig();const url=`https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers:listAccessibleCustomers`;const data=await googleAdsFetch(req,url,{method:"GET"});res.json({customers:(data.resourceNames||[]).map(r=>({resourceName:r,customerId:r.replace("customers/","")}))});}catch(e){res.status(500).json({error:e.message});}});
app.get("/api/google/insights",async(req,res)=>{try{requireGoogleConfig();const customerId=String(req.query.customerId||"").replace(/-/g,"");if(!customerId)return res.status(400).json({error:"Missing customerId"});const query=`SELECT campaign.id,campaign.name,campaign.status,metrics.impressions,metrics.clicks,metrics.ctr,metrics.average_cpc,metrics.cost_micros,metrics.conversions,metrics.conversions_value FROM campaign WHERE segments.date DURING LAST_30_DAYS LIMIT 100`;const url=`https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers/${customerId}/googleAds:searchStream`;const data=await googleAdsFetch(req,url,{method:"POST",body:JSON.stringify({query})});const rows=Array.isArray(data)?data.flatMap(x=>x.results||[]):data.results||[];const campaigns=rows.map(row=>{const m=row.metrics||{},c=row.campaign||{};const spend=Number(m.costMicros||0)/1000000,clicks=Number(m.clicks||0),impressions=Number(m.impressions||0),sales=Number(m.conversionsValue||0);return{platform:"Google",id:c.id||"",name:c.name||"Unknown campaign",status:c.status||"",spend,sales,impressions,clicks,ctr:Number(m.ctr||(impressions?clicks/impressions:0)),cpc:Number(m.averageCpc||0)/1000000,roas:spend?sales/spend:0,acos:sales?spend/sales:0,conversions:Number(m.conversions||0),raw:row};});res.json({platform:"Google",campaigns});}catch(e){res.status(500).json({error:e.message});}});

/* PINTEREST */
const PINTEREST_API_BASE="https://api.pinterest.com/v5";
function requirePinterestConfig(){if(!process.env.PINTEREST_CLIENT_ID||!process.env.PINTEREST_CLIENT_SECRET||!process.env.PINTEREST_REDIRECT_URI)throw new Error("Missing Pinterest env");}
function pinterestBasicAuth(){return Buffer.from(`${process.env.PINTEREST_CLIENT_ID}:${process.env.PINTEREST_CLIENT_SECRET}`).toString("base64");}
app.get("/auth/pinterest",(req,res)=>{try{requirePinterestConfig();const state=Math.random().toString(36).slice(2);req.session.pinterestOAuthState=state;const params=new URLSearchParams({response_type:"code",client_id:process.env.PINTEREST_CLIENT_ID,redirect_uri:process.env.PINTEREST_REDIRECT_URI,scope:"ads:read",state});res.redirect(`https://www.pinterest.com/oauth/?${params.toString()}`);}catch(e){res.status(500).send(e.message);}});
app.get("/auth/pinterest/callback",async(req,res)=>{try{requirePinterestConfig();const{code,state,error,error_description}=req.query;if(error)return res.redirect(`/?pinterest_error=${encodeURIComponent(error_description||error)}`);if(!code)return res.redirect("/?pinterest_error=Missing authorization code");if(!state||state!==req.session.pinterestOAuthState)return res.redirect("/?pinterest_error=Invalid OAuth state");const body=new URLSearchParams({grant_type:"authorization_code",code,redirect_uri:process.env.PINTEREST_REDIRECT_URI});const r=await fetch(`${PINTEREST_API_BASE}/oauth/token`,{method:"POST",headers:{Authorization:`Basic ${pinterestBasicAuth()}`,"Content-Type":"application/x-www-form-urlencoded"},body:body.toString()});const data=await r.json();if(!r.ok||!data.access_token)throw new Error(data.message||data.error_description||data.error||"Pinterest token exchange failed");req.session.pinterest={accessToken:data.access_token,refreshToken:data.refresh_token||null,connectedAt:new Date().toISOString()};res.redirect("/?pinterest_connected=1");}catch(e){res.redirect(`/?pinterest_error=${encodeURIComponent(e.message)}`);}});
app.get("/api/pinterest/status",(req,res)=>res.json({connected:!!req.session?.pinterest?.accessToken,connectedAt:req.session?.pinterest?.connectedAt||null}));
async function pinterestFetch(req,endpoint,options={}){if(!req.session?.pinterest?.accessToken)throw new Error("Pinterest not connected");const r=await fetch(`${PINTEREST_API_BASE}${endpoint}`,{...options,headers:{Authorization:`Bearer ${req.session.pinterest.accessToken}`,"Content-Type":"application/json",...(options.headers||{})}});const text=await r.text();let data;try{data=text?JSON.parse(text):{};}catch{data={raw:text};}if(!r.ok)throw new Error(data.message||data.error?.message||text||`Pinterest API error ${r.status}`);return data;}
app.get("/api/pinterest/adaccounts",async(req,res)=>{try{requirePinterestConfig();const data=await pinterestFetch(req,"/ad_accounts");res.json(data);}catch(e){res.status(500).json({error:e.message});}});
app.get("/api/pinterest/campaigns",async(req,res)=>{try{requirePinterestConfig();const adAccountId=req.query.adAccountId;if(!adAccountId)return res.status(400).json({error:"Missing adAccountId"});const data=await pinterestFetch(req,`/ad_accounts/${adAccountId}/campaigns`);res.json(data);}catch(e){res.status(500).json({error:e.message});}});
app.get("/api/pinterest/analytics",async(req,res)=>{try{requirePinterestConfig();const adAccountId=req.query.adAccountId;if(!adAccountId)return res.status(400).json({error:"Missing adAccountId"});const startDate=req.query.startDate||new Date(Date.now()-7*86400000).toISOString().slice(0,10);const endDate=req.query.endDate||new Date().toISOString().slice(0,10);const params=new URLSearchParams({start_date:startDate,end_date:endDate,granularity:"TOTAL",columns:"SPEND_IN_DOLLAR,IMPRESSION_1,CLICKTHROUGH_1,TOTAL_CONVERSIONS,TOTAL_CONVERSIONS_VALUE_IN_MICRO_DOLLAR"});const data=await pinterestFetch(req,`/ad_accounts/${adAccountId}/analytics?${params.toString()}`);res.json({platform:"Pinterest",adAccountId,startDate,endDate,data});}catch(e){res.status(500).json({error:e.message});}});

/* TIKTOK SKELETON */
function requireTikTokConfig(){if(!process.env.TIKTOK_CLIENT_ID||!process.env.TIKTOK_CLIENT_SECRET||!process.env.TIKTOK_REDIRECT_URI)throw new Error("TikTok pending: missing TikTok env");}
app.get("/auth/tiktok",(req,res)=>{try{requireTikTokConfig();res.status(501).json({status:"pending",message:"TikTok verification is pending. OAuth skeleton is ready.",redirectUri:process.env.TIKTOK_REDIRECT_URI});}catch(e){res.status(500).send(e.message);}});
app.get("/auth/tiktok/callback",(req,res)=>res.redirect("/?tiktok_pending=1"));
app.get("/api/tiktok/status",(req,res)=>res.json({connected:!!req.session?.tiktok?.accessToken,status:"pending_verification",connectedAt:req.session?.tiktok?.connectedAt||null}));

app.get("/api/unified/status",(req,res)=>res.json({meta:!!req.session?.meta?.accessToken,google:!!(req.session?.google?.tokens?.refresh_token||req.session?.google?.tokens?.access_token),pinterest:!!req.session?.pinterest?.accessToken,tiktok:!!req.session?.tiktok?.accessToken,tiktokStatus:"pending_verification"}));

if(process.env.VERCEL!=="1") app.listen(PORT,()=>console.log(`AdsTable dev server running on ${PORT}`));
module.exports=app;
