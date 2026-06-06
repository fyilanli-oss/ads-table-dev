
const express=require("express");
const session=require("express-session");
const path=require("path");
const crypto=require("crypto");
const {google}=require("googleapis");
const {createClient}=require("@supabase/supabase-js");
const app=express(); const PORT=process.env.PORT||3000;
app.set("trust proxy",1); app.use(express.json());
app.use(session({secret:process.env.SESSION_SECRET||"dev_secret_change_me",resave:false,saveUninitialized:false,cookie:{httpOnly:true,sameSite:"lax",secure:process.env.NODE_ENV==="production"||process.env.VERCEL==="1"}}));
app.use(express.static(path.join(__dirname,"public")));
const META_GRAPH_VERSION=process.env.META_GRAPH_VERSION||"v20.0";
const PINTEREST_API_BASE="https://api.pinterest.com/v5";
const KLAVIYO_API_BASE="https://a.klaviyo.com";
const KLAVIYO_WWW_BASE="https://www.klaviyo.com";
const GOOGLE_ADS_API_VERSION=process.env.GOOGLE_ADS_API_VERSION||"v24";
const supabaseAdmin=(process.env.SUPABASE_URL&&process.env.SUPABASE_SERVICE_ROLE_KEY)?createClient(process.env.SUPABASE_URL,process.env.SUPABASE_SERVICE_ROLE_KEY,{auth:{persistSession:false}}):null;
function sendFile(res,file){res.sendFile(path.join(__dirname,"public",file))}
app.get("/",(_,res)=>sendFile(res,"landing.html")); app.get("/dashboard-demo",(_,res)=>sendFile(res,"dashboard-demo.html")); app.get("/login",(_,res)=>sendFile(res,"login.html")); app.get("/signup",(_,res)=>sendFile(res,"signup.html")); app.get("/dashboard",(_,res)=>sendFile(res,"dashboard.html")); app.get("/demo",(_,res)=>sendFile(res,"dashboard-demo.html")); app.get("/privacy",(_,res)=>sendFile(res,"privacy.html")); app.get("/terms",(_,res)=>sendFile(res,"terms.html")); app.get("/data-deletion",(_,res)=>sendFile(res,"data-deletion.html"));
app.get("/api/public-config",(_,res)=>res.json({supabaseUrl:process.env.SUPABASE_URL||"",supabaseAnonKey:process.env.SUPABASE_ANON_KEY||process.env.SUPABASE_PUBLISHABLE_KEY||""}));
async function getUserFromRequest(req){const a=req.headers.authorization||"";const t=a.startsWith("Bearer ")?a.slice(7):null;if(!t||!supabaseAdmin)return null;const {data,error}=await supabaseAdmin.auth.getUser(t);if(error||!data?.user?.id)return null;return data.user}
async function requireUser(req,res){const u=await getUserFromRequest(req);if(!u){res.status(401).json({error:"Not authenticated"});return null}return u}
async function expireTrialsIfNeeded(){if(!supabaseAdmin)return;const{error}=await supabaseAdmin.rpc("expire_trials");if(error)throw error}
async function getUserSubscription(userId){await expireTrialsIfNeeded();const{data,error}=await supabaseAdmin.from("subscriptions").select("status,trial_end_date").eq("user_id",userId).maybeSingle();if(error)throw error;return data}
function getAccessByStatus(status){const full=["trial","active"].includes(status);const readonly=["expired","cancelled"].includes(status);const blocked=["suspended","deleted"].includes(status);return{dashboard:full||readonly,snapshots:full||readonly,insightHistory:full||readonly,connect:full,manualRefresh:full,dailySync:full,export:full,aiInsights:full,blocked}}
async function requireAccess(req,res,userId,capability){const sub=await getUserSubscription(userId);const access=getAccessByStatus(sub?.status);if(access.blocked||!access[capability]){res.status(403).json({error:"Subscription inactive",status:sub?.status||null});return null}return{sub,access}}
async function requireConnectAccessForOAuth(req,res){const userId=req.query.user_id;if(!userId){res.redirect("/dashboard?error=missing_user_id");return null}const sub=await getUserSubscription(userId);const access=getAccessByStatus(sub?.status);if(access.blocked||!access.connect){res.redirect(`/dashboard?subscription_inactive=1&status=${encodeURIComponent(sub?.status||"")}`);return null}return{userId,sub,access}}
function parseExpiry(s){return s?new Date(Date.now()+Number(s)*1000).toISOString():null}
async function saveConnection(userId,platform,payload){if(!supabaseAdmin||!userId)throw new Error("Supabase not configured or user missing");const row={user_id:userId,platform,access_token:payload.accessToken||null,refresh_token:payload.refreshToken||null,token_expires_at:payload.tokenExpiresAt||null,account_id:payload.accountId||null,account_name:payload.accountName||null,metadata:payload.metadata||{},connected:true,updated_at:new Date().toISOString()};const {error}=await supabaseAdmin.from("platform_connections").upsert(row,{onConflict:"user_id,platform"});if(error)throw new Error(error.message)}
async function getConnection(userId,platform){if(!supabaseAdmin||!userId)return null;const {data,error}=await supabaseAdmin.from("platform_connections").select("*").eq("user_id",userId).eq("platform",platform).eq("connected",true).maybeSingle();if(error)throw new Error(error.message);return data}
async function connectionStatus(userId,platform){const r=await getConnection(userId,platform).catch(()=>null);return{connected:Boolean(r&&(r.access_token||r.refresh_token)),source:r?"database":"none",updatedAt:r?.updated_at||null}}
async function requireConnection(req,res,platform){const user=await requireUser(req,res);if(!user)return null;const sub=await getSubscriptionForLifecycle(user.id);const access=getLifecycleAccess(sub?.status);if(access.blocked){res.status(403).json({error:"Account access blocked",status:access.status});return null}const conn=await getConnection(user.id,platform);if(!conn){res.status(404).json({error:`${platform} not connected`});return null}return{user,conn}}
function googleOAuthClient(){if(!process.env.GOOGLE_CLIENT_ID||!process.env.GOOGLE_CLIENT_SECRET||!process.env.GOOGLE_REDIRECT_URI)throw new Error("Missing Google OAuth env");return new google.auth.OAuth2(process.env.GOOGLE_CLIENT_ID,process.env.GOOGLE_CLIENT_SECRET,process.env.GOOGLE_REDIRECT_URI)}
async function getFreshGoogleAccessToken(userId){const conn=await getConnection(userId,"google");if(!conn)throw new Error("Google not connected");const exp=conn.token_expires_at?new Date(conn.token_expires_at).getTime():0;if(conn.access_token&&exp&&exp>Date.now()+120000)return conn.access_token;if(!conn.refresh_token){if(conn.access_token)return conn.access_token;throw new Error("Google refresh token missing. Please reconnect Google.")}const client=googleOAuthClient();client.setCredentials({refresh_token:conn.refresh_token});const {credentials}=await client.refreshAccessToken();const token=credentials.access_token;const expiry=credentials.expiry_date||(Date.now()+3600*1000);await saveConnection(userId,"google",{accessToken:token,refreshToken:conn.refresh_token,tokenExpiresAt:new Date(expiry).toISOString(),metadata:{...(conn.metadata||{}),refreshedAt:new Date().toISOString(),expiryDate:expiry}});return token}
app.get("/auth/meta",async(req,res)=>{try{const accessCheck=await requireConnectAccessForOAuth(req,res);if(!accessCheck)return;const userId=accessCheck.userId;if(!process.env.META_APP_ID||!process.env.META_REDIRECT_URI)throw new Error("Missing Meta env");const state=Math.random().toString(36).slice(2);req.session.metaOAuthState=state;req.session.oauthUserId=userId;const p=new URLSearchParams({client_id:process.env.META_APP_ID,redirect_uri:process.env.META_REDIRECT_URI,state,response_type:"code",scope:"ads_read"});res.redirect(`https://www.facebook.com/${META_GRAPH_VERSION}/dialog/oauth?${p}`)}catch(e){res.status(500).send(e.message)}});
app.get("/auth/meta/callback",async(req,res)=>{try{const{code,state,error,error_description}=req.query;if(error)return res.redirect(`/dashboard?meta_error=${encodeURIComponent(error_description||error)}`);if(!code)return res.redirect("/dashboard?meta_error=missing_code");if(!state||state!==req.session.metaOAuthState)return res.redirect("/dashboard?meta_error=invalid_state");const userId=req.session.oauthUserId;if(!userId)return res.redirect("/dashboard?meta_error=missing_user_id");const url=new URL(`https://graph.facebook.com/${META_GRAPH_VERSION}/oauth/access_token`);url.searchParams.set("client_id",process.env.META_APP_ID);url.searchParams.set("redirect_uri",process.env.META_REDIRECT_URI);url.searchParams.set("client_secret",process.env.META_APP_SECRET);url.searchParams.set("code",code);const r=await fetch(url);const data=await r.json();if(!r.ok||!data.access_token)throw new Error(data.error?.message||"Meta token exchange failed");await saveConnection(userId,"meta",{accessToken:data.access_token,tokenExpiresAt:parseExpiry(data.expires_in),metadata:{expiresIn:data.expires_in||null}});req.session.metaOAuthState=null;res.redirect("/dashboard?meta_connected=1")}catch(e){res.redirect(`/dashboard?meta_error=${encodeURIComponent(e.message)}`)}});
app.get("/auth/google",async(req,res)=>{try{const accessCheck=await requireConnectAccessForOAuth(req,res);if(!accessCheck)return;const userId=accessCheck.userId;const state=Math.random().toString(36).slice(2);req.session.googleOAuthState=state;req.session.oauthUserId=userId;const url=googleOAuthClient().generateAuthUrl({access_type:"offline",prompt:"consent",state,scope:["https://www.googleapis.com/auth/adwords"]});res.redirect(url)}catch(e){res.status(500).send(e.message)}});
app.get("/auth/google/callback",async(req,res)=>{try{const{code,state,error}=req.query;if(error)return res.redirect(`/dashboard?google_error=${encodeURIComponent(error)}`);if(!code)return res.redirect("/dashboard?google_error=missing_code");if(!state||state!==req.session.googleOAuthState)return res.redirect("/dashboard?google_error=invalid_state");const userId=req.session.oauthUserId;if(!userId)return res.redirect("/dashboard?google_error=missing_user_id");const client=googleOAuthClient();const{tokens}=await client.getToken(code);await saveConnection(userId,"google",{accessToken:tokens.access_token,refreshToken:tokens.refresh_token||null,tokenExpiresAt:tokens.expiry_date?new Date(tokens.expiry_date).toISOString():null,metadata:{scope:tokens.scope||null,expiryDate:tokens.expiry_date||null,tokenType:tokens.token_type||null}});req.session.googleOAuthState=null;res.redirect("/dashboard?google_connected=1")}catch(e){res.redirect(`/dashboard?google_error=${encodeURIComponent(e.message)}`)}});
function pinterestBasic(){return Buffer.from(`${process.env.PINTEREST_CLIENT_ID}:${process.env.PINTEREST_CLIENT_SECRET}`).toString("base64")}
app.get("/auth/pinterest",async(req,res)=>{try{const accessCheck=await requireConnectAccessForOAuth(req,res);if(!accessCheck)return;const userId=accessCheck.userId;if(!process.env.PINTEREST_CLIENT_ID||!process.env.PINTEREST_REDIRECT_URI)throw new Error("Missing Pinterest env");const state=Math.random().toString(36).slice(2);req.session.pinterestOAuthState=state;req.session.oauthUserId=userId;const p=new URLSearchParams({response_type:"code",client_id:process.env.PINTEREST_CLIENT_ID,redirect_uri:process.env.PINTEREST_REDIRECT_URI,scope:"ads:read",state});res.redirect(`https://www.pinterest.com/oauth/?${p}`)}catch(e){res.status(500).send(e.message)}});
app.get("/auth/pinterest/callback",async(req,res)=>{try{const{code,state,error,error_description}=req.query;if(error)return res.redirect(`/dashboard?pinterest_error=${encodeURIComponent(error_description||error)}`);if(!code)return res.redirect("/dashboard?pinterest_error=missing_code");if(!state||state!==req.session.pinterestOAuthState)return res.redirect("/dashboard?pinterest_error=invalid_state");const userId=req.session.oauthUserId;if(!userId)return res.redirect("/dashboard?pinterest_error=missing_user_id");const body=new URLSearchParams({grant_type:"authorization_code",code,redirect_uri:process.env.PINTEREST_REDIRECT_URI});const r=await fetch(`${PINTEREST_API_BASE}/oauth/token`,{method:"POST",headers:{Authorization:`Basic ${pinterestBasic()}`,"Content-Type":"application/x-www-form-urlencoded"},body:body.toString()});const data=await r.json();if(!r.ok||!data.access_token)throw new Error(data.message||data.error_description||data.error||"Pinterest token exchange failed");await saveConnection(userId,"pinterest",{accessToken:data.access_token,refreshToken:data.refresh_token||null,tokenExpiresAt:parseExpiry(data.expires_in),metadata:{scope:data.scope||null,expiresIn:data.expires_in||null}});req.session.pinterestOAuthState=null;res.redirect("/dashboard?pinterest_connected=1")}catch(e){res.redirect(`/dashboard?pinterest_error=${encodeURIComponent(e.message)}`)}});

function base64Url(input){return Buffer.from(input).toString("base64").replace(/\+/g,"-").replace(/\//g,"_").replace(/=+$/g,"")}
function klaviyoBasic(){return Buffer.from(`${process.env.KLAVIYO_CLIENT_ID}:${process.env.KLAVIYO_CLIENT_SECRET}`).toString("base64")}
function klaviyoScopes(){return process.env.KLAVIYO_SCOPES||"accounts:read campaigns:read events:read metrics:read conversations:read"}
function klaviyoDateWindow(range,startDate,endDate){
  const end=endDate?new Date(`${endDate}T23:59:59Z`):new Date();
  const start=startDate?new Date(`${startDate}T00:00:00Z`):new Date(end);
  if(!startDate){
    if(range==="today"){}
    else if(range==="yesterday"){start.setDate(start.getDate()-1);end.setDate(end.getDate()-1)}
    else if(range==="last_30d")start.setDate(start.getDate()-29);
    else start.setDate(start.getDate()-6);
  }
  const iso=d=>d.toISOString();
  const dayCount=Math.max(1,Math.ceil((end-start)/(24*60*60*1000))+1);
  return{start:iso(start),end:iso(end),selected_day_count:dayCount}
}
function safeNumber(v){return v===null||v===undefined||v===""?null:Number(v)}
function deepFindNumber(obj,keys){
  if(!obj||typeof obj!=="object")return null;
  for(const k of keys){if(obj[k]!==undefined&&obj[k]!==null&&obj[k]!==""&&!Number.isNaN(Number(obj[k])))return Number(obj[k])}
  for(const v of Object.values(obj)){if(v&&typeof v==="object"){const found=deepFindNumber(v,keys);if(found!==null)return found}}
  return null
}
async function klaviyoFetch(conn,endpoint,options={}){
  const r=await fetch(`${KLAVIYO_API_BASE}${endpoint}`,{...options,headers:{Authorization:`Bearer ${conn.access_token}`,Accept:"application/json",Revision:process.env.KLAVIYO_REVISION||"2024-10-15","Content-Type":"application/json",...(options.headers||{})}});
  const text=await r.text();let data;try{data=text?JSON.parse(text):{}}catch{data={raw:text}}
  if(!r.ok)throw new Error(data.errors?.[0]?.detail||data.errors?.[0]?.title||data.message||text||`Klaviyo API error ${r.status}`);
  return data
}
async function getKlaviyoMetricId(conn,names){
  const data=await klaviyoFetch(conn,"/api/metrics/");
  const list=Array.isArray(data.data)?data.data:[];
  const wanted=names.map(x=>String(x).toLowerCase());
  const found=list.find(m=>wanted.includes(String(m.attributes?.name||m.name||"").toLowerCase()));
  return found?.id||null
}
function klaviyoCampaignName(c){return c.attributes?.name||c.name||c.attributes?.campaign_name||null}
function klaviyoCampaignStatus(c){return c.attributes?.status||c.status||null}
function klaviyoCampaignSendTime(c){return c.attributes?.send_time||c.attributes?.sendTime||c.send_time||null}
function normalizeKlaviyoInsight({campaign,report,settings,window,extraEvents={}}){
  const attr=report?.attributes||report||{};
  const stats=attr.statistics||attr.stats||attr;
  const estimatedMonthlySpend=Number(settings.estimatedMonthlySpend||0);
  const estimatedPeriodSpend=(estimatedMonthlySpend/30)*window.selected_day_count;
  const spend=estimatedPeriodSpend||0;
  const delivered=deepFindNumber(stats,["delivered","emails_delivered","DELIVERED"])??0;
  const opens=deepFindNumber(stats,["opens","open","opened","OPENED_EMAIL"])??null;
  const clicks=deepFindNumber(stats,["clicks","click","CLICKED_EMAIL"])??0;
  const clickRate=deepFindNumber(stats,["click_rate","clickRate"])??(delivered?clicks/delivered:null);
  const sales=deepFindNumber(stats,["conversion_value","conversions_value","revenue","sales"])??0;
  const purchaseCount=deepFindNumber(stats,["conversions","placed_order","purchase_count"])??0;
  const addToCart=extraEvents.add_to_cart??null;
  const checkout=extraEvents.checkout_started??null;
  const siteVisit=extraEvents.site_visited??null;
  const abandoned=checkout!==null?Math.max((checkout||0)-(purchaseCount||0),0):null;
  const trafficScore=siteVisit&&clicks?(siteVisit/clicks)*100:null;
  return{
    platform:"Klaviyo",
    level:"campaign",
    campaign_id:campaign?.id||attr.campaign_id||null,
    campaign_name:klaviyoCampaignName(campaign)||attr.campaign_name||null,
    campaign_status:klaviyoCampaignStatus(campaign)||null,
    send_time:klaviyoCampaignSendTime(campaign)||null,
    currency:settings.spendCurrency||null,
    impressions:delivered,
    emails_delivered:delivered,
    clicks,
    ctr:clickRate!==null?Number(clickRate)*100:null,
    cpc:clicks?spend/clicks:null,
    spend,
    estimated_monthly_spend:estimatedMonthlySpend,
    estimated_period_spend:spend,
    selected_day_count:window.selected_day_count,
    sales,
    revenue:sales,
    roas:spend>0?sales/spend:null,
    acos:sales>0?(spend/sales)*100:null,
    cvr:clicks?purchaseCount/clicks*100:null,
    email_open:opens,
    opened_email:opens,
    link_clicks:clicks,
    site_visit:siteVisit,
    landing_page_views:siteVisit,
    traffic_score:trafficScore,
    real_cpc:siteVisit?spend/siteVisit:null,
    add_to_cart:addToCart,
    checkout,
    purchase:purchaseCount,
    purchases:purchaseCount,
    purchase_count:purchaseCount,
    abandoned,
    raw:{campaign,report,extraEvents}
  }
}
app.get("/auth/klaviyo",async(req,res)=>{try{const accessCheck=await requireConnectAccessForOAuth(req,res);if(!accessCheck)return;const userId=accessCheck.userId;if(!process.env.KLAVIYO_CLIENT_ID||!process.env.KLAVIYO_CLIENT_SECRET||!process.env.KLAVIYO_REDIRECT_URI)throw new Error("Missing Klaviyo env");const state=Math.random().toString(36).slice(2);const codeVerifier=base64Url(crypto.randomBytes(64));const codeChallenge=base64Url(crypto.createHash("sha256").update(codeVerifier).digest());req.session.klaviyoOAuthState=state;req.session.klaviyoCodeVerifier=codeVerifier;req.session.oauthUserId=userId;const p=new URLSearchParams({response_type:"code",client_id:process.env.KLAVIYO_CLIENT_ID,redirect_uri:process.env.KLAVIYO_REDIRECT_URI,scope:klaviyoScopes(),state,code_challenge_method:"S256",code_challenge:codeChallenge});res.redirect(`${KLAVIYO_WWW_BASE}/oauth/authorize?${p}`)}catch(e){res.status(500).send(e.message)}});
app.get("/auth/klaviyo/callback",async(req,res)=>{try{const{code,state,error,error_description}=req.query;if(error)return res.redirect(`/dashboard?klaviyo_error=${encodeURIComponent(error_description||error)}`);if(!code)return res.redirect("/dashboard?klaviyo_error=missing_code");if(!state||state!==req.session.klaviyoOAuthState)return res.redirect("/dashboard?klaviyo_error=invalid_state");const userId=req.session.oauthUserId;if(!userId)return res.redirect("/dashboard?klaviyo_error=missing_user_id");const verifier=req.session.klaviyoCodeVerifier;if(!verifier)return res.redirect("/dashboard?klaviyo_error=missing_code_verifier");const body=new URLSearchParams({grant_type:"authorization_code",code,redirect_uri:process.env.KLAVIYO_REDIRECT_URI,code_verifier:verifier});const r=await fetch(`${KLAVIYO_API_BASE}/oauth/token`,{method:"POST",headers:{Authorization:`Basic ${klaviyoBasic()}`,"Content-Type":"application/x-www-form-urlencoded"},body:body.toString()});const data=await r.json().catch(()=>({}));if(!r.ok||!data.access_token)throw new Error(data.error_description||data.error||data.message||"Klaviyo token exchange failed");await saveConnection(userId,"klaviyo",{accessToken:data.access_token,refreshToken:data.refresh_token||null,tokenExpiresAt:parseExpiry(data.expires_in),metadata:{scope:data.scope||klaviyoScopes(),tokenType:data.token_type||null,expiresIn:data.expires_in||null}});req.session.klaviyoOAuthState=null;req.session.klaviyoCodeVerifier=null;res.redirect("/dashboard?klaviyo_connected=1")}catch(e){res.redirect(`/dashboard?klaviyo_error=${encodeURIComponent(e.message)}`)}});
app.get("/api/klaviyo/status",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;const conn=await getConnection(user.id,"klaviyo");res.json({connected:Boolean(conn&&(conn.access_token||conn.refresh_token)),setupRequired:Boolean(conn?.metadata?.requiresSetup),estimatedMonthlySpend:conn?.metadata?.estimatedMonthlySpend||null,spendCurrency:conn?.metadata?.spendCurrency||null,updatedAt:conn?.updated_at||null})}catch(e){res.status(500).json({error:e.message})}});
app.post("/api/klaviyo/settings",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;const conn=await getConnection(user.id,"klaviyo");if(!conn)return res.status(404).json({error:"klaviyo not connected"});const estimatedMonthlySpend=Number(req.body.estimatedMonthlySpend);const spendCurrency=String(req.body.spendCurrency||"").toUpperCase();if(!estimatedMonthlySpend||estimatedMonthlySpend<=0)return res.status(400).json({error:"estimatedMonthlySpend is required"});if(!["USD","TRY","EUR"].includes(spendCurrency))return res.status(400).json({error:"spendCurrency must be USD, TRY or EUR"});const metadata={...(conn.metadata||{}),estimatedMonthlySpend,spendCurrency,requiresSetup:false,setupCompletedAt:new Date().toISOString()};const {error}=await supabaseAdmin.from("platform_connections").update({metadata,updated_at:new Date().toISOString()}).eq("user_id",user.id).eq("platform","klaviyo");if(error)throw error;res.json({ok:true,platform:"klaviyo",estimatedMonthlySpend,spendCurrency,setupRequired:false})}catch(e){res.status(500).json({error:e.message})}});
app.get("/api/klaviyo/campaigns",async(req,res)=>{try{const result=await requireConnection(req,res,"klaviyo");if(!result)return;const{conn}=result;const range=String(req.query.date_range||req.query.dateRange||"last_7d");const w=klaviyoDateWindow(range,req.query.start_date,req.query.end_date);const channel=String(req.query.channel||"email");const filter=`equals(messages.channel,\'${channel}\'),greater-or-equal(scheduled_at,${w.start}),less-or-equal(scheduled_at,${w.end})`;const data=await klaviyoFetch(conn,`/api/campaigns/?filter=${encodeURIComponent(filter)}`);res.json(data)}catch(e){res.status(500).json({error:e.message})}});
app.get("/api/klaviyo/insights",async(req,res)=>{try{const result=await requireConnection(req,res,"klaviyo");if(!result)return;const{conn}=result;if(conn.metadata?.requiresSetup)return res.status(400).json({error:"Klaviyo setup required. Please enter estimated monthly spend and currency."});const range=String(req.query.date_range||req.query.dateRange||"last_7d");const w=klaviyoDateWindow(range,req.query.start_date,req.query.end_date);const campaignLimit=Math.min(Number(req.query.limit||25),50);const channel=String(req.query.channel||"email");const filter=`equals(messages.channel,\'${channel}\'),greater-or-equal(scheduled_at,${w.start}),less-or-equal(scheduled_at,${w.end})`;const campaignsData=await klaviyoFetch(conn,`/api/campaigns/?filter=${encodeURIComponent(filter)}`);const campaigns=(campaignsData.data||[]).slice(0,campaignLimit);let placedOrderMetricId=req.query.placedOrderMetricId||process.env.KLAVIYO_PLACED_ORDER_METRIC_ID||null;if(!placedOrderMetricId)placedOrderMetricId=await getKlaviyoMetricId(conn,["Placed Order","Placed order","Order Placed"]);
const rows=[];const errors=[];
for(const campaign of campaigns){
  try{
    let report=null;
    if(placedOrderMetricId){
      const body={data:{type:"campaign-values-report",attributes:{timeframe:{start:w.start,end:w.end},conversion_metric_id:placedOrderMetricId,filter:`equals(campaign_id,"${campaign.id}")`,statistics:["delivered","opens","clicks","click_rate","conversion_value","conversions"]}}};
      report=await klaviyoFetch(conn,"/api/campaign-values-reports/",{method:"POST",body:JSON.stringify(body)});
    }
    rows.push(normalizeKlaviyoInsight({campaign,report,settings:conn.metadata||{},window:w}));
  }catch(err){errors.push({campaign_id:campaign.id,error:err.message});rows.push(normalizeKlaviyoInsight({campaign,report:null,settings:conn.metadata||{},window:w}))}
}
res.json({platform:"Klaviyo",level:"campaign",date_range:range,start:w.start,end:w.end,selected_day_count:w.selected_day_count,rows,rawCount:rows.length,errors,metadata:{campaignCount:campaigns.length,placedOrderMetricId:placedOrderMetricId||null}})
}catch(e){res.status(500).json({error:e.message})}});

app.get("/api/unified/status",async(req,res)=>{const user=await requireUser(req,res);if(!user)return;const meta=await connectionStatus(user.id,"meta"),google=await connectionStatus(user.id,"google"),pinterest=await connectionStatus(user.id,"pinterest"),klaviyo=await connectionStatus(user.id,"klaviyo");res.json({meta:meta.connected,google:google.connected,pinterest:pinterest.connected,klaviyo:klaviyo.connected,tiktok:false,tiktokStatus:"pending_verification",sources:{meta:meta.source,google:google.source,pinterest:pinterest.source,klaviyo:klaviyo.source},updatedAt:{meta:meta.updatedAt,google:google.updatedAt,pinterest:pinterest.updatedAt,klaviyo:klaviyo.updatedAt}})});
app.get("/api/debug/connections",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;const{data,error}=await supabaseAdmin.from("platform_connections").select("platform,connected,account_id,account_name,token_expires_at,metadata,updated_at").eq("user_id",user.id).order("updated_at",{ascending:false});if(error)throw error;res.json({connections:data||[]})}catch(e){res.status(500).json({error:e.message})}});
app.post("/api/connections/:platform/disconnect",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;const platform=req.params.platform;if(!["meta","google","pinterest","klaviyo"].includes(platform))return res.status(400).json({error:"Unsupported platform"});const{error}=await supabaseAdmin.from("platform_connections").update({connected:false,updated_at:new Date().toISOString()}).eq("user_id",user.id).eq("platform",platform);if(error)throw error;res.json({ok:true,platform,connected:false})}catch(e){res.status(500).json({error:e.message})}});
async function upsertAdAccount(userId,platform,account){if(!supabaseAdmin||!userId)return;const row={user_id:userId,platform,platform_business_id:account.business_id||null,platform_account_id:account.id||account.customerId||account.account_id,account_name:account.name||account.descriptiveName||account.account_name||null,currency:account.currency||account.currency_code||null,timezone:account.timezone_name||account.timezone||null,status:String(account.account_status||account.status||""),metadata:account,updated_at:new Date().toISOString()};if(!row.platform_account_id)return;await supabaseAdmin.from("platform_ad_accounts").upsert(row,{onConflict:"user_id,platform,platform_account_id"})}
async function metaGraph(pathname,params,token){const url=new URL(`https://graph.facebook.com/${META_GRAPH_VERSION}${pathname}`);Object.entries(params||{}).forEach(([k,v])=>{if(v!==undefined&&v!==null&&v!=="")url.searchParams.set(k,v)});url.searchParams.set("access_token",token);const r=await fetch(url);const data=await r.json();if(!r.ok)throw new Error(data.error?.message||JSON.stringify(data));return data}
app.get("/api/meta/adaccounts",async(req,res)=>{try{const result=await requireConnection(req,res,"meta");if(!result)return;const{user,conn}=result;const data=await metaGraph("/me/adaccounts",{fields:"id,name,account_status,currency,timezone_name",limit:"100"},conn.access_token);const accounts=data.data||[];for(const account of accounts)await upsertAdAccount(user.id,"meta",account);res.json({platform:"meta",accounts})}catch(e){res.status(500).json({error:e.message})}});
function actionValue(list,type){const f=Array.isArray(list)?list.find(x=>x.action_type===type):null;return f?Number(f.value||0):null}
function normalizeMetaInsight(row,level){const a=row.actions||[],c=row.cost_per_action_type||[],v=row.action_values||[];const addToCart=actionValue(a,"add_to_cart")??actionValue(a,"omni_add_to_cart");const checkout=actionValue(a,"initiate_checkout")??actionValue(a,"checkout")??actionValue(a,"omni_initiated_checkout");const purchase=actionValue(a,"purchase")??actionValue(a,"omni_purchase");const purchaseValue=actionValue(v,"purchase")??actionValue(v,"omni_purchase");const addToCartValue=actionValue(v,"add_to_cart")??actionValue(v,"omni_add_to_cart");const checkoutValue=actionValue(v,"initiate_checkout")??actionValue(v,"checkout")??actionValue(v,"omni_initiated_checkout");const abandoned=Math.max((checkout||0)-(purchase||0),0);const spend=Number(row.spend||0);const revenue=purchaseValue??null;return{platform:"Meta",level,campaign_id:row.campaign_id||null,campaign_name:row.campaign_name||null,campaign_status:row.campaign_status||null,adset_id:row.adset_id||null,adset_name:row.adset_name||null,ad_id:row.ad_id||null,ad_name:row.ad_name||null,currency:row.account_currency||null,impressions:Number(row.impressions||0),reach:Number(row.reach||0),clicks:Number(row.clicks||0),ctr:row.ctr!==undefined?Number(row.ctr):null,cpc:row.cpc!==undefined?Number(row.cpc):null,spend,link_clicks:actionValue(a,"link_click"),landing_page_views:actionValue(a,"landing_page_view"),omni_landing_page_views:actionValue(a,"omni_landing_page_view"),page_engagement:actionValue(a,"page_engagement"),post_engagement:actionValue(a,"post_engagement"),video_views:actionValue(a,"video_view"),add_to_cart:addToCart,checkout,purchase,purchases:purchase,abandoned,add_to_cart_value:addToCartValue,checkout_value:checkoutValue,purchase_value:purchaseValue,cost_per_link_click:actionValue(c,"link_click"),cost_per_landing_page_view:actionValue(c,"landing_page_view"),cost_per_page_engagement:actionValue(c,"page_engagement"),cost_per_video_view:actionValue(c,"video_view"),conversion_rate_ranking:row.conversion_rate_ranking||null,sales:revenue,revenue,roas:spend&&spend>0&&revenue!==null?revenue/spend:null,date_start:row.date_start||null,date_stop:row.date_stop||null,raw:row}}
app.get("/api/meta/insights",async(req,res)=>{try{const result=await requireConnection(req,res,"meta");if(!result)return;const{conn}=result;const adAccountId=req.query.adAccountId||req.query.ad_account_id;if(!adAccountId)return res.status(400).json({error:"Missing adAccountId"});const level=["campaign","adset","ad"].includes(String(req.query.level||"campaign"))?String(req.query.level||"campaign"):"campaign";const fields=["campaign_id","campaign_name","account_currency","impressions","reach","clicks","ctr","cpc","spend","actions","action_values","cost_per_action_type","conversion_rate_ranking"];if(level==="adset")fields.splice(2,0,"adset_id","adset_name");if(level==="ad")fields.splice(2,0,"adset_id","adset_name","ad_id","ad_name");const data=await metaGraph(`/${adAccountId}/insights`,{level,date_preset:req.query.date_preset||"last_7d",fields:fields.join(","),limit:req.query.limit||"100"},conn.access_token);res.json({platform:"Meta",level,date_preset:req.query.date_preset||"last_7d",rows:(data.data||[]).map(r=>normalizeMetaInsight(r,level)),paging:data.paging||null})}catch(e){res.status(500).json({error:e.message})}});
function normalizeCustomerId(id){return String(id||"").replace(/-/g,"").trim()}
function googleHeaders(token,loginCustomerId){const h={Authorization:`Bearer ${token}`,"developer-token":process.env.GOOGLE_DEVELOPER_TOKEN||"","Content-Type":"application/json"};if(loginCustomerId)h["login-customer-id"]=normalizeCustomerId(loginCustomerId);return h}
async function googleAdsSearch(userId,customerId,query,loginCustomerId){const token=await getFreshGoogleAccessToken(userId);const clean=normalizeCustomerId(customerId);const r=await fetch(`https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers/${clean}/googleAds:search`,{method:"POST",headers:googleHeaders(token,loginCustomerId),body:JSON.stringify({query})});const data=await r.json();if(!r.ok){const err=new Error(JSON.stringify(data));err.status=r.status;throw err}return data}
function googleDateClause(range){return range==="today"?"segments.date DURING TODAY":range==="yesterday"?"segments.date DURING YESTERDAY":range==="last_30d"?"segments.date DURING LAST_30_DAYS":"segments.date DURING LAST_7_DAYS"}
function googleQuery(level,range){const d=googleDateClause(range);if(level==="adgroup")return `SELECT segments.date, customer.currency_code, campaign.id, campaign.name, ad_group.id, ad_group.name, ad_group.status, metrics.impressions, metrics.clicks, metrics.invalid_clicks, metrics.ctr, metrics.average_cpc, metrics.cost_micros, metrics.conversions, metrics.all_conversions, metrics.conversions_value, metrics.conversions_from_interactions_rate FROM ad_group WHERE ${d} ORDER BY metrics.cost_micros DESC LIMIT 100`;if(level==="ad")return `SELECT segments.date, customer.currency_code, campaign.id, campaign.name, ad_group.id, ad_group.name, ad_group_ad.ad.id, ad_group_ad.status, metrics.impressions, metrics.clicks, metrics.invalid_clicks, metrics.ctr, metrics.average_cpc, metrics.cost_micros, metrics.conversions, metrics.all_conversions, metrics.conversions_value, metrics.conversions_from_interactions_rate FROM ad_group_ad WHERE ${d} ORDER BY metrics.cost_micros DESC LIMIT 100`;return `SELECT segments.date, customer.currency_code, campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, bidding_strategy.type, metrics.impressions, metrics.clicks, metrics.invalid_clicks, metrics.ctr, metrics.average_cpc, metrics.cost_micros, metrics.conversions, metrics.all_conversions, metrics.conversions_value, metrics.conversions_from_interactions_rate FROM campaign WHERE ${d} ORDER BY metrics.cost_micros DESC LIMIT 100`}
function googleConversionBreakdownQuery(level,range){const d=googleDateClause(range);if(level==="adgroup")return `SELECT campaign.id, campaign.name, ad_group.id, ad_group.name, segments.conversion_action, segments.conversion_action_name, segments.conversion_action_category, metrics.conversions, metrics.conversions_value FROM ad_group WHERE ${d} AND metrics.conversions > 0 LIMIT 1000`;if(level==="ad")return `SELECT campaign.id, campaign.name, ad_group.id, ad_group.name, ad_group_ad.ad.id, segments.conversion_action, segments.conversion_action_name, segments.conversion_action_category, metrics.conversions, metrics.conversions_value FROM ad_group_ad WHERE ${d} AND metrics.conversions > 0 LIMIT 1000`;return `SELECT campaign.id, campaign.name, segments.conversion_action, segments.conversion_action_name, segments.conversion_action_category, metrics.conversions, metrics.conversions_value FROM campaign WHERE ${d} AND metrics.conversions > 0 LIMIT 1000`}
function googleRowKey(row,level){const c=row.campaign||{},ag=row.adGroup||row.ad_group||{},aga=row.adGroupAd||row.ad_group_ad||{};if(level==="ad")return String(c.id||"")+"|"+String(ag.id||"")+"|"+String(nested(aga,"ad.id")||"");if(level==="adgroup")return String(c.id||"")+"|"+String(ag.id||"");return String(c.id||"")}
function googleConversionName(row){const s=row.segments||{},ca=row.conversionAction||row.conversion_action||{};return String(s.conversionActionName||s.conversion_action_name||ca.name||ca.resourceName||ca.resource_name||"").toLowerCase()}
function googleConversionCategory(row){const s=row.segments||{},ca=row.conversionAction||row.conversion_action||{};return String(s.conversionActionCategory||s.conversion_action_category||ca.category||"").toUpperCase()}
function googleMetricNumber(row,key){const m=row.metrics||{};return Number(m[key]??m[key.replace(/[A-Z]/g,x=>"_"+x.toLowerCase())]??0)}
function mergeGoogleConversionActions(performanceRows,breakdownRows,level){const byKey=new Map();for(const r of performanceRows)byKey.set(googleRowKey(r,level),r);for(const b of breakdownRows||[]){const key=googleRowKey(b,level);const target=byKey.get(key);if(!target)continue;target.__conversion_actions=target.__conversion_actions||[];target.__conversion_actions.push({name:googleConversionName(b),category:googleConversionCategory(b),conversions:googleMetricNumber(b,"conversions"),conversions_value:googleMetricNumber(b,"conversionsValue")});}return performanceRows}
function microsToMoney(v){return v===null||v===undefined||v===""?null:Number(v)/1000000}
function nested(o,p){return p.split(".").reduce((a,k)=>a&&a[k]!==undefined?a[k]:undefined,o)}
function googleMatchConversion(actions,kind){const list=Array.isArray(actions)?actions:[];const cfg={add_to_cart:{categories:["ADD_TO_CART"],names:["add_to_cart","add to cart","cart"]},checkout:{categories:["BEGIN_CHECKOUT"],names:["begin_checkout","checkout","start_checkout","started_checkout"]},purchase:{categories:["PURCHASE"],names:["purchase","placed_order","order","sale"]}}[kind];if(!cfg)return null;let total=0,value=0,found=false;for(const a of list){const name=String(a.name||"").toLowerCase();const cat=String(a.category||"").toUpperCase();const matched=cfg.categories.includes(cat)||cfg.names.some(n=>name.includes(n));if(matched){found=true;total+=Number(a.conversions||0);value+=Number(a.conversions_value||0)}}return found?{count:total,value}:null}
function normalizeGoogleInsight(row,level){const m=row.metrics||{},c=row.campaign||{},ag=row.adGroup||row.ad_group||{},aga=row.adGroupAd||row.ad_group_ad||{},cust=row.customer||{},seg=row.segments||{};const spend=microsToMoney(m.costMicros??m.cost_micros),cpc=microsToMoney(m.averageCpc??m.average_cpc),genericRevenue=Number(m.conversionsValue??m.conversions_value??0),genericConversions=Number(m.conversions??0),invalidClicks=Number(m.invalidClicks??m.invalid_clicks??0),clicks=Number(m.clicks||0),validClicks=Math.max(clicks-invalidClicks,0);const actions=row.__conversion_actions||[];const atc=googleMatchConversion(actions,"add_to_cart"),chk=googleMatchConversion(actions,"checkout"),pur=googleMatchConversion(actions,"purchase");const addToCart=atc?atc.count:null,checkout=chk?chk.count:null,purchase=pur?pur.count:null,purchaseValue=pur?pur.value:genericRevenue||null;const abandoned=checkout!==null&&purchase!==null?Math.max((checkout||0)-(purchase||0),0):null;const sales=purchaseValue;const roas=spend&&spend>0&&sales!==null?sales/spend:null;const acos=sales&&sales>0&&spend!==null?(spend/sales)*100:null;return{platform:"Google",level,date:seg.date||null,campaign_id:c.id||null,campaign_name:c.name||null,campaign_status:c.status||null,channel_type:c.advertisingChannelType||c.advertising_channel_type||null,bidding_strategy_type:nested(row,"biddingStrategy.type")||nested(row,"bidding_strategy.type")||null,adgroup_id:ag.id||null,adgroup_name:ag.name||null,adgroup_status:ag.status||null,ad_id:nested(aga,"ad.id")||null,ad_status:aga.status||null,currency:cust.currencyCode||cust.currency_code||null,impressions:Number(m.impressions||0),clicks,ad_clicks:clicks,link_clicks:clicks,landing_page_views:null,traffic_score:null,real_cpc:null,invalid_clicks:invalidClicks,valid_clicks:validClicks,ctr:m.ctr!==undefined?Number(m.ctr)*100:null,cpc,spend,conversions:genericConversions,all_conversions:Number(m.allConversions??m.all_conversions??0),add_to_cart:addToCart,checkout,purchase,purchases:purchase,abandoned,purchase_value:purchaseValue,revenue:sales,sales,conversion_rate:m.conversionsFromInteractionsRate!==undefined?Number(m.conversionsFromInteractionsRate)*100:m.conversions_from_interactions_rate!==undefined?Number(m.conversions_from_interactions_rate)*100:null,cvr:clicks&&purchase!==null?(purchase/clicks)*100:null,roas,acos,conversion_actions:actions,raw:row}}
app.get("/api/google/customers",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;const token=await getFreshGoogleAccessToken(user.id);const r=await fetch(`https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers:listAccessibleCustomers`,{method:"GET",headers:googleHeaders(token)});const data=await r.json();if(!r.ok)return res.status(r.status).json({error:JSON.stringify(data),status:r.status});const customers=(data.resourceNames||[]).map(resourceName=>({resourceName,customerId:String(resourceName).replace("customers/","")}));for(const c of customers)await upsertAdAccount(user.id,"google",{id:c.customerId,customerId:c.customerId,name:c.customerId,status:"accessible"});res.json({customers})}catch(e){res.status(500).json({error:e.message})}});
app.get("/api/google/insights",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;const customerId=req.query.customerId||req.query.customer_id;if(!customerId)return res.status(400).json({error:"Missing customerId"});const level=["campaign","adgroup","ad"].includes(String(req.query.level||"campaign"))?String(req.query.level||"campaign"):"campaign";const dateRange=String(req.query.date_range||req.query.dateRange||"last_7d");const loginCustomerId=req.query.loginCustomerId||req.query.login_customer_id||"";const query=googleQuery(level,dateRange);const data=await googleAdsSearch(user.id,customerId,query,loginCustomerId);let breakdownData={results:[]},breakdownError=null;try{breakdownData=await googleAdsSearch(user.id,customerId,googleConversionBreakdownQuery(level,dateRange),loginCustomerId)}catch(err){breakdownError=err.message}const performanceRows=data.results||[];const mergedRows=mergeGoogleConversionActions(performanceRows,breakdownData.results||[],level);res.json({platform:"Google",level,customerId:normalizeCustomerId(customerId),loginCustomerId:loginCustomerId?normalizeCustomerId(loginCustomerId):null,date_range:dateRange,rows:mergedRows.map(r=>normalizeGoogleInsight(r,level)),rawCount:mergedRows.length,conversionBreakdownCount:breakdownData.results?breakdownData.results.length:0,conversionBreakdownError:breakdownError,fieldMask:data.fieldMask||null,conversionFieldMask:breakdownData.fieldMask||null,requestId:data.requestId||null,nextPageToken:data.nextPageToken||null})}catch(e){res.status(e.status||500).json({error:e.message})}});
async function pinterestFetch(conn,endpoint,options={}){const r=await fetch(`${PINTEREST_API_BASE}${endpoint}`,{...options,headers:{Authorization:`Bearer ${conn.access_token}`,"Content-Type":"application/json",...(options.headers||{})}});const text=await r.text();let data;try{data=text?JSON.parse(text):{}}catch{data={raw:text}}if(!r.ok)throw new Error(data.message||text||`Pinterest API error ${r.status}`);return data}

function dateRangeToPinterestDates(range){
  const end=new Date();
  const start=new Date(end);
  if(range==="today"){}
  else if(range==="yesterday"){start.setDate(start.getDate()-1);end.setDate(end.getDate()-1)}
  else if(range==="last_30d")start.setDate(start.getDate()-29);
  else start.setDate(start.getDate()-6);
  const fmt=d=>d.toISOString().slice(0,10);
  return{start_date:fmt(start),end_date:fmt(end)};
}
function num(v){return v===null||v===undefined||v===""?0:Number(v)||0}
function maybeNum(v){return v===null||v===undefined||v===""?null:Number(v)}
function microToMoneyPinterest(v){const n=maybeNum(v);return n===null?null:n/1000000}
function firstMetric(m,...keys){for(const k of keys){if(m&&m[k]!==undefined&&m[k]!==null)return m[k]}return null}
function sumMetrics(m,...keys){let found=false,total=0;for(const k of keys){const v=firstMetric(m,k);if(v!==null&&v!==undefined&&v!==""){found=true;total+=Number(v)||0}}return found?total:null}
function normalizePinterestInsight(row,level){
  const m=row.metrics||row||{};
  const campaign=row.campaign||{};
  const adGroup=row.ad_group||row.adGroup||{};
  const ad=row.ad||{};
  const customer=row.customer||{};
  const seg=row.segments||{};
  const impressions=num(firstMetric(m,"impressions","IMPRESSION_1","PAID_IMPRESSION","TOTAL_IMPRESSION"));
  const clicks=num(firstMetric(m,"clicks","CLICKTHROUGH_1","CLICKTHROUGH_1_GROSS","TOTAL_CLICKTHROUGH"));
  const outboundClicks=maybeNum(firstMetric(m,"OUTBOUND_CLICK_1","OUTBOUND_CLICK_2","OUTBOUND_CLICK","outbound_clicks","outboundClicks"));
  const lpv=maybeNum(firstMetric(m,"TOTAL_PAGE_VISIT","TOTAL_WEB_SESSIONS","WEB_SESSIONS_1","WEB_SESSIONS_2","LANDING_PAGE_VIEW","landing_page_views","landingPageViews"));
  const spend=microToMoneyPinterest(firstMetric(m,"spend_in_micro_dollar","SPEND_IN_MICRO_DOLLAR"));
  const addToCart=firstMetric(m,"TOTAL_ADD_TO_CART")!==null?maybeNum(firstMetric(m,"TOTAL_ADD_TO_CART")):sumMetrics(m,"TOTAL_CLICK_ADD_TO_CART","TOTAL_VIEW_ADD_TO_CART");
  const checkout=maybeNum(firstMetric(m,"TOTAL_CHECKOUT","TOTAL_WEB_CHECKOUT","TOTAL_CLICK_CHECKOUT","TOTAL_VIEW_CHECKOUT"));
  const purchase=maybeNum(firstMetric(m,"TOTAL_WEB_CHECKOUT","TOTAL_CHECKOUT","TOTAL_CONVERSIONS"));
  const addToCartValue=microToMoneyPinterest(firstMetric(m,"TOTAL_ADD_TO_CART_VALUE_IN_MICRO_DOLLAR"));
  const checkoutValue=microToMoneyPinterest(firstMetric(m,"TOTAL_CHECKOUT_VALUE_IN_MICRO_DOLLAR","TOTAL_WEB_CHECKOUT_VALUE_IN_MICRO_DOLLAR"));
  const purchaseValue=microToMoneyPinterest(firstMetric(m,"TOTAL_WEB_CHECKOUT_VALUE_IN_MICRO_DOLLAR","TOTAL_CHECKOUT_VALUE_IN_MICRO_DOLLAR","TOTAL_PURCHASE_VALUE_IN_MICRO_DOLLAR","TOTAL_CONVERSIONS_VALUE_IN_MICRO_DOLLAR"));
  const abandoned=checkout!==null&&purchase!==null?Math.max((checkout||0)-(purchase||0),0):null;
  const cartAbandoned=addToCart!==null&&checkout!==null?Math.max((addToCart||0)-(checkout||0),0):null;
  const cvr=outboundClicks&&outboundClicks>0&&purchase!==null?(purchase/outboundClicks)*100:null;
  const roas=spend&&spend>0&&purchaseValue!==null?purchaseValue/spend:null;
  const acos=purchaseValue&&purchaseValue>0&&spend!==null?spend/purchaseValue:null;
  return{platform:"Pinterest",level,date:seg.date||row.date||null,report_level:row.report_level||null,campaign_id:campaign.id||row.campaign_id||null,campaign_name:campaign.name||row.campaign_name||null,campaign_status:campaign.status||null,objective_type:campaign.objective_type||campaign.objectiveType||null,adgroup_id:adGroup.id||row.ad_group_id||null,adgroup_name:adGroup.name||row.ad_group_name||null,adgroup_status:adGroup.status||null,ad_id:ad.id||row.ad_id||null,pin_id:ad.pin_id||ad.pinId||null,ad_status:ad.status||null,currency:customer.currency||customer.currency_code||null,impressions,reach:null,clicks,link_clicks:outboundClicks,outbound_clicks:outboundClicks,landing_page_views:lpv,ctr:firstMetric(m,"ctr","CTR")!==null?Number(firstMetric(m,"ctr","CTR"))*100:null,cpc:firstMetric(m,"average_cpc","CPC_IN_MICRO_DOLLAR")!==null?(firstMetric(m,"average_cpc")!==null?Number(firstMetric(m,"average_cpc")):microToMoneyPinterest(firstMetric(m,"CPC_IN_MICRO_DOLLAR"))):null,spend,save_rate:maybeNum(firstMetric(m,"save_rate","SAVE_RATE")),add_to_cart:addToCart,checkout,purchase,purchases:purchase,abandoned,cart_abandoned:cartAbandoned,add_to_cart_value:addToCartValue,checkout_value:checkoutValue,purchase_value:purchaseValue,sales:purchaseValue,revenue:purchaseValue,cvr,roas,acos,raw:row};
}
function pinterestAnalyticsEndpoint(adAccountId,level){
  if(level==="adgroup")return `/ad_accounts/${adAccountId}/ad_groups/analytics`;
  if(level==="ad")return `/ad_accounts/${adAccountId}/ads/analytics`;
  if(level==="account")return `/ad_accounts/${adAccountId}/analytics`;
  return `/ad_accounts/${adAccountId}/campaigns/analytics`;
}
function normalizePinterestRows(data,level){
  const rows=Array.isArray(data)?data:Array.isArray(data.items)?data.items:Array.isArray(data.data)?data.data:Array.isArray(data.results)?data.results:[];
  return rows.map(r=>normalizePinterestInsight(r,level));
}
app.get("/api/pinterest/insights",async(req,res)=>{try{const result=await requireConnection(req,res,"pinterest");if(!result)return;const{conn}=result;const adAccountId=req.query.adAccountId||req.query.ad_account_id;if(!adAccountId)return res.status(400).json({error:"Missing adAccountId"});const level=["account","campaign","adgroup","ad"].includes(String(req.query.level||"campaign"))?String(req.query.level||"campaign"):"campaign";const dateRange=String(req.query.date_range||req.query.dateRange||"last_7d");const dates=req.query.start_date&&req.query.end_date?{start_date:String(req.query.start_date),end_date:String(req.query.end_date)}:dateRangeToPinterestDates(dateRange);const defaultColumns="SPEND_IN_MICRO_DOLLAR,IMPRESSION_1,CLICKTHROUGH_1,OUTBOUND_CLICK_1,TOTAL_PAGE_VISIT,TOTAL_CLICK_ADD_TO_CART,TOTAL_VIEW_ADD_TO_CART,TOTAL_CHECKOUT,TOTAL_CHECKOUT_VALUE_IN_MICRO_DOLLAR,TOTAL_WEB_CHECKOUT,TOTAL_WEB_CHECKOUT_VALUE_IN_MICRO_DOLLAR";const params=new URLSearchParams({start_date:dates.start_date,end_date:dates.end_date,granularity:String(req.query.granularity||"DAY"),columns:String(req.query.columns||defaultColumns),click_window_days:String(req.query.click_window_days||30),engagement_window_days:String(req.query.engagement_window_days||30),view_window_days:String(req.query.view_window_days||1),conversion_report_time:String(req.query.conversion_report_time||"TIME_OF_AD_ACTION")});const campaignIds=req.query.campaign_ids||req.query.campaignIds||"";const adGroupIds=req.query.ad_group_ids||req.query.adGroupIds||"";const adIds=req.query.ad_ids||req.query.adIds||"";if(level==="campaign"&&!campaignIds)return res.status(400).json({error:"Parameter 'campaign_ids' is required."});if(level==="adgroup"&&!adGroupIds)return res.status(400).json({error:"Parameter 'ad_group_ids' is required."});if(level==="ad"&&!adIds)return res.status(400).json({error:"Parameter 'ad_ids' is required."});if(campaignIds)params.set("campaign_ids",String(campaignIds));if(adGroupIds)params.set("ad_group_ids",String(adGroupIds));if(adIds)params.set("ad_ids",String(adIds));const endpoint=`${pinterestAnalyticsEndpoint(adAccountId,level)}?${params.toString()}`;const data=await pinterestFetch(conn,endpoint);const normalizedRows=normalizePinterestRows(data,level);res.json({platform:"Pinterest",level,adAccountId,date_range:dateRange,start_date:dates.start_date,end_date:dates.end_date,campaign_ids:campaignIds||null,ad_group_ids:adGroupIds||null,ad_ids:adIds||null,rows:normalizedRows,rawCount:normalizedRows.length,raw:data})}catch(e){res.status(500).json({error:e.message})}});

app.get("/api/pinterest/adaccounts",async(req,res)=>{try{const result=await requireConnection(req,res,"pinterest");if(!result)return;const{user,conn}=result;const data=await pinterestFetch(conn,"/ad_accounts");const accounts=data.items||[];for(const account of accounts)await upsertAdAccount(user.id,"pinterest",{id:account.id,name:account.name,currency:account.currency,status:"accessible",...account});res.json(data)}catch(e){res.status(500).json({error:e.message})}});
app.get("/api/accounts",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;const{data,error}=await supabaseAdmin.from("platform_ad_accounts").select("*").eq("user_id",user.id).order("platform",{ascending:true});if(error)throw error;res.json({accounts:data||[]})}catch(e){res.status(500).json({error:e.message})}});


// ===== D.2A.1 META CONNECT / DISCONNECT =====
app.get("/api/platform/meta/status",async(req,res)=>{
  try{
    const user=await requireUser(req,res);
    if(!user)return;

    const conn=await getConnection(user.id,"meta");
    res.json({
      state: conn ? "CONNECTED" : "NOT_CONNECTED"
    });
  }catch(e){
    res.status(500).json({error:e.message});
  }
});

app.post("/api/platform/meta/disconnect",async(req,res)=>{
  try{
    const user=await requireUser(req,res);
    if(!user)return;

    const {error}=await supabaseAdmin
      .from("platform_connections")
      .update({
        connected:false,
        updated_at:new Date().toISOString()
      })
      .eq("user_id",user.id)
      .eq("platform","meta");

    if(error)throw error;

    res.json({state:"NOT_CONNECTED"});
  }catch(e){
    res.status(500).json({error:e.message});
  }
});
// ===== END D.2A.1 META CONNECT / DISCONNECT =====


// ===== D.2A.2 GOOGLE CONNECT / DISCONNECT =====
app.get("/api/platform/google/status",async(req,res)=>{
  try{
    const user=await requireUser(req,res);
    if(!user)return;

    const conn=await getConnection(user.id,"google");
    res.json({
      state: conn ? "CONNECTED" : "NOT_CONNECTED"
    });
  }catch(e){
    res.status(500).json({error:e.message});
  }
});

app.post("/api/platform/google/disconnect",async(req,res)=>{
  try{
    const user=await requireUser(req,res);
    if(!user)return;

    const {error}=await supabaseAdmin
      .from("platform_connections")
      .update({
        connected:false,
        updated_at:new Date().toISOString()
      })
      .eq("user_id",user.id)
      .eq("platform","google");

    if(error)throw error;

    res.json({state:"NOT_CONNECTED"});
  }catch(e){
    res.status(500).json({error:e.message});
  }
});
// ===== END D.2A.2 GOOGLE CONNECT / DISCONNECT =====


// ===== D.2B.1 KLAVIYO CONNECT / DISCONNECT + ESTIMATED MONTHLY SPEND =====
app.get("/api/platform/klaviyo/status",async(req,res)=>{
  try{
    const user=await requireUser(req,res);
    if(!user)return;

    const conn=await getConnection(user.id,"klaviyo");
    const estimated=conn?.metadata?.estimated_monthly_spend||null;

    res.json({
      state: conn ? "CONNECTED" : "NOT_CONNECTED",
      estimated_monthly_spend: estimated
    });
  }catch(e){
    res.status(500).json({error:e.message});
  }
});

app.post("/api/platform/klaviyo/disconnect",async(req,res)=>{
  try{
    const user=await requireUser(req,res);
    if(!user)return;

    const {error}=await supabaseAdmin
      .from("platform_connections")
      .update({
        connected:false,
        updated_at:new Date().toISOString()
      })
      .eq("user_id",user.id)
      .eq("platform","klaviyo");

    if(error)throw error;

    res.json({state:"NOT_CONNECTED"});
  }catch(e){
    res.status(500).json({error:e.message});
  }
});

app.post("/api/platform/klaviyo/estimated-spend",async(req,res)=>{
  try{
    const user=await requireUser(req,res);
    if(!user)return;

    const conn=await getConnection(user.id,"klaviyo");
    if(!conn)return res.status(404).json({error:"Klaviyo not connected"});

    const amount=Number(req.body?.amount);
    const currency=String(req.body?.currency||"").trim().toUpperCase();

    if(!amount||amount<=0)return res.status(400).json({error:"Amount is required."});
    if(!["USD","TRY","EUR","GBP"].includes(currency))return res.status(400).json({error:"Currency is required."});

    const metadata={
      ...(conn.metadata||{}),
      estimated_monthly_spend:{
        amount,
        currency
      }
    };

    const {error}=await supabaseAdmin
      .from("platform_connections")
      .update({
        metadata,
        updated_at:new Date().toISOString()
      })
      .eq("user_id",user.id)
      .eq("platform","klaviyo");

    if(error)throw error;

    res.json({
      message:"Saved",
      estimated_monthly_spend:metadata.estimated_monthly_spend
    });
  }catch(e){
    res.status(500).json({error:e.message});
  }
});
// ===== END D.2B.1 KLAVIYO CONNECT / DISCONNECT + ESTIMATED MONTHLY SPEND =====

// ===== PHASE C ACCOUNT MANAGEMENT API =====
async function syncPublicUserFromAuth(user){
  if(!supabaseAdmin||!user?.id)throw new Error("Supabase not configured or user missing");
  const authEmail=user.email||null;
  const metaName=user.user_metadata?.name||user.user_metadata?.full_name||null;
  const now=new Date().toISOString();

  const {data:existing,error:selectError}=await supabaseAdmin
    .from("users")
    .select("id,email,name,account_currency")
    .eq("id",user.id)
    .maybeSingle();

  if(selectError)throw selectError;

  if(!existing){
    const {error:insertError}=await supabaseAdmin
      .from("users")
      .insert({id:user.id,email:authEmail,name:metaName,account_currency:null,updated_at:now});
    if(insertError)throw insertError;
    return {id:user.id,email:authEmail,name:metaName,account_currency:null};
  }

  const patch={updated_at:now};
  let changed=false;

  if(authEmail&&existing.email!==authEmail){
    patch.email=authEmail;
    changed=true;
  }

  if(changed){
    const {data,error}=await supabaseAdmin
      .from("users")
      .update(patch)
      .eq("id",user.id)
      .select("id,email,name,account_currency")
      .maybeSingle();
    if(error)throw error;
    return data;
  }

  return existing;
}

app.get("/api/account/profile",async(req,res)=>{
  try{
    const user=await requireUser(req,res);
    if(!user)return;
    const profile=await syncPublicUserFromAuth(user);
    res.json({profile,authEmail:user.email||null});
  }catch(e){
    res.status(500).json({error:e.message});
  }
});

app.post("/api/account/profile",async(req,res)=>{
  try{
    const user=await requireUser(req,res);
    if(!user)return;

    const name=typeof req.body?.name==="string"?req.body.name.trim():null;
    if(!name)return res.status(400).json({error:"Name is required."});

    await syncPublicUserFromAuth(user);

    const {data,error}=await supabaseAdmin
      .from("users")
      .update({name,updated_at:new Date().toISOString()})
      .eq("id",user.id)
      .select("id,email,name,account_currency")
      .maybeSingle();

    if(error)throw error;
    res.json({profile:data,message:"Saved"});
  }catch(e){
    res.status(500).json({error:e.message});
  }
});
// ===== END PHASE C ACCOUNT MANAGEMENT API =====



// ===== D.1 V2 ACCOUNT CURRENCY REQUIRED =====
const SUPPORTED_ACCOUNT_CURRENCIES=["USD","TRY","EUR","GBP"];
function normalizeAccountCurrency(value){const c=String(value||"").trim().toUpperCase();return SUPPORTED_ACCOUNT_CURRENCIES.includes(c)?c:null}
app.get("/api/account/currency",async(req,res)=>{try{const user=await requireLifecycleAccess(req,res,"dashboard");if(!user)return;await syncPublicUserFromAuth(user.user);const{data,error}=await supabaseAdmin.from("users").select("account_currency").eq("id",user.user.id).maybeSingle();if(error)throw error;res.json({account_currency:data?.account_currency||null,required:!data?.account_currency,supported:SUPPORTED_ACCOUNT_CURRENCIES})}catch(e){res.status(500).json({error:e.message})}});
app.post("/api/account/currency",async(req,res)=>{try{const user=await requireLifecycleAccess(req,res,"dashboard");if(!user)return;const accountCurrency=normalizeAccountCurrency(req.body?.account_currency);if(!accountCurrency)return res.status(400).json({error:"Please select your account currency."});await syncPublicUserFromAuth(user.user);const{data,error}=await supabaseAdmin.from("users").update({account_currency:accountCurrency,updated_at:new Date().toISOString()}).eq("id",user.user.id).select("id,email,name,account_currency").maybeSingle();if(error)throw error;res.json({profile:data,message:"Saved"})}catch(e){res.status(500).json({error:e.message})}});
// ===== END D.1 V2 ACCOUNT CURRENCY REQUIRED =====

// ===== PHASE C ACCOUNT LIFECYCLE + DELETE MY DATA =====
function normalizeAccountStatus(status){return String(status||"").toLowerCase()}
function getLifecycleAccess(status){const s=normalizeAccountStatus(status);const full=["trial","active"].includes(s);const readonly=["expired","cancelled"].includes(s);const blocked=["suspended","deleted"].includes(s);return{status:s||null,login:full||readonly,dashboard:full||readonly,snapshots:full||readonly,insightHistory:full||readonly,connect:full,manualRefresh:full,refresh:full,dailySync:full,export:full,aiInsights:full,blocked}}
async function getSubscriptionForLifecycle(userId){await expireTrialsIfNeeded();const{data,error}=await supabaseAdmin.from("subscriptions").select("status,trial_end_date").eq("user_id",userId).maybeSingle();if(error)throw error;return data}
async function requireLifecycleAccess(req,res,capability){const user=await requireUser(req,res);if(!user)return null;const sub=await getSubscriptionForLifecycle(user.id);const access=getLifecycleAccess(sub?.status);if(access.blocked||!access[capability]){res.status(403).json({error:"Account access blocked",status:access.status,capability});return null}return{user,sub,access}}
app.get("/api/account/status",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;const sub=await getSubscriptionForLifecycle(user.id);const access=getLifecycleAccess(sub?.status);res.json({status:access.status,access,deleted_at:null,hard_delete_at:null})}catch(e){res.status(500).json({error:e.message})}});
app.post("/api/account/request-delete",async(req,res)=>{try{const result=await requireLifecycleAccess(req,res,"dashboard");if(!result)return;const token=crypto.randomBytes(32).toString("hex");const expiresAt=new Date(Date.now()+30*60*1000).toISOString();const{error}=await supabaseAdmin.from("subscriptions").update({deletion_token:token,deletion_token_expires_at:expiresAt,updated_at:new Date().toISOString()}).eq("user_id",result.user.id).in("status",["trial","active","expired","cancelled"]);if(error)throw error;const proto=req.headers["x-forwarded-proto"]||req.protocol||"https";const host=req.headers["x-forwarded-host"]||req.headers.host;const confirmationUrl=`${proto}://${host}/api/account/confirm-delete?token=${encodeURIComponent(token)}`;res.json({message:"Delete confirmation ready",confirmationUrl,tokenExpiresAt:expiresAt})}catch(e){res.status(500).json({error:e.message})}});
app.get("/api/account/confirm-delete",async(req,res)=>{try{const token=String(req.query.token||"");if(!token)return res.status(400).send("Missing delete token.");const{data,error}=await supabaseAdmin.from("subscriptions").select("user_id,status,deletion_token_expires_at").eq("deletion_token",token).maybeSingle();if(error)throw error;if(!data)return res.status(400).send("Invalid or expired delete token.");if(data.status==="deleted")return res.redirect("/login?account_deleted=1");const expiresAt=data.deletion_token_expires_at?new Date(data.deletion_token_expires_at).getTime():0;if(!expiresAt||expiresAt<Date.now())return res.status(400).send("Invalid or expired delete token.");const deletedAt=new Date();const hardDeleteAt=new Date(deletedAt.getTime()+90*24*60*60*1000);const{error:updateError}=await supabaseAdmin.from("subscriptions").update({status:"deleted",deleted_at:deletedAt.toISOString(),hard_delete_at:hardDeleteAt.toISOString(),deletion_token:null,deletion_token_expires_at:null,updated_at:deletedAt.toISOString()}).eq("user_id",data.user_id);if(updateError)throw updateError;res.redirect("/login?account_deleted=1")}catch(e){res.status(500).send(e.message)}});
// ===== END PHASE C ACCOUNT LIFECYCLE + DELETE MY DATA =====

app.get("/api/tiktok/status",(_,res)=>res.json({connected:false,status:"pending_verification"}));
if(process.env.VERCEL!=="1") app.listen(PORT,()=>console.log(`AdsTable server running on ${PORT}`));
module.exports=app;
