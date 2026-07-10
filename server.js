
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
const GOOGLE_SNAPSHOT_CUSTOMER_ID=process.env.GOOGLE_SNAPSHOT_CUSTOMER_ID||process.env.GOOGLE_TEST_CUSTOMER_ID||"5580593360";
const GOOGLE_SNAPSHOT_LOGIN_CUSTOMER_ID=process.env.GOOGLE_SNAPSHOT_LOGIN_CUSTOMER_ID||process.env.GOOGLE_TEST_LOGIN_CUSTOMER_ID||"5383556660";
const TIKTOK_AUTH_BASE="https://business-api.tiktok.com/portal/auth";
const TIKTOK_API_BASE="https://business-api.tiktok.com/open_api";
const TIKTOK_SANDBOX_API_BASE="https://sandbox-ads.tiktok.com/open_api";
const TIKTOK_REVOKE_ENDPOINT=process.env.TIKTOK_REVOKE_ENDPOINT||`${TIKTOK_API_BASE}/v1.3/oauth2/revoke/`;
const ORGANIC_GOOGLE_REDIRECT_URI=process.env.ORGANIC_GOOGLE_REDIRECT_URI||process.env.GOOGLE_ORGANIC_REDIRECT_URI||process.env.GOOGLE_REDIRECT_URI;
const ORGANIC_GOOGLE_SCOPES=(process.env.ORGANIC_GOOGLE_SCOPES||"https://www.googleapis.com/auth/analytics.readonly https://www.googleapis.com/auth/webmasters.readonly").split(/\s+/).filter(Boolean);
const GA4_ADMIN_API_BASE="https://analyticsadmin.googleapis.com/v1beta";
const GA4_DATA_API_BASE="https://analyticsdata.googleapis.com/v1beta";
const SEARCH_CONSOLE_API_BASE="https://www.googleapis.com/webmasters/v3";
const supabaseAdmin=(process.env.SUPABASE_URL&&process.env.SUPABASE_SERVICE_ROLE_KEY)?createClient(process.env.SUPABASE_URL,process.env.SUPABASE_SERVICE_ROLE_KEY,{auth:{persistSession:false}}):null;
function sendFile(res,file){res.sendFile(path.join(__dirname,"public",file))}
app.get("/",(_,res)=>sendFile(res,"landing.html")); app.get("/dashboard-demo",(_,res)=>sendFile(res,"dashboard-demo.html")); app.get("/login",(_,res)=>sendFile(res,"login.html")); app.get("/signup",(_,res)=>sendFile(res,"signup.html")); app.get("/dashboard",(_,res)=>sendFile(res,"dashboard.html")); app.get("/demo",(_,res)=>sendFile(res,"dashboard-demo.html")); app.get("/privacy",(_,res)=>sendFile(res,"privacy.html")); app.get("/terms",(_,res)=>sendFile(res,"terms.html")); app.get("/data-deletion",(_,res)=>sendFile(res,"data-deletion.html")); app.get("/tiktok-test",(_,res)=>sendFile(res,"tiktok-test.html"));
app.get("/api/public-config",(_,res)=>res.json({supabaseUrl:process.env.SUPABASE_URL||"",supabaseAnonKey:process.env.SUPABASE_ANON_KEY||process.env.SUPABASE_PUBLISHABLE_KEY||""}));
async function getUserFromRequest(req){const a=req.headers.authorization||"";const t=a.startsWith("Bearer ")?a.slice(7):null;if(!t||!supabaseAdmin)return null;const {data,error}=await supabaseAdmin.auth.getUser(t);if(error||!data?.user?.id)return null;return data.user}
async function requireUser(req,res){const u=await getUserFromRequest(req);if(!u){res.status(401).json({error:"Not authenticated"});return null}return u}
async function expireTrialsIfNeeded(){if(!supabaseAdmin)return;const{error}=await supabaseAdmin.rpc("expire_trials");if(error)throw error}
async function getUserSubscription(userId){await expireTrialsIfNeeded();const{data,error}=await supabaseAdmin.from("subscriptions").select("status,trial_end_date").eq("user_id",userId).maybeSingle();if(error)throw error;return data}
function getAccessByStatus(status){const full=["trial","active"].includes(status);const readonly=["expired","cancelled"].includes(status);const blocked=["suspended","deleted"].includes(status);return{dashboard:full||readonly,snapshots:full||readonly,insightHistory:full||readonly,connect:full,manualRefresh:full,dailySync:full,export:full,aiInsights:full,blocked}}
async function requireAccess(req,res,userId,capability){const sub=await getUserSubscription(userId);const access=getAccessByStatus(sub?.status);if(access.blocked||!access[capability]){res.status(403).json({error:"Subscription inactive",status:sub?.status||null});return null}return{sub,access}}
async function requireConnectAccessForOAuth(req,res){const userId=req.query.user_id;if(!userId){res.redirect("/dashboard?error=missing_user_id");return null}const sub=await getUserSubscription(userId);const access=getAccessByStatus(sub?.status);if(access.blocked||!access.connect){res.redirect(`/dashboard?subscription_inactive=1&status=${encodeURIComponent(sub?.status||"")}`);return null}return{userId,sub,access}}
function parseExpiry(s){return s?new Date(Date.now()+Number(s)*1000).toISOString():null}
async function saveConnection(userId,platform,payload){
  if(!supabaseAdmin||!userId)throw new Error("Supabase not configured or user missing");
  const {data:existing,error:existingError}=await supabaseAdmin
    .from("platform_connections")
    .select("account_id,account_name,metadata,access_token,refresh_token,token_expires_at")
    .eq("user_id",userId)
    .eq("platform",platform)
    .maybeSingle();
  if(existingError)throw new Error(existingError.message);
  const row={
    user_id:userId,
    platform,
    access_token:payload.accessToken!==undefined?payload.accessToken:(existing?.access_token||null),
    refresh_token:payload.refreshToken!==undefined?payload.refreshToken:(existing?.refresh_token||null),
    token_expires_at:payload.tokenExpiresAt!==undefined?payload.tokenExpiresAt:(existing?.token_expires_at||null),
    account_id:payload.accountId!==undefined?payload.accountId:(existing?.account_id||null),
    account_name:payload.accountName!==undefined?payload.accountName:(existing?.account_name||null),
    metadata:{...(existing?.metadata||{}),...(payload.metadata||{})},
    connected:true,
    updated_at:new Date().toISOString()
  };
  const {error}=await supabaseAdmin.from("platform_connections").upsert(row,{onConflict:"user_id,platform"});
  if(error)throw new Error(error.message)
}
async function getConnection(userId,platform){if(!supabaseAdmin||!userId)return null;const {data,error}=await supabaseAdmin.from("platform_connections").select("*").eq("user_id",userId).eq("platform",platform).eq("connected",true).maybeSingle();if(error)throw new Error(error.message);return data}
async function connectionStatus(userId,platform){const r=await getConnection(userId,platform).catch(()=>null);return{connected:Boolean(r&&(r.access_token||r.refresh_token)),source:r?"database":"none",updatedAt:r?.updated_at||null}}
async function requireConnection(req,res,platform){const user=await requireUser(req,res);if(!user)return null;const sub=await getSubscriptionForLifecycle(user.id);const access=getLifecycleAccess(sub?.status);if(access.blocked){res.status(403).json({error:"Account access blocked",status:access.status});return null}const conn=await getConnection(user.id,platform);if(!conn){res.status(404).json({error:`${platform} not connected`});return null}return{user,conn}}

// ===== PHASE 1 CONSTITUTION PACK HELPERS =====
const PHASE1_PLATFORM_LIMITS={meta:3,google:3,klaviyo:3,tiktok:3,organic:1};
const PHASE1_REPORTABLE_ACCOUNT_TYPES={
  meta:"meta_ads_account",
  google:"google_ads_customer_account",
  tiktok:"tiktok_advertiser_account",
  klaviyo:"klaviyo_account",
  organic:"organic_property"
};
function phase1ReportableAccountType(platform){return PHASE1_REPORTABLE_ACCOUNT_TYPES[platform]||`${platform}_platform_account`}
function normalizePlatformAccountId(value){return String(value||"").trim()}
function activeOwnershipStatuses(){return ["connected","active"]}

const PASSIVE_LEGACY_PLATFORMS={
  pinterest:{
    status:"passive_legacy",
    label:"Pinterest",
    message:"Pinterest is currently Passive / Legacy. New Pinterest connections are disabled; existing data remains available."
  }
};
function passiveLegacyPlatform(platform){return PASSIVE_LEGACY_PLATFORMS[String(platform||"").toLowerCase()]||null}
function passiveLegacyPlatformStatus(platform){const cfg=passiveLegacyPlatform(platform);return cfg?{platform:String(platform||"").toLowerCase(),status:cfg.status,label:cfg.label,message:cfg.message}:null}
function assertPlatformNotPassiveLegacy(platform){
  const cfg=passiveLegacyPlatform(platform);
  if(!cfg)return;
  const err=new Error(cfg.message);
  err.status=410;
  err.code="PLATFORM_PASSIVE_LEGACY";
  err.platform=String(platform||"").toLowerCase();
  err.platform_status=cfg.status;
  throw err;
}

const DISCONNECT_LIFECYCLE_VERSION="v1";
const BACKFILL_DAYS_ON_RECONNECT=30;

function disconnectReasonText(reason){
  return String(reason||"user_disconnect").trim().slice(0,120)||"user_disconnect";
}

function hasToken(conn){
  return Boolean(conn&&(conn.access_token||conn.refresh_token));
}

async function revokePlatformToken(platform,conn){
  const result={platform,attempted:false,ok:true,provider:"none",error:null,response:null};

  if(!conn||!hasToken(conn))return result;

  try{
    if(platform==="meta"){
      result.attempted=true;
      result.provider="meta";

      if(!conn.access_token){
        throw new Error("Meta revoke requires active user access token before disconnect");
      }

      // Meta app/user authorization revoke must use the connected USER access token.
      // App access token does not remove the user's Business Integration authorization.
      const url=new URL(`https://graph.facebook.com/${META_GRAPH_VERSION}/me/permissions`);
      url.searchParams.set("access_token",conn.access_token);

      const r=await fetch(url,{method:"DELETE"});
      const data=await r.json().catch(()=>({}));
      result.response=data;

      if(!r.ok || data?.success===false){
        throw new Error(data.error?.message||`Meta revoke failed ${r.status}`);
      }

      result.ok=true;
      return result;
    }

    if(platform==="google"&&(conn.refresh_token||conn.access_token)){
      result.attempted=true;
      result.provider="google";
      const url=new URL("https://oauth2.googleapis.com/revoke");
      url.searchParams.set("token",conn.refresh_token||conn.access_token);
      const r=await fetch(url,{method:"POST",headers:{"Content-Type":"application/x-www-form-urlencoded"}});
      result.response={status:r.status};
      if(!r.ok&&r.status!==400)throw new Error(`Google revoke failed ${r.status}`);
      result.ok=true;
      return result;
    }

    if(platform==="tiktok"){
      result.attempted=true;
      result.provider="tiktok";

      const accessToken=conn.access_token;
      if(!accessToken){
        throw new Error("TikTok revoke requires active access token before disconnect");
      }
      if(!tiktokClientId()||!tiktokClientSecret()){
        throw new Error("TikTok revoke requires TIKTOK_CLIENT_ID and TIKTOK_CLIENT_SECRET");
      }

      const r=await fetch(TIKTOK_REVOKE_ENDPOINT,{
        method:"POST",
        headers:{"Content-Type":"application/json"},
        body:JSON.stringify({
          app_id:tiktokClientId(),
          secret:tiktokClientSecret(),
          access_token:accessToken
        })
      });
      const data=await r.json().catch(()=>({status:r.status}));
      result.response=data;

      if(!r.ok || (data.code!==undefined&&data.code!==0)){
        throw new Error(data.message||data.error?.message||`TikTok revoke failed ${r.status}`);
      }

      result.ok=true;
      return result;
    }

    if(platform==="klaviyo"){
      result.attempted=true;
      result.provider=platform;
      result.ok=true;
      result.error="Provider revoke endpoint not configured; internal tokens destroyed";
      return result;
    }

    return result;
  }catch(e){
    result.ok=false;
    result.error=e.message;
    return result;
  }
}

async function stopPlatformSchedules(userId,platform,reason="disconnect"){
  const now=new Date().toISOString();
  const {data,error}=await supabaseAdmin
    .from("snapshot_schedules")
    .update({
      active:false,
      stopped_at:now,
      stop_reason:reason,
      lifecycle_version:DISCONNECT_LIFECYCLE_VERSION,
      updated_at:now
    })
    .eq("user_id",userId)
    .eq("platform",platform)
    .select("id,platform,platform_account_id,active,stopped_at,stop_reason");
  if(error)throw error;
  return data||[];
}

async function failOpenPlatformJobs(userId,platform,reason="Stopped by disconnect"){
  const now=new Date().toISOString();
  const {data,error}=await supabaseAdmin
    .from("snapshot_jobs")
    .update({
      status:"failed",
      error_message:reason,
      finished_at:now,
      updated_at:now,
      lifecycle_version:DISCONNECT_LIFECYCLE_VERSION
    })
    .eq("user_id",userId)
    .eq("platform",platform)
    .in("status",["queued","running"])
    .select("id,status,platform_account_id,error_message");
  if(error)throw error;
  return data||[];
}

async function ensureSnapshotSchedule(userId,platform,platformAccountId,metadata={}){
  const now=new Date().toISOString();
  const normalized=normalizePlatformAccountId(platformAccountId);
  if(!normalized)throw new Error("platform account id is required for schedule");

  let {data:existing,error:existingError}=await supabaseAdmin
    .from("snapshot_schedules")
    .select("*")
    .eq("user_id",userId)
    .eq("platform",platform)
    .eq("platform_account_id",normalized)
    .maybeSingle();
  if(existingError)throw existingError;

  if(!existing){
    const fallback=await supabaseAdmin
      .from("snapshot_schedules")
      .select("*")
      .eq("user_id",userId)
      .eq("platform",platform)
      .order("updated_at",{ascending:false})
      .limit(1)
      .maybeSingle();
    if(fallback.error)throw fallback.error;
    existing=fallback.data||null;
  }

  const patch={
    user_id:userId,
    platform,
    platform_account_id:normalized,
    active:true,
    interval_minutes:Number(existing?.interval_minutes||240),
    metadata:{...(existing?.metadata||{}),...metadata,lifecycleVersion:DISCONNECT_LIFECYCLE_VERSION},
    stopped_at:null,
    stop_reason:null,
    lifecycle_version:DISCONNECT_LIFECYCLE_VERSION,
    next_run_at:existing?.next_run_at||nextAutomationSlotUtc(new Date()),
    updated_at:now
  };

  if(existing){
    const {data,error}=await supabaseAdmin
      .from("snapshot_schedules")
      .update(patch)
      .eq("id",existing.id)
      .select("*")
      .maybeSingle();
    if(error)throw error;
    return data;
  }

  const {data,error}=await supabaseAdmin
    .from("snapshot_schedules")
    .insert({...patch,created_at:now})
    .select("*")
    .maybeSingle();
  if(error)throw error;
  return data;
}

async function requestLifecycleBackfill({userId,platform,platformAccountId,reason}){
  const now=new Date().toISOString();
  const normalized=normalizePlatformAccountId(platformAccountId);
  const cleanPlatform=String(platform||"").toLowerCase().trim();
  const cleanReason=String(reason||"account_backfill_30d").trim();
  if(!normalized)throw new Error("Backfill platform account id is required");
  if(!cleanPlatform)throw new Error("Backfill platform is required");

  const {data:existing,error:existingError}=await supabaseAdmin
    .from("snapshot_jobs")
    .select("id,status,created_at,updated_at,job_type,capture_reason,metadata")
    .eq("user_id",userId)
    .eq("platform",cleanPlatform)
    .eq("platform_account_id",normalized)
    .eq("job_type","backfill_30d")
    .eq("capture_reason",cleanReason)
    .in("status",["queued","running","completed"])
    .order("created_at",{ascending:false})
    .limit(1)
    .maybeSingle();
  if(existingError)throw existingError;
  if(existing)return {created:false,job:existing,reason:"existing_backfill_30d"};

  const {data,error}=await supabaseAdmin
    .from("snapshot_jobs")
    .insert({
      user_id:userId,
      platform:cleanPlatform,
      platform_account_id:normalized,
      status:"queued",
      job_type:"backfill_30d",
      capture_reason:cleanReason,
      lifecycle_version:DISCONNECT_LIFECYCLE_VERSION,
      metadata:{
        trigger:cleanReason,
        days:BACKFILL_DAYS_ON_RECONNECT,
        datePreset:"last_30d",
        captureReason:cleanReason,
        snapshotClass:"backfill",
        queuedBackfillVersion:"v1",
        lifecycleVersion:DISCONNECT_LIFECYCLE_VERSION
      },
      created_at:now,
      updated_at:now
    })
    .select("*")
    .maybeSingle();
  if(error)throw error;

  await supabaseAdmin
    .from("platform_account_ownerships")
    .update({last_backfill_requested_at:now,updated_at:now,lifecycle_version:DISCONNECT_LIFECYCLE_VERSION})
    .eq("owner_user_id",userId)
    .eq("platform",cleanPlatform)
    .eq("platform_account_id",normalized);

  return {created:true,job:data};
}

async function reactivatePlatformLifecycle(userId,platform,platformAccountId,reason="account_reactivation"){
  const normalized=normalizePlatformAccountId(platformAccountId);
  const now=new Date().toISOString();
  const ownership=await getOwnership(platform,normalized);
  if(!ownership||ownership.owner_user_id!==userId){
    const err=new Error("ownership not found for reactivation");
    err.status=404;
    throw err;
  }

  const {data:updatedOwnership,error:ownershipError}=await supabaseAdmin
    .from("platform_account_ownerships")
    .update({
      status:"active",
      reconnected_at:now,
      disconnected_at:null,
      disconnect_reason:null,
      lifecycle_version:DISCONNECT_LIFECYCLE_VERSION,
      updated_at:now
    })
    .eq("id",ownership.id)
    .select("*")
    .maybeSingle();
  if(ownershipError)throw ownershipError;

  const schedule=await ensureSnapshotSchedule(userId,platform,normalized,{reactivatedAt:now,reactivationReason:reason});
  const backfill=await requestLifecycleBackfill({userId,platform,platformAccountId:normalized,reason});

  return {ok:true,platform,platform_account_id:normalized,ownership:updatedOwnership,schedule,backfill};
}



const TIME_ENGINE_VERSION="v1.2";
const FX_ENGINE_VERSION="v1.1";
const FX_PROVIDER="snapshot_static_v1";
const DEFAULT_REPORTING_CURRENCY="TRY";

function normalizeCurrency(value){
  const s=String(value||"").trim().toUpperCase();
  return /^[A-Z]{3}$/.test(s)?s:null;
}

async function resolveFxRate(sourceCurrency,targetCurrency,options={}){
  const source=normalizeCurrency(sourceCurrency);
  const target=normalizeCurrency(targetCurrency)||source||DEFAULT_REPORTING_CURRENCY;
  const requestedRateDate=options.rateDate||options.snapshotDate||options.businessDate||new Date();

  if(!source){
    const fallback=target||DEFAULT_REPORTING_CURRENCY;
    return {
      fx_rate:1,
      fx_rate_date:fxDateOnly(requestedRateDate),
      fx_provider:FX_PROVIDER,
      fx_rate_timestamp:new Date().toISOString(),
      fx_source_currency:fallback,
      fx_target_currency:fallback,
      fx_engine_version:FX_ENGINE_VERSION,
      fx_source:"missing_source_currency_fallback"
    };
  }

  if(source===target){
    return {
      fx_rate:1,
      fx_rate_date:fxDateOnly(requestedRateDate),
      fx_provider:FX_PROVIDER,
      fx_rate_timestamp:new Date().toISOString(),
      fx_source_currency:source,
      fx_target_currency:target,
      fx_engine_version:FX_ENGINE_VERSION,
      fx_source:"same_currency"
    };
  }

  const daily=await getFxRateDaily({rateDate:requestedRateDate,baseCurrency:source,quoteCurrency:target});
  return {
    fx_rate:Number(daily.rate),
    fx_rate_date:daily.rate_date,
    fx_provider:daily.provider||FX_RATES_PROVIDER,
    fx_rate_timestamp:daily.fetched_at||new Date().toISOString(),
    fx_source_currency:daily.base_currency||source,
    fx_target_currency:daily.quote_currency||target,
    fx_engine_version:FX_ENGINE_VERSION,
    fx_source:daily.source||"fx_rates_daily"
  };
}

function convertMoney(value,fxRate){
  const n=Number(value||0);
  const rate=Number(fxRate||1);
  return Number.isFinite(n)&&Number.isFinite(rate)?n*rate:0;
}

function cloneJson(value){
  return JSON.parse(JSON.stringify(value||{}));
}

function applyFxToSnapshotPayload(snapshot,fx){
  const fxRate=Number(fx.fx_rate||1);
  const converted=cloneJson(snapshot);

  converted.account_currency=fx.fx_target_currency||converted.account_currency||null;

  if(converted.kpis){
    converted.kpis.spend=convertMoney(converted.kpis.spend,fxRate);
    converted.kpis.revenue=convertMoney(converted.kpis.revenue,fxRate);
    converted.kpis.sales=convertMoney(converted.kpis.sales,fxRate);
    converted.kpis.cpc=converted.kpis.clicks>0?converted.kpis.spend/converted.kpis.clicks:null;
  }

  if(converted.click_journey){
    converted.click_journey.real_cpc=converted.click_journey.landing_page_views>0?converted.kpis.spend/converted.click_journey.landing_page_views:null;
  }

  const rows=converted.performance_summary?.rows;
  if(Array.isArray(rows)){
    for(const row of rows){
      row.currency=converted.account_currency;
      row.spend=convertMoney(row.spend,fxRate);
      row.sales=convertMoney(row.sales,fxRate);
      row.revenue=convertMoney(row.revenue,fxRate);
      row.cpc=Number(row.clicks||0)>0?row.spend/Number(row.clicks||0):null;
      if(row.raw&&typeof row.raw==="object"){
        row.raw.fx_applied={
          fx_rate:fx.fx_rate,
          fx_rate_date:fx.fx_rate_date||null,
          fx_provider:fx.fx_provider,
          fx_rate_timestamp:fx.fx_rate_timestamp,
          fx_source_currency:fx.fx_source_currency,
          fx_target_currency:fx.fx_target_currency,
          fx_engine_version:fx.fx_engine_version
        };
      }
    }
  }

  return converted;
}



// ===== FX RATES DAILY ENGINE v1 =====
// Design rule: FX is fetched globally on UTC time, but applied by snapshot_date/business_date.
// Snapshot/Dataset values are not rewritten; Dashboard and Ask AI can convert at read time.
const FX_RATES_ENGINE_VERSION="v1";
const FX_RATES_PROVIDER="frankfurter_v1";
const FX_RATES_PROVIDER_BASE=process.env.FX_RATES_PROVIDER_BASE||"https://api.frankfurter.app";
const FX_SUPPORTED_CURRENCIES=(process.env.FX_SUPPORTED_CURRENCIES||"USD,EUR,TRY,GBP,JPY,CNY,AUD,CAD,CHF,SEK,NOK,DKK,PLN").split(",").map(normalizeCurrency).filter(Boolean);

function fxDateOnly(value=new Date()){
  const d=value instanceof Date?value:new Date(value);
  if(Number.isNaN(d.getTime()))return new Date().toISOString().slice(0,10);
  return d.toISOString().slice(0,10);
}

function fxUniqueCurrencies(list){
  return [...new Set((list||[]).map(normalizeCurrency).filter(Boolean))].sort();
}

function fxRateFromEurBase(eurRates,sourceCurrency,quoteCurrency){
  const source=normalizeCurrency(sourceCurrency);
  const quote=normalizeCurrency(quoteCurrency);
  if(!source||!quote)return null;
  if(source===quote)return 1;
  const eurToSource=source==="EUR"?1:Number(eurRates[source]);
  const eurToQuote=quote==="EUR"?1:Number(eurRates[quote]);
  if(!Number.isFinite(eurToSource)||eurToSource<=0||!Number.isFinite(eurToQuote)||eurToQuote<=0)return null;
  return eurToQuote/eurToSource;
}

async function fetchFrankfurterEurRates(rateDate,currencies=FX_SUPPORTED_CURRENCIES){
  const targetCurrencies=fxUniqueCurrencies(currencies).filter(c=>c!=="EUR");
  const endpoint=rateDate?`/${encodeURIComponent(rateDate)}`:"/latest";
  const url=new URL(`${FX_RATES_PROVIDER_BASE}${endpoint}`);
  if(targetCurrencies.length)url.searchParams.set("to",targetCurrencies.join(","));
  const response=await fetch(url);
  const data=await response.json().catch(()=>({}));
  if(!response.ok)throw new Error(data.message||data.error||`FX provider failed ${response.status}`);
  return {
    provider:FX_RATES_PROVIDER,
    engine_version:FX_RATES_ENGINE_VERSION,
    rate_date:fxDateOnly(data.date||rateDate||new Date()),
    base_currency:"EUR",
    rates:data.rates||{},
    raw:data
  };
}

async function upsertFxRatesDaily({rateDate=null,currencies=FX_SUPPORTED_CURRENCIES}={}){
  if(!supabaseAdmin)throw new Error("Supabase not configured");
  const providerData=await fetchFrankfurterEurRates(rateDate,currencies);
  const allCurrencies=fxUniqueCurrencies(["EUR",...currencies]);
  const fetchedAt=new Date().toISOString();
  const rows=[];
  for(const baseCurrency of allCurrencies){
    for(const quoteCurrency of allCurrencies){
      const rate=fxRateFromEurBase(providerData.rates,baseCurrency,quoteCurrency);
      if(rate===null)continue;
      rows.push({
        rate_date:providerData.rate_date,
        base_currency:baseCurrency,
        quote_currency:quoteCurrency,
        rate,
        provider:FX_RATES_PROVIDER,
        fetched_at:fetchedAt,
        engine_version:FX_RATES_ENGINE_VERSION,
        raw:{
          source:"eur_cross_rate",
          provider_base_currency:"EUR",
          provider_rate_date:providerData.rate_date,
          provider_raw:providerData.raw
        },
        updated_at:fetchedAt
      });
    }
  }
  if(!rows.length)throw new Error("FX provider returned no usable rates");
  const {data,error}=await supabaseAdmin
    .from("fx_rates_daily")
    .upsert(rows,{onConflict:"rate_date,base_currency,quote_currency,provider"})
    .select("rate_date,base_currency,quote_currency,rate,provider,fetched_at,engine_version");
  if(error)throw error;
  return {ok:true,provider:FX_RATES_PROVIDER,engine_version:FX_RATES_ENGINE_VERSION,rate_date:providerData.rate_date,rows:(data||[]).length,currencies:allCurrencies};
}

async function getFxRateDaily({rateDate,baseCurrency,quoteCurrency,provider=FX_RATES_PROVIDER}){
  const base=normalizeCurrency(baseCurrency);
  const quote=normalizeCurrency(quoteCurrency);
  if(!base||!quote)throw new Error("baseCurrency and quoteCurrency are required");
  if(base===quote){
    return {ok:true,rate_date:fxDateOnly(rateDate||new Date()),base_currency:base,quote_currency:quote,rate:1,provider,engine_version:FX_RATES_ENGINE_VERSION,source:"same_currency"};
  }
  const targetDate=fxDateOnly(rateDate||new Date());
  const {data,error}=await supabaseAdmin
    .from("fx_rates_daily")
    .select("rate_date,base_currency,quote_currency,rate,provider,fetched_at,engine_version")
    .eq("provider",provider)
    .eq("base_currency",base)
    .eq("quote_currency",quote)
    .lte("rate_date",targetDate)
    .order("rate_date",{ascending:false})
    .limit(1)
    .maybeSingle();
  if(error)throw error;
  if(!data){
    const err=new Error(`FX rate not found for ${base}->${quote} on or before ${targetDate}`);
    err.status=404;
    throw err;
  }
  return {ok:true,...data,requested_rate_date:targetDate,source:data.rate_date===targetDate?"exact_date":"last_available_rate"};
}

app.get("/api/cron/fx-rates",async(req,res)=>{
  const startedAt=new Date().toISOString();
  try{
    const rateDate=String(req.query.date||"").trim()||fxDateOnly(new Date());
    const currencies=String(req.query.currencies||"").trim()?String(req.query.currencies).split(","):FX_SUPPORTED_CURRENCIES;
    const result=await upsertFxRatesDaily({rateDate,currencies});
    res.json({ok:true,started_at:startedAt,...result});
  }catch(e){
    res.status(e.status||500).json({ok:false,error:e.message,started_at:startedAt,stage:"fx_rates_cron"});
  }
});

app.post("/api/fx/refresh",async(req,res)=>{
  try{
    const user=await requireUser(req,res);if(!user)return;
    const rateDate=String(req.body?.date||req.query.date||"").trim()||fxDateOnly(new Date());
    const currencies=String(req.body?.currencies||req.query.currencies||"").trim()?String(req.body?.currencies||req.query.currencies).split(","):FX_SUPPORTED_CURRENCIES;
    res.json(await upsertFxRatesDaily({rateDate,currencies}));
  }catch(e){res.status(e.status||500).json({ok:false,error:e.message,stage:"fx_refresh"})}
});

app.get("/api/fx/rate",async(req,res)=>{
  try{
    const user=await requireUser(req,res);if(!user)return;
    const rateDate=req.query.date||req.query.snapshot_date||fxDateOnly(new Date());
    const baseCurrency=req.query.base||req.query.base_currency||req.query.source_currency;
    const quoteCurrency=req.query.quote||req.query.quote_currency||req.query.target_currency;
    res.json(await getFxRateDaily({rateDate,baseCurrency,quoteCurrency}));
  }catch(e){res.status(e.status||500).json({ok:false,error:e.message,stage:"fx_rate_lookup"})}
});

app.get("/api/fx/convert",async(req,res)=>{
  try{
    const user=await requireUser(req,res);if(!user)return;
    const amount=Number(req.query.amount||0);
    const rateDate=req.query.date||req.query.snapshot_date||fxDateOnly(new Date());
    const baseCurrency=req.query.base||req.query.base_currency||req.query.source_currency;
    const quoteCurrency=req.query.quote||req.query.quote_currency||req.query.target_currency;
    const fx=await getFxRateDaily({rateDate,baseCurrency,quoteCurrency});
    res.json({...fx,amount,converted_amount:Number.isFinite(amount)?amount*Number(fx.rate):null});
  }catch(e){res.status(e.status||500).json({ok:false,error:e.message,stage:"fx_convert"})}
});
// ===== END FX RATES DAILY ENGINE v1 =====

const AUTOMATION_PLATFORM_HOURS=[0,4,8,12,16,20];
function nextAutomationSlotUtc(date=new Date()){
  const slots=[...AUTOMATION_PLATFORM_HOURS].map(Number).filter(h=>Number.isInteger(h)&&h>=0&&h<=23).sort((a,b)=>a-b);
  const base=new Date(date);
  for(const hour of slots){
    const candidate=new Date(Date.UTC(base.getUTCFullYear(),base.getUTCMonth(),base.getUTCDate(),hour,0,0,0));
    if(candidate.getTime()>base.getTime())return candidate.toISOString();
  }
  const first=slots.length?slots[0]:0;
  return new Date(Date.UTC(base.getUTCFullYear(),base.getUTCMonth(),base.getUTCDate()+1,first,0,0,0)).toISOString();
}
const DEFAULT_PLATFORM_TIMEZONE="UTC";
const DEFAULT_DATA_MATURITY_WINDOW_HOURS={meta:3,google:3,tiktok:3,klaviyo:3,organic:3,pinterest:3};

function validTimeZone(tz){
  try{
    if(!tz)return false;
    new Intl.DateTimeFormat("en-US",{timeZone:tz}).format(new Date());
    return true;
  }catch{
    return false;
  }
}

function normalizeTimeZone(tz){
  return validTimeZone(tz)?tz:DEFAULT_PLATFORM_TIMEZONE;
}

function timePartsInZone(date=new Date(),timeZone=DEFAULT_PLATFORM_TIMEZONE){
  const tz=normalizeTimeZone(timeZone);
  const parts=new Intl.DateTimeFormat("en-CA",{
    timeZone:tz,
    year:"numeric",
    month:"2-digit",
    day:"2-digit",
    hour:"2-digit",
    minute:"2-digit",
    second:"2-digit",
    hour12:false
  }).formatToParts(date);
  const get=type=>parts.find(p=>p.type===type)?.value;
  let hour=Number(get("hour")||0);
  if(hour===24)hour=0;
  const year=get("year");
  const month=get("month");
  const day=get("day");
  const minute=get("minute");
  const second=get("second");
  return {
    timeZone:tz,
    date:`${year}-${month}-${day}`,
    hour,
    minute:Number(minute||0),
    second:Number(second||0),
    text:`${year}-${month}-${day} ${String(hour).padStart(2,"0")}:${minute}:${second}`
  };
}

function resolveAdminTimeSync(date=new Date(),platformTimeZone=DEFAULT_PLATFORM_TIMEZONE){
  const serverTimeUtc=date.toISOString();
  const istanbul=timePartsInZone(date,"Europe/Istanbul");
  const platform=timePartsInZone(date,platformTimeZone);
  return {
    server_time_utc:serverTimeUtc,
    platform_business_at:serverTimeUtc,
    istanbul_time:istanbul.text,
    platform_account_time:platform.text,
    platform_account_timezone:platform.timeZone,
    platform_business_date:platform.date,
    platform_business_hour:platform.hour
  };
}

function dataMaturityWindowHours(platform){
  return DEFAULT_DATA_MATURITY_WINDOW_HOURS[platform]??3;
}

async function getPlatformAccountTimezone(userId,platform,platformAccountId,conn=null,ownership=null){
  const normalized=normalizePlatformAccountId(platformAccountId);
  let candidates=[
    conn?.metadata?.timezone_name,
    conn?.metadata?.timezone,
    conn?.metadata?.platform_account_timezone,
    conn?.metadata?.account_timezone,
    ownership?.metadata?.timezone_name,
    ownership?.metadata?.timezone,
    ownership?.metadata?.platform_account_timezone
  ];

  if(supabaseAdmin&&userId&&normalized){
    const {data,error}=await supabaseAdmin
      .from("platform_ad_accounts")
      .select("timezone,metadata")
      .eq("user_id",userId)
      .eq("platform",platform)
      .eq("platform_account_id",normalized)
      .maybeSingle();
    if(!error&&data){
      candidates=[
        data.timezone,
        data.metadata?.timezone_name,
        data.metadata?.timezone,
        ...candidates
      ];
    }
  }

  const found=candidates.find(validTimeZone);
  return normalizeTimeZone(found);
}


async function getUserAccountCurrency(userId){
  const {data,error}=await supabaseAdmin.from("users").select("account_currency").eq("id",userId).maybeSingle();
  if(error)throw error;
  return normalizeCurrency(data?.account_currency)||DEFAULT_REPORTING_CURRENCY;
}
async function getOwnership(platform,platformAccountId){
  const id=normalizePlatformAccountId(platformAccountId);
  if(!id)return null;
  const {data,error}=await supabaseAdmin
    .from("platform_account_ownerships")
    .select("*")
    .eq("platform",platform)
    .eq("platform_account_id",id)
    .maybeSingle();
  if(error)throw error;
  return data;
}
async function countActiveOwnerships(userId,platform){
  const {count,error}=await supabaseAdmin
    .from("platform_account_ownerships")
    .select("id",{count:"exact",head:true})
    .eq("owner_user_id",userId)
    .eq("platform",platform)
    .eq("account_type",phase1ReportableAccountType(platform))
    .in("status",activeOwnershipStatuses());
  if(error)throw error;
  return count||0;
}

function accountSelectionLimitMessage(platform,selectedCount,limit){
  const label=String(platform||"platform").replace(/^./,c=>c.toUpperCase());
  if(selectedCount>limit){
    return `You selected ${selectedCount} accounts. AdsTable supports up to ${limit} accounts per platform. Select up to ${limit} accounts to continue.`;
  }
  return `You can connect up to ${limit} accounts per platform. You already have ${limit} connected ${label} accounts. Disconnect an existing account to connect a new one.`;
}

async function validateSelectedAccounts(userId,platform,selectedAccounts=[]){
  const cleanPlatform=String(platform||"").toLowerCase().trim();
  assertPlatformNotPassiveLegacy(cleanPlatform);
  const limit=PHASE1_PLATFORM_LIMITS[cleanPlatform]||3;
  const accounts=(Array.isArray(selectedAccounts)?selectedAccounts:[])
    .map(account=>({...(account||{}),platform_account_id:normalizePlatformAccountId(account?.platform_account_id||account?.property_id||account?.site_url||account?.id||account?.customerId||account?.account_id||account?.advertiser_id)}))
    .filter(account=>account.platform_account_id);
  const unique=[];
  const seen=new Set();
  for(const account of accounts){
    if(seen.has(account.platform_account_id))continue;
    seen.add(account.platform_account_id);
    unique.push(account);
  }
  if(!cleanPlatform){const err=new Error("platform is required");err.status=400;throw err;}
  if(!unique.length){const err=new Error("Select at least 1 account to continue.");err.status=400;throw err;}
  if(unique.length>limit){const err=new Error(accountSelectionLimitMessage(cleanPlatform,unique.length,limit));err.status=403;err.code="ACCOUNT_SELECTION_LIMIT";err.limit=limit;err.selectedCount=unique.length;throw err;}
  const activeCount=await countActiveOwnerships(userId,cleanPlatform);
  const existingIds=[];
  for(const account of unique){
    const existing=await getOwnership(cleanPlatform,account.platform_account_id);
    if(existing&&existing.owner_user_id===userId&&activeOwnershipStatuses().includes(existing.status))existingIds.push(account.platform_account_id);
  }
  const newCount=unique.length-existingIds.length;
  if(activeCount+newCount>limit){
    const err=new Error(accountSelectionLimitMessage(cleanPlatform,activeCount+newCount,limit));
    err.status=403;err.code="ACCOUNT_SELECTION_LIMIT";err.limit=limit;err.activeCount=activeCount;err.selectedCount=unique.length;throw err;
  }
  return {platform:cleanPlatform,limit,activeCount,selectedAccounts:unique};
}

async function selectPlatformAccountsForLifecycle(userId,platform,selectedAccounts=[]){
  const validation=await validateSelectedAccounts(userId,platform,selectedAccounts);
  const now=new Date().toISOString();
  const results=[];
  for(const account of validation.selectedAccounts){
    const row={
      user_id:userId,
      platform:validation.platform,
      platform_business_id:account.business_id||account.platform_business_id||null,
      platform_account_id:account.platform_account_id,
      account_name:account.name||account.descriptiveName||account.account_name||account.advertiser_name||`Account ${account.platform_account_id}`,
      currency:account.currency||account.currency_code||null,
      timezone:account.timezone_name||account.timezone||DEFAULT_PLATFORM_TIMEZONE,
      status:String(account.account_status||account.status||"active"),
      metadata:{...account,selectedByUserAt:now,accountSelectionGuardVersion:"v1"},
      updated_at:now
    };
    const ownership=await ensurePlatformOwnership(userId,validation.platform,row);
    await supabaseAdmin.from("platform_ad_accounts").upsert(row,{onConflict:"user_id,platform,platform_account_id"});
    const schedule=await ensureSnapshotSchedule(userId,validation.platform,row.platform_account_id,{accountSelectionGuardVersion:"v1",selectedByUserAt:now,account_type:phase1ReportableAccountType(validation.platform)});
    const backfill=await requestLifecycleBackfill({userId,platform:validation.platform,platformAccountId:row.platform_account_id,reason:"account_initial_connect"});
    await saveConnection(userId,validation.platform,{accountId:row.platform_account_id,accountName:row.account_name,metadata:{lastOwnedPlatformAccountId:row.platform_account_id,selectedPlatformAccountId:row.platform_account_id,baseCurrency:row.currency,accountSelectionGuardVersion:"v1",selectedByUserAt:now,lastBackfill30dJobId:backfill?.job?.id||null,lastBackfill30dCreated:backfill?.created||false}});
    results.push({platform:validation.platform,platform_account_id:row.platform_account_id,account_name:row.account_name,ownership_id:ownership?.id||null,schedule_id:schedule?.id||null,backfill_30d:backfill});
  }
  return {ok:true,platform:validation.platform,limit:validation.limit,selected_count:results.length,accounts:results,message:`Selected ${results.length} account(s).`};
}
async function ensurePlatformOwnership(userId,platform,account){
  if(!supabaseAdmin||!userId)throw new Error("Supabase not configured or user missing");
  const platformAccountId=normalizePlatformAccountId(account.platform_account_id||account.property_id||account.site_url||account.id||account.customerId||account.account_id);
  if(!platformAccountId)throw new Error("Platform account id is required for ownership");
  const existing=await getOwnership(platform,platformAccountId);
  const now=new Date().toISOString();
  if(existing&&existing.owner_user_id!==userId&&activeOwnershipStatuses().includes(existing.status)){
    const err=new Error("Platform account already owned by another user");
    err.status=409;
    throw err;
  }
  if(!existing){
    const limit=PHASE1_PLATFORM_LIMITS[platform]||3;
    const activeCount=await countActiveOwnerships(userId,platform);
    if(activeCount>=limit){
      const err=new Error(`Connected reportable platform account limit reached for ${platform}`);
      err.status=403;
      throw err;
    }
    const {data,error}=await supabaseAdmin
      .from("platform_account_ownerships")
      .insert({
        owner_user_id:userId,
        platform,
        platform_account_id:platformAccountId,
        platform_account_name:account.account_name||account.name||account.descriptiveName||null,
        account_type:phase1ReportableAccountType(platform),
        base_currency:account.currency||account.currency_code||null,
        status:"active",
        connected_at:now,
        updated_at:now,
        metadata:account
      })
      .select("*")
      .maybeSingle();
    if(error)throw error;
    return data;
  }
  const wasDisconnected=existing.status==="disconnected";
  const {data,error}=await supabaseAdmin
    .from("platform_account_ownerships")
    .update({
      owner_user_id:userId,
      platform_account_name:account.account_name||account.name||account.descriptiveName||existing.platform_account_name||null,
      account_type:phase1ReportableAccountType(platform),
      base_currency:account.currency||account.currency_code||existing.base_currency||null,
      status:"active",
      connected_at:existing.connected_at||now,
      reconnected_at:wasDisconnected?now:(existing.reconnected_at||null),
      disconnected_at:null,
      disconnect_reason:null,
      lifecycle_version:DISCONNECT_LIFECYCLE_VERSION,
      updated_at:now,
      metadata:{...(existing.metadata||{}),...account}
    })
    .eq("id",existing.id)
    .select("*")
    .maybeSingle();
  if(error)throw error;

  if(wasDisconnected){
    await ensureSnapshotSchedule(userId,platform,platformAccountId,{reconnectedAt:now,reconnectSource:"ensurePlatformOwnership"});
    await requestLifecycleBackfill({userId,platform,platformAccountId,reason:"account_reconnect"});
  }

  return data;
}
async function requireActiveOwnership(userId,platform,platformAccountId){
  const ownership=await getOwnership(platform,platformAccountId);
  if(!ownership||ownership.owner_user_id!==userId||!activeOwnershipStatuses().includes(ownership.status)){
    const err=new Error("Platform account ownership is not active");
    err.status=403;
    throw err;
  }
  return ownership;
}
async function disconnectPlatformLifecycle(userId,platform,options={}){
  const now=new Date().toISOString();
  const reason=disconnectReasonText(options.reason||"user_disconnect");

  const {data:connections,error:connReadError}=await supabaseAdmin
    .from("platform_connections")
    .select("*")
    .eq("user_id",userId)
    .eq("platform",platform);
  if(connReadError)throw connReadError;

  const revoke_results=[];
  for(const conn of connections||[]){
    revoke_results.push(await revokePlatformToken(platform,conn));
  }

  const failedRevoke=revoke_results.find(r=>r.attempted&&r.ok===false);
  if(failedRevoke){
    const err=new Error(`${platform} revoke failed: ${failedRevoke.error}`);
    err.status=502;
    err.revoke_results=revoke_results;
    throw err;
  }

  const {data:connData,error:connError}=await supabaseAdmin
    .from("platform_connections")
    .update({
      connected:false,
      access_token:null,
      refresh_token:null,
      token_expires_at:null,
      disconnected_at:now,
      disconnect_reason:reason,
      lifecycle_version:DISCONNECT_LIFECYCLE_VERSION,
      updated_at:now
    })
    .eq("user_id",userId)
    .eq("platform",platform)
    .select("platform,account_id,account_name,connected,disconnected_at,disconnect_reason,lifecycle_version");
  if(connError)throw connError;

  const {data:ownershipData,error:ownershipError}=await supabaseAdmin
    .from("platform_account_ownerships")
    .update({
      status:"disconnected",
      disconnected_at:now,
      disconnect_reason:reason,
      lifecycle_version:DISCONNECT_LIFECYCLE_VERSION,
      updated_at:now
    })
    .eq("owner_user_id",userId)
    .eq("platform",platform)
    .in("status",activeOwnershipStatuses())
    .select("id,platform,platform_account_id,platform_account_name,status,disconnected_at,disconnect_reason,lifecycle_version");
  if(ownershipError)throw ownershipError;

  const stopped_schedules=await stopPlatformSchedules(userId,platform,reason);
  const stopped_jobs=await failOpenPlatformJobs(userId,platform,"Stopped by disconnect");

  return {
    ok:true,
    platform,
    connected:false,
    lifecycle_version:DISCONNECT_LIFECYCLE_VERSION,
    disconnected_at:now,
    reason,
    revoke_results,
    connections:connData||[],
    ownerships:ownershipData||[],
    stopped_schedules,
    stopped_jobs,
    snapshot_generation:"stopped",
    snapshots:"preserved"
  };
}
async function createRefreshJob(userId,platform,platformAccountId,metadata={}){
  const existing=await supabaseAdmin
    .from("snapshot_jobs")
    .select("id,status")
    .eq("user_id",userId)
    .eq("platform",platform)
    .eq("platform_account_id",platformAccountId)
    .in("status",["queued","running"])
    .limit(1)
    .maybeSingle();
  if(existing.error)throw existing.error;
  if(existing.data){
    const err=new Error("Refresh job already queued or running for this platform account");
    err.status=409;
    err.job=existing.data;
    throw err;
  }
  const {data,error}=await supabaseAdmin
    .from("snapshot_jobs")
    .insert({
      user_id:userId,
      platform,
      platform_account_id:platformAccountId,
      status:"queued",
      job_type:metadata.jobType||metadata.job_type||metadata.trigger||"refresh",
      capture_reason:metadata.captureReason||metadata.capture_reason||null,
      lifecycle_version:metadata.lifecycleVersion||metadata.lifecycle_version||DISCONNECT_LIFECYCLE_VERSION,
      metadata,
      created_at:new Date().toISOString(),
      updated_at:new Date().toISOString()
    })
    .select("*")
    .maybeSingle();
  if(error)throw error;
  return data;
}
async function setRefreshJobStatus(jobId,status,extra={}){
  const now=new Date().toISOString();
  const patch={status,updated_at:now,...extra};
  if(status==="running")patch.started_at=now;
  if(["completed","failed"].includes(status))patch.finished_at=now;
  const {data,error}=await supabaseAdmin.from("snapshot_jobs").update(patch).eq("id",jobId).select("*").maybeSingle();
  if(error)throw error;
  return data;
}
// ===== END PHASE 1 CONSTITUTION PACK HELPERS =====
function googleOAuthClient(){if(!process.env.GOOGLE_CLIENT_ID||!process.env.GOOGLE_CLIENT_SECRET||!process.env.GOOGLE_REDIRECT_URI)throw new Error("Missing Google OAuth env");return new google.auth.OAuth2(process.env.GOOGLE_CLIENT_ID,process.env.GOOGLE_CLIENT_SECRET,process.env.GOOGLE_REDIRECT_URI)}
async function getFreshGoogleAccessToken(userId){const conn=await getConnection(userId,"google");if(!conn)throw new Error("Google not connected");const exp=conn.token_expires_at?new Date(conn.token_expires_at).getTime():0;if(conn.access_token&&exp&&exp>Date.now()+120000)return conn.access_token;if(!conn.refresh_token){if(conn.access_token)return conn.access_token;throw new Error("Google refresh token missing. Please reconnect Google.")}const client=googleOAuthClient();client.setCredentials({refresh_token:conn.refresh_token});const {credentials}=await client.refreshAccessToken();const token=credentials.access_token;const expiry=credentials.expiry_date||(Date.now()+3600*1000);await saveConnection(userId,"google",{accessToken:token,refreshToken:conn.refresh_token,tokenExpiresAt:new Date(expiry).toISOString(),metadata:{...(conn.metadata||{}),refreshedAt:new Date().toISOString(),expiryDate:expiry}});return token}
app.get("/auth/meta",async(req,res)=>{try{const accessCheck=await requireConnectAccessForOAuth(req,res);if(!accessCheck)return;const userId=accessCheck.userId;if(!process.env.META_APP_ID||!process.env.META_REDIRECT_URI)throw new Error("Missing Meta env");const state=Math.random().toString(36).slice(2);req.session.metaOAuthState=state;req.session.oauthUserId=userId;const p=new URLSearchParams({client_id:process.env.META_APP_ID,redirect_uri:process.env.META_REDIRECT_URI,state,response_type:"code",scope:"ads_read"});res.redirect(`https://www.facebook.com/${META_GRAPH_VERSION}/dialog/oauth?${p}`)}catch(e){res.status(500).send(e.message)}});
app.get("/auth/meta/callback",async(req,res)=>{try{const{code,state,error,error_description}=req.query;if(error)return res.redirect(`/dashboard?meta_error=${encodeURIComponent(error_description||error)}`);if(!code)return res.redirect("/dashboard?meta_error=missing_code");if(!state||state!==req.session.metaOAuthState)return res.redirect("/dashboard?meta_error=invalid_state");const userId=req.session.oauthUserId;if(!userId)return res.redirect("/dashboard?meta_error=missing_user_id");const url=new URL(`https://graph.facebook.com/${META_GRAPH_VERSION}/oauth/access_token`);url.searchParams.set("client_id",process.env.META_APP_ID);url.searchParams.set("redirect_uri",process.env.META_REDIRECT_URI);url.searchParams.set("client_secret",process.env.META_APP_SECRET);url.searchParams.set("code",code);const r=await fetch(url);const data=await r.json();if(!r.ok||!data.access_token)throw new Error(data.error?.message||"Meta token exchange failed");await saveConnection(userId,"meta",{accessToken:data.access_token,tokenExpiresAt:parseExpiry(data.expires_in),metadata:{expiresIn:data.expires_in||null}});

const metaConnAfterReconnect=await getConnection(userId,"meta");
if(metaConnAfterReconnect){
  try{
    const accountsData=await metaGraph("/me/adaccounts",{fields:"id,name,account_status,currency,timezone_name",limit:"100"},data.access_token);
    for(const account of accountsData.data||[])await upsertAdAccount(userId,"meta",account);
    await saveConnection(userId,"meta",{metadata:{accountSelectionRequired:true,availableAccountCount:(accountsData.data||[]).length,accountSelectionGuardVersion:"v1"}});
  }catch(discoveryError){
    await saveConnection(userId,"meta",{metadata:{accountSelectionRequired:true,accountDiscoveryError:discoveryError.message,accountSelectionGuardVersion:"v1"}});
  }
}
req.session.metaOAuthState=null;res.redirect("/dashboard?meta_connected=1&account_selection_required=1")}catch(e){res.redirect(`/dashboard?meta_error=${encodeURIComponent(e.message)}`)}});
app.get("/auth/google",async(req,res)=>{try{const accessCheck=await requireConnectAccessForOAuth(req,res);if(!accessCheck)return;const userId=accessCheck.userId;const state=Math.random().toString(36).slice(2);req.session.googleOAuthState=state;req.session.oauthUserId=userId;const url=googleOAuthClient().generateAuthUrl({access_type:"offline",prompt:"consent",state,scope:["https://www.googleapis.com/auth/adwords"]});res.redirect(url)}catch(e){res.status(500).send(e.message)}});
app.get("/auth/google/callback",async(req,res)=>{try{const{code,state,error}=req.query;if(error)return res.redirect(`/dashboard?google_error=${encodeURIComponent(error)}`);if(!code)return res.redirect("/dashboard?google_error=missing_code");if(!state||state!==req.session.googleOAuthState)return res.redirect("/dashboard?google_error=invalid_state");const userId=req.session.oauthUserId;if(!userId)return res.redirect("/dashboard?google_error=missing_user_id");const client=googleOAuthClient();const{tokens}=await client.getToken(code);await saveConnection(userId,"google",{accessToken:tokens.access_token,refreshToken:tokens.refresh_token||null,tokenExpiresAt:tokens.expiry_date?new Date(tokens.expiry_date).toISOString():null,metadata:{scope:tokens.scope||null,expiryDate:tokens.expiry_date||null,tokenType:tokens.token_type||null}});req.session.googleOAuthState=null;res.redirect("/dashboard?google_connected=1&account_selection_required=1")}catch(e){res.redirect(`/dashboard?google_error=${encodeURIComponent(e.message)}`)}});

// ===== ORGANIC GOOGLE OAUTH + DISCOVERY v1 =====
function organicGoogleOAuthClient(){
  if(!process.env.GOOGLE_CLIENT_ID||!process.env.GOOGLE_CLIENT_SECRET||!ORGANIC_GOOGLE_REDIRECT_URI)throw new Error("Missing Organic Google OAuth env");
  return new google.auth.OAuth2(process.env.GOOGLE_CLIENT_ID,process.env.GOOGLE_CLIENT_SECRET,ORGANIC_GOOGLE_REDIRECT_URI);
}
async function getFreshOrganicAccessToken(userId){
  const conn=await getConnection(userId,"organic");
  if(!conn)throw new Error("Organic not connected");
  const exp=conn.token_expires_at?new Date(conn.token_expires_at).getTime():0;
  if(conn.access_token&&exp&&exp>Date.now()+120000)return conn.access_token;
  if(!conn.refresh_token){
    if(conn.access_token)return conn.access_token;
    throw new Error("Organic Google refresh token missing. Please reconnect Organic.");
  }
  const client=organicGoogleOAuthClient();
  client.setCredentials({refresh_token:conn.refresh_token});
  const {credentials}=await client.refreshAccessToken();
  const token=credentials.access_token;
  const expiry=credentials.expiry_date||(Date.now()+3600*1000);
  await saveConnection(userId,"organic",{accessToken:token,refreshToken:conn.refresh_token,tokenExpiresAt:new Date(expiry).toISOString(),metadata:{...(conn.metadata||{}),refreshedAt:new Date().toISOString(),expiryDate:expiry,organicOAuthVersion:"v1"}});
  return token;
}
async function organicGoogleFetch(userId,url){
  const token=await getFreshOrganicAccessToken(userId);
  const r=await fetch(url,{headers:{Authorization:`Bearer ${token}`,Accept:"application/json"}});
  const data=await r.json().catch(()=>({status:r.status}));
  if(!r.ok)throw new Error(data.error?.message||data.message||`Organic Google API failed ${r.status}`);
  return data;
}
function normalizeGa4PropertySummary(accountSummary,propertySummary){
  const propertyName=propertySummary?.property||"";
  const propertyId=String(propertyName).replace(/^properties\//,"");
  return {
    platform_account_id:propertyId,
    property_id:propertyId,
    property_resource_name:propertyName||null,
    property_name:propertySummary?.displayName||propertySummary?.display_name||propertyName||null,
    account_id:String(accountSummary?.account||"").replace(/^accounts\//,"")||null,
    account_resource_name:accountSummary?.account||null,
    account_name:accountSummary?.displayName||accountSummary?.display_name||null,
    raw:{accountSummary,propertySummary}
  };
}
function normalizeSearchConsoleSite(site){
  const siteUrl=String(site?.siteUrl||site?.site_url||"").trim();
  return {
    platform_account_id:siteUrl,
    site_url:siteUrl,
    permission_level:site?.permissionLevel||site?.permission_level||null,
    raw:site
  };
}
app.get("/auth/organic",async(req,res)=>{
  try{
    const accessCheck=await requireConnectAccessForOAuth(req,res);if(!accessCheck)return;
    const userId=accessCheck.userId;
    const state=Math.random().toString(36).slice(2);
    req.session.organicOAuthState=state;
    req.session.oauthUserId=userId;
    const url=organicGoogleOAuthClient().generateAuthUrl({
      access_type:"offline",
      prompt:"consent",
      include_granted_scopes:true,
      state,
      scope:ORGANIC_GOOGLE_SCOPES
    });
    res.redirect(url);
  }catch(e){res.status(500).send(e.message)}
});
app.get("/auth/organic/callback",async(req,res)=>{
  try{
    const{code,state,error}=req.query;
    if(error)return res.redirect(`/dashboard?organic_error=${encodeURIComponent(error)}`);
    if(!code)return res.redirect("/dashboard?organic_error=missing_code");
    if(!state||state!==req.session.organicOAuthState)return res.redirect("/dashboard?organic_error=invalid_state");
    const userId=req.session.oauthUserId;
    if(!userId)return res.redirect("/dashboard?organic_error=missing_user_id");
    const client=organicGoogleOAuthClient();
    const{tokens}=await client.getToken(code);
    await saveConnection(userId,"organic",{
      accessToken:tokens.access_token,
      refreshToken:tokens.refresh_token||null,
      tokenExpiresAt:tokens.expiry_date?new Date(tokens.expiry_date).toISOString():null,
      metadata:{
        scope:tokens.scope||ORGANIC_GOOGLE_SCOPES.join(" "),
        expiryDate:tokens.expiry_date||null,
        tokenType:tokens.token_type||null,
        setupStage:"oauth_connected",
        organicOAuthVersion:"v1",
        ga4PropertySelectionRequired:true,
        searchConsoleSiteSelectionRequired:true
      }
    });
    req.session.organicOAuthState=null;
    res.redirect("/dashboard?organic_connected=1&organic_setup=property_selection_required");
  }catch(e){res.redirect(`/dashboard?organic_error=${encodeURIComponent(e.message)}`)}
});
async function listOrganicGa4Properties(userId){
  const data=await organicGoogleFetch(userId,`${GA4_ADMIN_API_BASE}/accountSummaries?pageSize=200`);
  const accounts=Array.isArray(data.accountSummaries)?data.accountSummaries:[];
  const properties=[];
  for(const account of accounts){
    for(const prop of account.propertySummaries||[])properties.push(normalizeGa4PropertySummary(account,prop));
  }
  return {ok:true,platform:"organic",source:"ga4_admin_api",properties,rawCount:properties.length,nextPageToken:data.nextPageToken||null};
}
async function listOrganicSearchConsoleSites(userId){
  const data=await organicGoogleFetch(userId,`${SEARCH_CONSOLE_API_BASE}/sites`);
  const sites=(Array.isArray(data.siteEntry)?data.siteEntry:[]).map(normalizeSearchConsoleSite).filter(s=>s.site_url);
  return {ok:true,platform:"organic",source:"search_console_api",sites,rawCount:sites.length};
}
app.get("/api/organic/ga4/properties",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;res.json(await listOrganicGa4Properties(user.id))}catch(e){res.status(e.status||500).json({ok:false,error:e.message,stage:"organic_ga4_property_discovery"})}});
app.get("/api/platform/organic/ga4/properties",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;res.json(await listOrganicGa4Properties(user.id))}catch(e){res.status(e.status||500).json({ok:false,error:e.message,stage:"organic_ga4_property_discovery"})}});
app.get("/api/organic/search-console/sites",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;res.json(await listOrganicSearchConsoleSites(user.id))}catch(e){res.status(e.status||500).json({ok:false,error:e.message,stage:"organic_search_console_site_discovery"})}});
app.get("/api/platform/organic/search-console/sites",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;res.json(await listOrganicSearchConsoleSites(user.id))}catch(e){res.status(e.status||500).json({ok:false,error:e.message,stage:"organic_search_console_site_discovery"})}});

function pickOrganicGa4Property(input){
  const propertyId=normalizePlatformAccountId(input?.property_id||input?.propertyId||input?.platform_account_id||input?.id||String(input?.property_resource_name||input?.property||"").replace(/^properties\//,""));
  if(!propertyId)return null;
  return {
    platform_account_id:propertyId,
    property_id:propertyId,
    property_resource_name:input?.property_resource_name||input?.property||`properties/${propertyId}`,
    property_name:input?.property_name||input?.displayName||input?.display_name||input?.name||`GA4 Property ${propertyId}`,
    account_id:input?.account_id||null,
    account_name:input?.account_name||null,
    raw:input
  };
}
function pickOrganicSearchConsoleSite(input){
  const siteUrl=String(input?.site_url||input?.siteUrl||input?.platform_account_id||input?.id||"").trim();
  if(!siteUrl)return null;
  return {
    platform_account_id:siteUrl,
    site_url:siteUrl,
    permission_level:input?.permission_level||input?.permissionLevel||null,
    raw:input
  };
}
async function bindOrganicPropertyAndSite(userId,body={}){
  const now=new Date().toISOString();
  const conn=await getConnection(userId,"organic");
  if(!conn)throw Object.assign(new Error("Organic OAuth connection is required before property binding"),{status:404});

  const requestedProperty=pickOrganicGa4Property(body.ga4_property||body.ga4Property||body.property||body);
  const requestedSite=pickOrganicSearchConsoleSite(body.search_console_site||body.searchConsoleSite||body.site||body);
  if(!requestedProperty)throw Object.assign(new Error("GA4 property selection is required"),{status:400});
  if(!requestedSite)throw Object.assign(new Error("Search Console site selection is required"),{status:400});

  const availableProperties=(await listOrganicGa4Properties(userId)).properties||[];
  const verifiedProperty=availableProperties.find(p=>p.property_id===requestedProperty.property_id||p.platform_account_id===requestedProperty.platform_account_id);
  if(!verifiedProperty)throw Object.assign(new Error("Selected GA4 property is not available for this Organic connection"),{status:403});

  const availableSites=(await listOrganicSearchConsoleSites(userId)).sites||[];
  const verifiedSite=availableSites.find(s=>s.site_url===requestedSite.site_url);
  if(!verifiedSite)throw Object.assign(new Error("Selected Search Console site is not available for this Organic connection"),{status:403});

  const organicAccount={
    platform_account_id:verifiedProperty.property_id,
    account_name:verifiedProperty.property_name||`GA4 Property ${verifiedProperty.property_id}`,
    name:verifiedProperty.property_name||`GA4 Property ${verifiedProperty.property_id}`,
    property_id:verifiedProperty.property_id,
    property_resource_name:verifiedProperty.property_resource_name,
    site_url:verifiedSite.site_url,
    currency:null,
    metadata:{
      source:"organic_property_site_binding",
      organicBindingVersion:"v1",
      selectedAt:now,
      ga4_property:verifiedProperty,
      search_console_site:verifiedSite
    }
  };

  const ownership=await ensurePlatformOwnership(userId,"organic",organicAccount);

  await supabaseAdmin.from("platform_ad_accounts").upsert({
    user_id:userId,
    platform:"organic",
    platform_business_id:verifiedProperty.account_id||null,
    platform_account_id:verifiedProperty.property_id,
    account_name:verifiedProperty.property_name||`GA4 Property ${verifiedProperty.property_id}`,
    currency:null,
    timezone:DEFAULT_PLATFORM_TIMEZONE,
    status:"active",
    metadata:{
      organicBindingVersion:"v1",
      selectedAt:now,
      ga4_property:verifiedProperty,
      search_console_site:verifiedSite
    },
    updated_at:now
  },{onConflict:"user_id,platform,platform_account_id"});

  await saveConnection(userId,"organic",{
    accountId:verifiedProperty.property_id,
    accountName:verifiedProperty.property_name||`GA4 Property ${verifiedProperty.property_id}`,
    metadata:{
      ...(conn.metadata||{}),
      setupStage:"configured",
      configured:true,
      configuredAt:now,
      organicBindingVersion:"v1",
      selectedPlatformAccountId:verifiedProperty.property_id,
      lastOwnedPlatformAccountId:verifiedProperty.property_id,
      ga4PropertySelectionRequired:false,
      searchConsoleSiteSelectionRequired:false,
      selectedGa4Property:verifiedProperty,
      selectedSearchConsoleSite:verifiedSite
    }
  });

  return {
    ok:true,
    platform:"organic",
    setupStage:"configured",
    configured:true,
    platform_account_id:verifiedProperty.property_id,
    ga4_property:verifiedProperty,
    search_console_site:verifiedSite,
    ownership_id:ownership?.id||null
  };
}
app.post("/api/organic/bind",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;res.json(await bindOrganicPropertyAndSite(user.id,req.body||{}))}catch(e){res.status(e.status||500).json({ok:false,error:e.message,stage:"organic_property_site_binding"})}});
app.post("/api/platform/organic/bind",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;res.json(await bindOrganicPropertyAndSite(user.id,req.body||{}))}catch(e){res.status(e.status||500).json({ok:false,error:e.message,stage:"organic_property_site_binding"})}});
app.get("/api/organic/binding",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;const conn=await getConnection(user.id,"organic");res.json({ok:true,platform:"organic",configured:Boolean(conn?.metadata?.configured),setupStage:conn?.metadata?.setupStage||null,ga4_property:conn?.metadata?.selectedGa4Property||null,search_console_site:conn?.metadata?.selectedSearchConsoleSite||null,updatedAt:conn?.updated_at||null})}catch(e){res.status(e.status||500).json({ok:false,error:e.message,stage:"organic_binding_status"})}});
app.get("/api/platform/organic/binding",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;const conn=await getConnection(user.id,"organic");res.json({ok:true,platform:"organic",configured:Boolean(conn?.metadata?.configured),setupStage:conn?.metadata?.setupStage||null,ga4_property:conn?.metadata?.selectedGa4Property||null,search_console_site:conn?.metadata?.selectedSearchConsoleSite||null,updatedAt:conn?.updated_at||null})}catch(e){res.status(e.status||500).json({ok:false,error:e.message,stage:"organic_binding_status"})}});



// ===== ORGANIC SNAPSHOT v1 =====
function organicMetricValue(row,metricName){
  const headers=row?.metricHeaders||[];
  const values=row?.rows?.[0]?.metricValues||[];
  const index=headers.findIndex(h=>h?.name===metricName);
  if(index<0)return 0;
  const n=Number(values[index]?.value||0);
  return Number.isFinite(n)?n:0;
}

function organicIsoDate(value){
  return fxDateOnly(value||new Date());
}

async function fetchOrganicGa4Metrics(userId,propertyId,startDate,endDate){
  const cleanPropertyId=normalizePlatformAccountId(propertyId);
  if(!cleanPropertyId)throw Object.assign(new Error("Organic GA4 property id is required"),{status:400});
  const token=await getFreshOrganicAccessToken(userId);
  const url=`${GA4_DATA_API_BASE}/properties/${encodeURIComponent(cleanPropertyId)}:runReport`;
  const body={
    dateRanges:[{startDate,endDate}],
    metrics:[
      {name:"sessions"},
      {name:"addToCarts"},
      {name:"checkouts"},
      {name:"purchases"},
      {name:"purchaseRevenue"}
    ]
  };
  const r=await fetch(url,{method:"POST",headers:{Authorization:`Bearer ${token}`,Accept:"application/json","Content-Type":"application/json"},body:JSON.stringify(body)});
  const data=await r.json().catch(()=>({status:r.status}));
  if(!r.ok)throw new Error(data.error?.message||data.message||`Organic GA4 Data API failed ${r.status}`);
  return {
    sessions:organicMetricValue(data,"sessions"),
    add_to_cart:organicMetricValue(data,"addToCarts"),
    checkout:organicMetricValue(data,"checkouts"),
    purchase:organicMetricValue(data,"purchases"),
    revenue:organicMetricValue(data,"purchaseRevenue"),
    raw:data
  };
}

async function fetchOrganicSearchConsoleMetrics(userId,siteUrl,startDate,endDate){
  const cleanSiteUrl=String(siteUrl||"").trim();
  if(!cleanSiteUrl)throw Object.assign(new Error("Organic Search Console site url is required"),{status:400});
  const token=await getFreshOrganicAccessToken(userId);
  const url=`${SEARCH_CONSOLE_API_BASE}/sites/${encodeURIComponent(cleanSiteUrl)}/searchAnalytics/query`;
  const body={startDate,endDate,rowLimit:1};
  const r=await fetch(url,{method:"POST",headers:{Authorization:`Bearer ${token}`,Accept:"application/json","Content-Type":"application/json"},body:JSON.stringify(body)});
  const data=await r.json().catch(()=>({status:r.status}));
  if(!r.ok)throw new Error(data.error?.message||data.message||`Organic Search Console API failed ${r.status}`);
  const row=Array.isArray(data.rows)?(data.rows[0]||{}):{};
  const clicks=Number(row.clicks||0);
  const impressions=Number(row.impressions||0);
  const ctr=row.ctr===undefined||row.ctr===null?null:Number(row.ctr)*100;
  const averagePosition=row.position===undefined||row.position===null?null:Number(row.position);
  return {
    clicks:Number.isFinite(clicks)?clicks:0,
    impressions:Number.isFinite(impressions)?impressions:0,
    ctr:Number.isFinite(ctr)?ctr:null,
    average_position:Number.isFinite(averagePosition)?averagePosition:null,
    raw:data
  };
}

function buildOrganicSnapshotPayload({snapshotDate,accountCurrency,ga4,gsc,property,site}){
  const sessions=Number(ga4.sessions||0);
  const addToCart=Number(ga4.add_to_cart||0);
  const checkout=Number(ga4.checkout||0);
  const purchase=Number(ga4.purchase||0);
  const revenue=Number(ga4.revenue||0);
  const clicks=Number(gsc.clicks||0);
  const impressions=Number(gsc.impressions||0);
  const ctr=gsc.ctr!==null&&gsc.ctr!==undefined?Number(gsc.ctr):(impressions>0?clicks/impressions*100:null);
  const abandoned=checkout>0?Math.max(checkout-purchase,0):0;
  return {
    platform:"organic",
    snapshot_date:snapshotDate,
    account_currency:accountCurrency||DEFAULT_REPORTING_CURRENCY,
    kpis:{
      spend:0,
      sales:revenue,
      revenue,
      impressions,
      clicks,
      sessions,
      ctr,
      cpc:null,
      roas:null
    },
    purchase_journey:{
      add_to_cart:addToCart,
      checkout,
      abandoned,
      purchase,
      purchases:purchase,
      purchase_value:revenue
    },
    click_journey:{
      ad_clicks:clicks,
      link_clicks:0,
      landing_page_views:0,
      sessions,
      traffic_score:null,
      real_cpc:null
    },
    performance_summary:{
      rows:[{
        platform:"Organic",
        level:"platform",
        campaign_id:"organic",
        campaign_name:"Organic",
        campaign_status:"active",
        currency:accountCurrency||DEFAULT_REPORTING_CURRENCY,
        impressions,
        clicks,
        ad_clicks:clicks,
        sessions,
        ctr,
        cpc:null,
        spend:0,
        sales:revenue,
        revenue,
        roas:null,
        add_to_cart:addToCart,
        checkout,
        purchase,
        purchases:purchase,
        purchase_count:purchase,
        abandoned,
        conversion_value:revenue,
        conversions:purchase,
        raw:{
          source:"organic_snapshot_v1",
          ga4_property:property,
          search_console_site:site,
          gsc_average_position:gsc.average_position
        }
      }],
      counts:{platform:1},
      source_confidence:"organic_snapshot_v1",
      null_policy:"Organic Snapshot v1 uses GA4 for sessions/events/revenue and Search Console for impressions/clicks/ctr. Dataset spread is intentionally disabled in this patch.",
      raw_report:{ga4:ga4.raw,gsc:gsc.raw}
    }
  };
}

async function writeOrganicSnapshotV1({user,datePreset="today",snapshotDate=null,captureReason="manual_refresh",snapshotClass="primary"}){
  const conn=await getConnection(user.id,"organic");
  if(!conn)throw Object.assign(new Error("Organic not connected"),{status:404});
  if(!conn.metadata?.configured)throw Object.assign(new Error("Organic property and site binding is required before snapshot"),{status:400});
  const property=conn.metadata.selectedGa4Property||{};
  const site=conn.metadata.selectedSearchConsoleSite||{};
  const platformAccountId=normalizePlatformAccountId(conn.account_id||property.property_id||conn.metadata.selectedPlatformAccountId);
  if(!platformAccountId)throw Object.assign(new Error("Organic GA4 property id is missing"),{status:400});
  await requireActiveOwnership(user.id,"organic",platformAccountId);
  const platformTimeZone=DEFAULT_PLATFORM_TIMEZONE;
  const effectiveSnapshotDate=e2aSnapshotDate(snapshotDate,platformTimeZone);
  const period=resolveSnapshotCapturePeriod(datePreset,effectiveSnapshotDate,platformTimeZone,new Date());
  const timeSync=resolveAdminTimeSync(new Date(),platformTimeZone);
  const startDate=organicIsoDate(period.start);
  const endDate=organicIsoDate(period.end);
  const [ga4,gsc]=await Promise.all([
    fetchOrganicGa4Metrics(user.id,platformAccountId,startDate,endDate),
    fetchOrganicSearchConsoleMetrics(user.id,site.site_url,startDate,endDate)
  ]);
  const accountCurrency=await getUserAccountCurrency(user.id);
  const snapshot=buildOrganicSnapshotPayload({snapshotDate:effectiveSnapshotDate,accountCurrency,ga4,gsc,property,site});
  const fx=await resolveFxRate(accountCurrency,accountCurrency,{rateDate:snapshot.snapshot_date});
  const convertedSnapshot=applyFxToSnapshotPayload(snapshot,fx);
  const existingVersionResult=await supabaseAdmin.from("dashboard_snapshots").select("snapshot_version").eq("user_id",user.id).eq("platform","organic").eq("platform_account_id",platformAccountId).eq("snapshot_date",convertedSnapshot.snapshot_date).order("snapshot_version",{ascending:false}).limit(1).maybeSingle();
  if(existingVersionResult.error)throw existingVersionResult.error;
  const snapshotVersion=Number(existingVersionResult.data?.snapshot_version||0)+1;
  const now=new Date().toISOString();
  const row={
    user_id:user.id,
    platform:"organic",
    platform_account_id:platformAccountId,
    platform_base_currency:accountCurrency,
    snapshot_version:snapshotVersion,
    source_job_id:null,
    date_preset:period.datePreset,
    snapshot_period_start:period.start,
    snapshot_period_end:period.end,
    snapshot_scope:period.scope||period.datePreset,
    capture_reason:captureReason,
    snapshot_class:snapshotClass,
    platform_account_timezone:platformTimeZone,
    platform_business_date:timeSync.platform_business_date,
    platform_business_at:timeSync.platform_business_at||timeSync.server_time_utc,
    platform_business_hour:timeSync.platform_business_hour,
    data_maturity_window_hours:dataMaturityWindowHours("organic"),
    server_time_utc:timeSync.server_time_utc,
    istanbul_time:timeSync.istanbul_time,
    platform_account_time:timeSync.platform_account_time,
    time_engine_version:TIME_ENGINE_VERSION,
    fx_rate:fx.fx_rate,
    fx_provider:fx.fx_provider,
    fx_rate_timestamp:fx.fx_rate_timestamp,
    fx_rate_date:fx.fx_rate_date||null,
    fx_source_currency:fx.fx_source_currency,
    fx_target_currency:fx.fx_target_currency,
    fx_engine_version:fx.fx_engine_version,
    snapshot_date:convertedSnapshot.snapshot_date,
    snapshot_created_at:now,
    account_currency:convertedSnapshot.account_currency,
    kpis:convertedSnapshot.kpis,
    purchase_journey:convertedSnapshot.purchase_journey,
    click_journey:convertedSnapshot.click_journey,
    performance_summary:convertedSnapshot.performance_summary
  };
  const {data,error}=await supabaseAdmin.from("dashboard_snapshots").insert(row).select("id,user_id,platform,platform_account_id,snapshot_version,date_preset,snapshot_date,snapshot_created_at,account_currency,kpis,purchase_journey,click_journey,performance_summary").maybeSingle();
  if(error)throw error;
  return {ok:true,platform:"organic",mode:"snapshot_insert_only",dataset_spread:false,snapshot:data,row_counts:convertedSnapshot.performance_summary.counts};
}

app.post("/api/organic/snapshot",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;res.json(await writeOrganicSnapshotV1({user,datePreset:String(req.body?.date_preset||req.body?.dateRange||req.query.date_preset||req.query.dateRange||"today"),snapshotDate:req.body?.snapshot_date||req.query.snapshot_date||null,captureReason:req.body?.capture_reason||"manual_refresh",snapshotClass:req.body?.snapshot_class||"primary"}))}catch(e){res.status(e.status||500).json({ok:false,error:e.message,stage:"organic_snapshot_v1"})}});
app.post("/api/platform/organic/snapshot",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;res.json(await writeOrganicSnapshotV1({user,datePreset:String(req.body?.date_preset||req.body?.dateRange||req.query.date_preset||req.query.dateRange||"today"),snapshotDate:req.body?.snapshot_date||req.query.snapshot_date||null,captureReason:req.body?.capture_reason||"manual_refresh",snapshotClass:req.body?.snapshot_class||"primary"}))}catch(e){res.status(e.status||500).json({ok:false,error:e.message,stage:"organic_snapshot_v1"})}});
// ===== END ORGANIC SNAPSHOT v1 =====

// ===== END ORGANIC GOOGLE OAUTH + DISCOVERY v1 =====
function pinterestBasic(){return Buffer.from(`${process.env.PINTEREST_CLIENT_ID}:${process.env.PINTEREST_CLIENT_SECRET}`).toString("base64")}
app.get("/auth/pinterest",async(req,res)=>{
  const status=passiveLegacyPlatformStatus("pinterest");
  res.redirect(`/dashboard?pinterest_legacy=1&platform_status=${encodeURIComponent(status.status)}&message=${encodeURIComponent(status.message)}`);
});
app.get("/auth/pinterest/callback",async(req,res)=>{
  req.session.pinterestOAuthState=null;
  req.session.oauthUserId=null;
  const status=passiveLegacyPlatformStatus("pinterest");
  res.redirect(`/dashboard?pinterest_legacy=1&platform_status=${encodeURIComponent(status.status)}&message=${encodeURIComponent(status.message)}`);
});

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

async function resolveKlaviyoAccountIdentity(conn){
  let raw=null;
  try{
    raw=await klaviyoFetch(conn,"/api/accounts/");
  }catch(e){
    raw={error:e.message};
  }
  const item=Array.isArray(raw?.data)?raw.data[0]:raw?.data;
  const attr=item?.attributes||{};
  const id=String(item?.id||attr.account_id||conn?.account_id||conn?.metadata?.accountId||conn?.metadata?.account_id||"").trim();
  const fallbackId=id||`klaviyo_${String(conn?.user_id||"account").slice(0,8)}`;
  return {
    platform_account_id:fallbackId,
    account_name:attr.name||attr.company_name||conn?.account_name||`Klaviyo Account ${fallbackId}`,
    currency:conn?.metadata?.spendCurrency||conn?.metadata?.currency||null,
    raw_account:raw
  };
}

async function bootstrapKlaviyoLifecycle(userId,conn,source="oauth_callback"){
  const account=await resolveKlaviyoAccountIdentity(conn||{});
  const now=new Date().toISOString();
  await saveConnection(userId,"klaviyo",{
    accountId:account.platform_account_id,
    accountName:account.account_name,
    metadata:{
      ...(conn?.metadata||{}),
      selectedPlatformAccountId:account.platform_account_id,
      lastOwnedPlatformAccountId:account.platform_account_id,
      accountResolutionSource:source,
      accountResolutionAt:now,
      rawAccount:account.raw_account
    }
  });
  const refreshedConn=await getConnection(userId,"klaviyo").catch(()=>conn);
  const ownership=await ensurePlatformOwnership(userId,"klaviyo",{
    platform_account_id:account.platform_account_id,
    account_name:account.account_name,
    name:account.account_name,
    currency:account.currency||refreshedConn?.metadata?.spendCurrency||null,
    metadata:{source,accountResolutionAt:now,raw_account:account.raw_account}
  });
  const schedule=await ensureSnapshotSchedule(userId,"klaviyo",account.platform_account_id,{
    bootstrapSource:source,
    bootstrappedAt:now,
    account_type:phase1ReportableAccountType("klaviyo")
  });
  return {ok:true,platform:"klaviyo",platform_account_id:account.platform_account_id,account_name:account.account_name,ownership,schedule};
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
app.get("/auth/klaviyo/callback",async(req,res)=>{try{const{code,state,error,error_description}=req.query;if(error)return res.redirect(`/dashboard?klaviyo_error=${encodeURIComponent(error_description||error)}`);if(!code)return res.redirect("/dashboard?klaviyo_error=missing_code");if(!state||state!==req.session.klaviyoOAuthState)return res.redirect("/dashboard?klaviyo_error=invalid_state");const userId=req.session.oauthUserId;if(!userId)return res.redirect("/dashboard?klaviyo_error=missing_user_id");const verifier=req.session.klaviyoCodeVerifier;if(!verifier)return res.redirect("/dashboard?klaviyo_error=missing_code_verifier");const body=new URLSearchParams({grant_type:"authorization_code",code,redirect_uri:process.env.KLAVIYO_REDIRECT_URI,code_verifier:verifier});const r=await fetch(`${KLAVIYO_API_BASE}/oauth/token`,{method:"POST",headers:{Authorization:`Basic ${klaviyoBasic()}`,"Content-Type":"application/x-www-form-urlencoded"},body:body.toString()});const data=await r.json().catch(()=>({}));if(!r.ok||!data.access_token)throw new Error(data.error_description||data.error||data.message||"Klaviyo token exchange failed");await saveConnection(userId,"klaviyo",{accessToken:data.access_token,refreshToken:data.refresh_token||null,tokenExpiresAt:parseExpiry(data.expires_in),metadata:{scope:data.scope||klaviyoScopes(),tokenType:data.token_type||null,expiresIn:data.expires_in||null}});const klaviyoConn=await getConnection(userId,"klaviyo");
const klaviyoAccount=await resolveKlaviyoAccountIdentity(klaviyoConn);
await saveConnection(userId,"klaviyo",{accountId:klaviyoAccount.platform_account_id,accountName:klaviyoAccount.account_name,metadata:{selectedPlatformAccountId:null,lastDiscoveredPlatformAccountId:klaviyoAccount.platform_account_id,accountSelectionRequired:true,accountSelectionGuardVersion:"v1",rawAccount:klaviyoAccount.raw_account}});
req.session.klaviyoOAuthState=null;req.session.klaviyoCodeVerifier=null;res.redirect("/dashboard?klaviyo_connected=1&account_selection_required=1")}catch(e){res.redirect(`/dashboard?klaviyo_error=${encodeURIComponent(e.message)}`)}});
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



app.post("/api/disconnect/:platform",async(req,res)=>{
  try{
    const user=await requireUser(req,res);
    if(!user)return;
    const platform=String(req.params.platform||"").toLowerCase();
    if(!platform)return res.status(400).json({ok:false,error:"platform is required"});
    const result=await disconnectPlatformLifecycle(user.id,platform,{reason:req.body?.reason||"user_disconnect"});
    res.json(result);
  }catch(e){
    res.status(e.status||500).json({ok:false,error:e.message,stage:"disconnect_lifecycle"});
  }
});

app.post("/api/reconnect/:platform",async(req,res)=>{
  try{
    const user=await requireUser(req,res);
    if(!user)return;
    const platform=String(req.params.platform||"").toLowerCase();
    const platformAccountId=normalizePlatformAccountId(req.body?.platform_account_id||req.body?.adAccountId||req.query.platform_account_id);
    if(!platformAccountId)return res.status(400).json({ok:false,error:"platform_account_id is required"});
    const result=await reactivatePlatformLifecycle(user.id,platform,platformAccountId,"account_reconnect");
    res.json(result);
  }catch(e){
    res.status(e.status||500).json({ok:false,error:e.message,stage:"reconnect_lifecycle"});
  }
});

app.post("/api/lifecycle/reactivate",async(req,res)=>{
  try{
    const user=await requireUser(req,res);
    if(!user)return;
    const platform=String(req.body?.platform||req.query.platform||"meta").toLowerCase();
    const platformAccountId=normalizePlatformAccountId(req.body?.platform_account_id||req.body?.adAccountId||req.query.platform_account_id);
    if(!platformAccountId)return res.status(400).json({ok:false,error:"platform_account_id is required"});
    const result=await reactivatePlatformLifecycle(user.id,platform,platformAccountId,"account_reactivation");
    res.json(result);
  }catch(e){
    res.status(e.status||500).json({ok:false,error:e.message,stage:"reactivation_lifecycle"});
  }
});

app.get("/api/lifecycle/status",async(req,res)=>{
  try{
    const user=await requireUser(req,res);
    if(!user)return;
    const platform=String(req.query.platform||"meta").toLowerCase();
    const platformAccountId=normalizePlatformAccountId(req.query.platform_account_id||req.query.adAccountId||"");

    let ownershipQuery=supabaseAdmin
      .from("platform_account_ownerships")
      .select("*")
      .eq("owner_user_id",user.id)
      .eq("platform",platform)
      .order("updated_at",{ascending:false});
    if(platformAccountId)ownershipQuery=ownershipQuery.eq("platform_account_id",platformAccountId);

    let scheduleQuery=supabaseAdmin
      .from("snapshot_schedules")
      .select("*")
      .eq("user_id",user.id)
      .eq("platform",platform)
      .order("updated_at",{ascending:false});
    if(platformAccountId)scheduleQuery=scheduleQuery.eq("platform_account_id",platformAccountId);

    let jobQuery=supabaseAdmin
      .from("snapshot_jobs")
      .select("*")
      .eq("user_id",user.id)
      .eq("platform",platform)
      .order("created_at",{ascending:false})
      .limit(10);
    if(platformAccountId)jobQuery=jobQuery.eq("platform_account_id",platformAccountId);

    const [ownerships,schedules,jobs,connections]=await Promise.all([
      ownershipQuery,
      scheduleQuery,
      jobQuery,
      supabaseAdmin.from("platform_connections").select("platform,account_id,account_name,connected,updated_at,disconnected_at,disconnect_reason,lifecycle_version").eq("user_id",user.id).eq("platform",platform)
    ]);

    if(ownerships.error)throw ownerships.error;
    if(schedules.error)throw schedules.error;
    if(jobs.error)throw jobs.error;
    if(connections.error)throw connections.error;

    res.json({
      ok:true,
      platform,
      platform_account_id:platformAccountId||null,
      lifecycle_version:DISCONNECT_LIFECYCLE_VERSION,
      connections:connections.data||[],
      ownerships:ownerships.data||[],
      schedules:schedules.data||[],
      jobs:jobs.data||[]
    });
  }catch(e){
    res.status(e.status||500).json({ok:false,error:e.message,stage:"lifecycle_status"});
  }
});



// ===== DATASET SPREAD POLICY v1 =====
function perfNormalizePlatform(platform){const p=String(platform||"meta").toLowerCase();return p==="tik_tok"?"tiktok":p}

function shouldSpreadSnapshotToPerformanceDataset(snapshotOrClass){
  const cls=typeof snapshotOrClass==="string"?snapshotOrClass:String(snapshotOrClass?.snapshot_class||"");
  return cls.toLowerCase()!=="recovery";
}

// ===== PERFORMANCE SPREAD ENGINE v1 =====
const PERFORMANCE_SPREAD_ENGINE_VERSION="v1.1";
function perfHasValue(value){return value!==null&&value!==undefined&&value!==""}
function perfNullableNumber(value){if(!perfHasValue(value))return null;const n=Number(value);return Number.isFinite(n)?n:null}
function perfText(value){const s=String(value??"").trim();return s?s:null}
function perfDate(value){return value?String(value).slice(0,10):null}
function perfNormalizeLevel(level){const l=String(level||"").toLowerCase();if(l==="adset")return "adgroup";if(["campaign","adgroup","ad"].includes(l))return l;return null}
function perfCtrRatio(value){const n=perfNullableNumber(value);if(n===null)return null;return Math.abs(n)>1?n/100:n}
function perfEntityId(row,level){
  if(level==="campaign")return perfText(row.id_in_platform||row.campaign_id||row.id);
  if(level==="adgroup")return perfText(row.id_in_platform||row.adgroup_id||row.adset_id||row.id);
  if(level==="ad")return perfText(row.id_in_platform||row.ad_id||row.id);
  return perfText(row.id_in_platform||row.id||row.campaign_id||row.adgroup_id||row.adset_id||row.ad_id);
}
function perfParentId(row,level){
  if(level==="campaign")return null;
  if(level==="adgroup")return perfText(row.parent_id||row.campaign_id);
  if(level==="ad")return perfText(row.parent_id||row.adgroup_id||row.adset_id||row.campaign_id);
  return perfText(row.parent_id);
}
function perfEntityName(row,level){
  if(level==="campaign")return perfText(row.name||row.campaign_name);
  if(level==="adgroup")return perfText(row.name||row.adgroup_name||row.adset_name);
  if(level==="ad")return perfText(row.name||row.ad_name);
  return perfText(row.name);
}
function perfEntityStatus(row,level){
  if(level==="campaign")return perfText(row.status||row.campaign_status);
  if(level==="adgroup")return perfText(row.status||row.adgroup_status||row.adset_status);
  if(level==="ad")return perfText(row.status||row.ad_status);
  return perfText(row.status);
}
function performanceDatasetRowFromSnapshotRow(snapshot,row){
  const level=perfNormalizeLevel(row?.level);
  if(!level)return null;
  const idInPlatform=perfEntityId(row,level);
  if(!idInPlatform)return null;

  const platform=perfNormalizePlatform(snapshot.platform||row.platform);
  const spend=perfNullableNumber(row.spend);
  const revenue=perfNullableNumber(row.revenue);
  const sales=perfNullableNumber(row.sales);
  const conversionValue=perfNullableNumber(row.conversion_value??row.purchase_value);
  const adClicks=perfNullableNumber(row.ad_clicks??row.clicks??(platform==="klaviyo"?row.link_clicks:null));
  const addToCart=perfNullableNumber(row.add_to_cart);
  const checkout=perfNullableNumber(row.checkout);
  const purchase=perfNullableNumber(row.purchase??row.purchases??row.purchase_count??row.conversions);
  const abandoned=checkout!==null&&purchase!==null?Math.max(checkout-purchase,0):perfNullableNumber(row.abandoned);
  const profit=sales!==null&&spend!==null?sales-spend:(revenue!==null&&spend!==null?revenue-spend:null);
  const cps=spend!==null&&purchase!==null&&purchase>0?spend/purchase:null;

  return {
    snapshot_id:snapshot.id,
    user_id:snapshot.user_id,
    platform,
    platform_account_id:perfText(snapshot.platform_account_id),
    level,
    id_in_platform:idInPlatform,
    parent_id:perfParentId(row,level),
    name:perfEntityName(row,level),
    status:perfEntityStatus(row,level),
    currency:perfText(row.currency||snapshot.account_currency),
    date_start:perfDate(snapshot.snapshot_period_start),
    date_end:perfDate(snapshot.snapshot_period_end),
    date_range:perfText(snapshot.date_preset),
    snapshot_date:perfDate(snapshot.snapshot_date),
    platform_business_at:snapshot.platform_business_at||snapshot.server_time_utc||snapshot.snapshot_created_at||null,
    platform_account_timezone:perfText(snapshot.platform_account_timezone),
    server_time_utc:snapshot.server_time_utc||null,
    time_engine_version:perfText(snapshot.time_engine_version||TIME_ENGINE_VERSION),
    spend,
    impressions:perfNullableNumber(row.impressions),
    clicks:adClicks,
    ad_clicks:adClicks,
    ctr:perfCtrRatio(row.ctr),
    cpc:perfNullableNumber(row.cpc),
    sales,
    revenue,
    roas:perfNullableNumber(row.roas),
    conversions:purchase,
    conversion_value:conversionValue!==null?conversionValue:revenue,
    add_to_cart:addToCart,
    checkout,
    purchase,
    abandoned,
    profit,
    cps,
    fx_rate:perfNullableNumber(snapshot.fx_rate),
    fx_rate_date:perfDate(snapshot.fx_rate_date),
    fx_provider:perfText(snapshot.fx_provider),
    fx_source_currency:perfText(snapshot.fx_source_currency),
    fx_target_currency:perfText(snapshot.fx_target_currency),
    fx_engine_version:perfText(snapshot.fx_engine_version),
    raw:{
      ...((row&&typeof row.raw==="object")?row.raw:{}),
      performance_dataset_source_row:row,
      performance_spread_engine_version:PERFORMANCE_SPREAD_ENGINE_VERSION,
      ctr_storage_standard:"ratio",
      zero_null_policy:"0 is measured zero; null is unknown/unavailable/not computable",
      kpi_normalization_policy:"reach/link_click/landing_page_view excluded from dataset; klaviyo link_click normalized into ad_clicks"
    }
  };
}
async function spreadSnapshotToPerformanceDataset(snapshot){
  if(!snapshot?.id)throw new Error("snapshot.id is required for Performance Spread");
  const rows=Array.isArray(snapshot.performance_summary?.rows)?snapshot.performance_summary.rows:[];
  const {error:deleteError}=await supabaseAdmin
    .from("performance_dataset_rows")
    .delete()
    .eq("snapshot_id",snapshot.id);
  if(deleteError)throw deleteError;
  const datasetRows=rows.map(row=>performanceDatasetRowFromSnapshotRow(snapshot,row)).filter(Boolean);
  if(!datasetRows.length){
    return {ok:true,performance_spread_engine_version:PERFORMANCE_SPREAD_ENGINE_VERSION,snapshot_id:snapshot.id,rows:0,source_rows:rows.length};
  }
  const {data,error}=await supabaseAdmin
    .from("performance_dataset_rows")
    .insert(datasetRows)
    .select("id");
  if(error)throw error;
  return {ok:true,performance_spread_engine_version:PERFORMANCE_SPREAD_ENGINE_VERSION,snapshot_id:snapshot.id,rows:(data||[]).length,source_rows:rows.length};
}
// ===== END PERFORMANCE SPREAD ENGINE v1 =====



// ===== PHASE E.2A META SNAPSHOT WRITE =====
function e2aNumber(value){
  const n=Number(value);
  return Number.isFinite(n)?n:0;
}

function e2aNullableNumber(value){
  if(value===null||value===undefined||value==="")return null;
  const n=Number(value);
  return Number.isFinite(n)?n:null;
}

function e2aSum(rows,field){
  return (rows||[]).reduce((total,row)=>total+e2aNumber(row?.[field]),0);
}

function e2aWeightedAverage(rows,valueField,weightField){
  let weighted=0,weight=0;
  for(const row of rows||[]){
    const value=e2aNullableNumber(row?.[valueField]);
    const w=e2aNumber(row?.[weightField]);
    if(value!==null&&w>0){
      weighted+=value*w;
      weight+=w;
    }
  }
  return weight>0?weighted/weight:null;
}

function e2aSnapshotDate(value,timeZone=DEFAULT_PLATFORM_TIMEZONE){
  if(value)return String(value).slice(0,10);
  return timePartsInZone(new Date(),timeZone).date;
}

function e2aBuildMetaSnapshot({snapshotDate,accountCurrency,campaignRows,adsetRows,adRows}){
  const rows=[...(campaignRows||[]),...(adsetRows||[]),...(adRows||[])];
  const aggregateRows=(campaignRows&&campaignRows.length)?campaignRows:rows;

  const spend=e2aSum(aggregateRows,"spend");
  const revenue=e2aSum(aggregateRows,"revenue");
  const sales=revenue;
  const impressions=e2aSum(aggregateRows,"impressions");
  const reach=e2aSum(aggregateRows,"reach");
  const clicks=e2aSum(aggregateRows,"clicks");
  const linkClicks=e2aSum(aggregateRows,"link_clicks");
  const landingPageViews=e2aSum(aggregateRows,"landing_page_views");
  const addToCart=e2aSum(aggregateRows,"add_to_cart");
  const checkout=e2aSum(aggregateRows,"checkout");
  const purchase=e2aSum(aggregateRows,"purchase");
  const abandoned=Math.max(checkout-purchase,0);

  const ctr=clicks>0&&impressions>0?(clicks/impressions)*100:e2aWeightedAverage(aggregateRows,"ctr","impressions");
  const cpc=clicks>0?spend/clicks:e2aWeightedAverage(aggregateRows,"cpc","clicks");
  const roas=spend>0?revenue/spend:null;
  const trafficScore=linkClicks>0?(landingPageViews/linkClicks)*100:null;
  const realCpc=landingPageViews>0?spend/landingPageViews:null;

  const normalizeRow=row=>({
    platform:row.platform||"Meta",
    level:row.level||null,
    campaign_id:row.campaign_id||null,
    campaign_name:row.campaign_name||null,
    adset_id:row.adset_id||null,
    adset_name:row.adset_name||null,
    ad_id:row.ad_id||null,
    ad_name:row.ad_name||null,
    currency:row.currency||accountCurrency||null,
    spend:e2aNumber(row.spend),
    sales:e2aNumber(row.sales??row.revenue),
    revenue:e2aNumber(row.revenue??row.sales),
    impressions:e2aNumber(row.impressions),
    reach:e2aNumber(row.reach),
    clicks:e2aNumber(row.clicks),
    ctr:e2aNullableNumber(row.ctr),
    cpc:e2aNullableNumber(row.cpc),
    roas:e2aNullableNumber(row.roas),
    link_clicks:e2aNumber(row.link_clicks),
    landing_page_views:e2aNumber(row.landing_page_views),
    add_to_cart:e2aNumber(row.add_to_cart),
    checkout:e2aNumber(row.checkout),
    purchase:e2aNumber(row.purchase),
    abandoned:e2aNumber(row.abandoned),
    raw:row.raw||{}
  });

  return {
    snapshot_date:snapshotDate,
    account_currency:accountCurrency||null,
    kpis:{
      spend,
      sales,
      revenue,
      impressions,
      reach,
      clicks,
      ctr,
      cpc,
      roas
    },
    purchase_journey:{
      add_to_cart:addToCart,
      checkout,
      purchase,
      abandoned
    },
    click_journey:{
      ad_clicks:clicks,
      link_clicks:linkClicks,
      landing_page_views:landingPageViews,
      traffic_score:trafficScore,
      real_cpc:realCpc
    },
    performance_summary:{
      rows:rows.map(normalizeRow),
      counts:{
        campaign:(campaignRows||[]).length,
        adset:(adsetRows||[]).length,
        ad:(adRows||[]).length,
        total:rows.length
      }
    }
  };
}

function e2aZeroMetaEntityRow(entity,level,accountCurrency=null){
  const row={
    platform:"Meta",
    level,
    campaign_id:entity.campaign_id||entity.id||null,
    campaign_name:entity.campaign_name||(level==="campaign"?entity.name:null)||null,
    campaign_status:level==="campaign"?(entity.effective_status||entity.status||null):null,
    adset_id:entity.adset_id||(level==="adset"?entity.id:null)||null,
    adset_name:entity.adset_name||(level==="adset"?entity.name:null)||null,
    adset_status:level==="adset"?(entity.effective_status||entity.status||null):null,
    ad_id:level==="ad"?entity.id:null,
    ad_name:level==="ad"?entity.name:null,
    ad_status:level==="ad"?(entity.effective_status||entity.status||null):null,
    currency:accountCurrency||null,
    impressions:0, reach:null, clicks:0, ctr:null, cpc:null, spend:0,
    link_clicks:0, landing_page_views:0, add_to_cart:0, checkout:0, purchase:0, purchases:0,
    abandoned:0, sales:null, revenue:null, roas:null,
    raw:{entity,entity_fallback:true,reason:"No insights row returned for selected date range; entity row preserved with zero/unknown metrics."}
  };
  if(level==="ad"&&entity.adset_id&&!row.adset_name)row.adset_name=null;
  return row;
}
async function e2aFetchMetaEntityRowsForLevel(conn,adAccountId,level,limit,accountCurrency=null){
  let endpoint="campaigns", fields="id,name,status,effective_status";
  if(level==="adset"){endpoint="adsets";fields="id,name,status,effective_status,campaign_id"}
  if(level==="ad"){endpoint="ads";fields="id,name,status,effective_status,adset_id,campaign_id"}
  const data=await metaGraph(`/${adAccountId}/${endpoint}`,{fields,limit},conn.access_token);
  return (data.data||[]).map(entity=>e2aZeroMetaEntityRow(entity,level,accountCurrency));
}
async function e2aFetchMetaInsightsForLevel(conn,adAccountId,level,datePreset,limit){
  const fields=["campaign_id","campaign_name","account_currency","impressions","reach","clicks","ctr","cpc","spend","actions","action_values","cost_per_action_type","conversion_rate_ranking"];
  if(level==="adset")fields.splice(2,0,"adset_id","adset_name");
  if(level==="ad")fields.splice(2,0,"adset_id","adset_name","ad_id","ad_name");

  const data=await metaGraph(`/${adAccountId}/insights`,{
    level,
    date_preset:datePreset,
    fields:fields.join(","),
    limit
  },conn.access_token);

  const insightRows=(data.data||[]).map(row=>normalizeMetaInsight(row,level));
  if(insightRows.length)return insightRows;
  const accountCurrency=(data.data||[]).find(r=>r.account_currency)?.account_currency||null;
  return e2aFetchMetaEntityRowsForLevel(conn,adAccountId,level,limit,accountCurrency);
}

async function resolveMetaRefreshAccount(user,conn,requestedAccountId){
  const requested=normalizePlatformAccountId(requestedAccountId);
  if(requested){
    const existingAdAccount=await supabaseAdmin
      .from("platform_ad_accounts")
      .select("platform_account_id,account_name,currency,metadata")
      .eq("user_id",user.id)
      .eq("platform","meta")
      .eq("platform_account_id",requested)
      .maybeSingle();
    if(existingAdAccount.error)throw existingAdAccount.error;
    const account={
      id:requested,
      platform_account_id:requested,
      name:existingAdAccount.data?.account_name||conn.account_name||requested,
      account_name:existingAdAccount.data?.account_name||conn.account_name||requested,
      currency:existingAdAccount.data?.currency||conn.metadata?.baseCurrency||null,
      ...(existingAdAccount.data?.metadata||{})
    };
    await ensurePlatformOwnership(user.id,"meta",account);
    return {platformAccountId:requested,account};
  }

  const storedId=normalizePlatformAccountId(conn.account_id||conn.metadata?.lastOwnedPlatformAccountId||conn.metadata?.selectedPlatformAccountId);
  if(storedId){
    const account={
      id:storedId,
      platform_account_id:storedId,
      name:conn.account_name||storedId,
      account_name:conn.account_name||storedId,
      currency:conn.metadata?.baseCurrency||null
    };
    await ensurePlatformOwnership(user.id,"meta",account);
    return {platformAccountId:storedId,account};
  }

  const err=new Error("Meta account selection is required before refresh. Select up to 3 ad accounts to continue.");
  err.status=409;
  err.code="ACCOUNT_SELECTION_REQUIRED";
  throw err;
}


function resolveSnapshotCapturePeriod(datePreset,snapshotDate,platformTimeZone=DEFAULT_PLATFORM_TIMEZONE,nowDate=new Date()){
  const normalized=String(datePreset||"today").trim().toLowerCase();
  const nowParts=timePartsInZone(nowDate,platformTimeZone);
  const baseDate=snapshotDate?String(snapshotDate).slice(0,10):nowParts.date;

  if(normalized==="last_7d" || normalized==="last_7_days"){
    return {
      datePreset:"last_7d",
      start:addUtcDays(baseDate,-6),
      end:baseDate,
      scope:"recovery_last_7d",
      snapshotClass:"recovery"
    };
  }

  if(normalized==="yesterday"){
    const y=addUtcDays(baseDate,-1);
    return {
      datePreset:"yesterday",
      start:y,
      end:y,
      scope:"yesterday",
      snapshotClass:"primary"
    };
  }

  if(normalized==="day_close"){
    const y=addUtcDays(baseDate,-1);
    return {
      datePreset:"yesterday",
      start:y,
      end:y,
      scope:"final_day_close",
      snapshotClass:"primary"
    };
  }

  return {
    datePreset:"today",
    start:baseDate,
    end:baseDate,
    scope:"today",
    snapshotClass:"primary"
  };
}

function resolveAutoRefreshPolicy({date=new Date(),platformTimeZone=DEFAULT_PLATFORM_TIMEZONE,platform="meta"}={}){
  const sync=resolveAdminTimeSync(date,platformTimeZone);
  const platformParts=timePartsInZone(date,platformTimeZone);
  const utcHour=date.getUTCHours();
  const isAutomationHour=AUTOMATION_PLATFORM_HOURS.includes(utcHour);
  const maturityHours=dataMaturityWindowHours(platform);
  const isRecoveryHour=utcHour===4;

  return {
    ...sync,
    hour:utcHour,
    utc_hour:utcHour,
    minute:date.getUTCMinutes(),
    platform_minute:platformParts.minute,
    isAutomationHour,
    automation_hours:AUTOMATION_PLATFORM_HOURS,
    datePreset:"today",
    captureReason:"automation_today",
    snapshotClass:"primary",
    shouldRunRecoverySnapshot:isRecoveryHour,
    recoveryDatePreset:"last_7d",
    recoveryCaptureReason:"automation_recovery",
    recoverySnapshotClass:"recovery",
    data_maturity_window_hours:maturityHours
  };
}

async function writeMetaSnapshotImmutable({user,conn,adAccountId,datePreset="today",snapshotDate,limit="100",sourceJobId=null,captureReason="manual_refresh",platformTimeZone=null,snapshotClass=null,adminTimeSync=null}){
  const platformAccountId=normalizePlatformAccountId(adAccountId);
  const ownership=await requireActiveOwnership(user.id,"meta",platformAccountId);
  const resolvedTimeZone=normalizeTimeZone(platformTimeZone||await getPlatformAccountTimezone(user.id,"meta",platformAccountId,conn,ownership));
  const timeSync=adminTimeSync||resolveAdminTimeSync(new Date(),resolvedTimeZone);
  const effectiveSnapshotDate=e2aSnapshotDate(snapshotDate,resolvedTimeZone);
  const period=resolveSnapshotCapturePeriod(datePreset,effectiveSnapshotDate,resolvedTimeZone,new Date());
  const normalizedDatePreset=period.datePreset;
  const resolvedSnapshotClass=snapshotClass||period.snapshotClass||"primary";

  const campaignRows=await e2aFetchMetaInsightsForLevel(conn,platformAccountId,"campaign",normalizedDatePreset,limit);
  const adsetRows=await e2aFetchMetaInsightsForLevel(conn,platformAccountId,"adset",normalizedDatePreset,limit);
  const adRows=await e2aFetchMetaInsightsForLevel(conn,platformAccountId,"ad",normalizedDatePreset,limit);

  const platformBaseCurrency=
    ownership.base_currency||
    campaignRows.find(r=>r.currency)?.currency||
    adsetRows.find(r=>r.currency)?.currency||
    adRows.find(r=>r.currency)?.currency||
    null;
  const accountCurrency=await getUserAccountCurrency(user.id)||normalizeCurrency(platformBaseCurrency)||DEFAULT_REPORTING_CURRENCY;
  const fx=await resolveFxRate(platformBaseCurrency,accountCurrency,{rateDate:effectiveSnapshotDate});

  const rawSnapshot=e2aBuildMetaSnapshot({
    snapshotDate:effectiveSnapshotDate,
    accountCurrency:platformBaseCurrency,
    campaignRows,
    adsetRows,
    adRows
  });

  const snapshot=applyFxToSnapshotPayload(rawSnapshot,fx);

  const existingVersionResult=await supabaseAdmin
    .from("dashboard_snapshots")
    .select("snapshot_version")
    .eq("user_id",user.id)
    .eq("platform","meta")
    .eq("platform_account_id",platformAccountId)
    .eq("snapshot_date",snapshot.snapshot_date)
    .order("snapshot_version",{ascending:false})
    .limit(1)
    .maybeSingle();
  if(existingVersionResult.error)throw existingVersionResult.error;
  const snapshotVersion=Number(existingVersionResult.data?.snapshot_version||0)+1;
  const now=new Date().toISOString();

  const row={
    user_id:user.id,
    platform:"meta",
    platform_account_id:platformAccountId,
    platform_base_currency:platformBaseCurrency,
    snapshot_version:snapshotVersion,
    source_job_id:sourceJobId,
    date_preset:normalizedDatePreset,
    snapshot_period_start:period.start,
    snapshot_period_end:period.end,
    snapshot_scope:period.scope,
    capture_reason:captureReason,
    snapshot_class:resolvedSnapshotClass,
    platform_account_timezone:resolvedTimeZone,
    platform_business_date:timeSync.platform_business_date,
    platform_business_at:timeSync.platform_business_at||timeSync.server_time_utc,
    platform_business_hour:timeSync.platform_business_hour,
    data_maturity_window_hours:dataMaturityWindowHours("meta"),
    server_time_utc:timeSync.server_time_utc,
    istanbul_time:timeSync.istanbul_time,
    platform_account_time:timeSync.platform_account_time,
    time_engine_version:TIME_ENGINE_VERSION,
    fx_rate:fx.fx_rate,
    fx_provider:fx.fx_provider,
    fx_rate_timestamp:fx.fx_rate_timestamp,
    fx_rate_date:fx.fx_rate_date||null,
    fx_source_currency:fx.fx_source_currency,
    fx_target_currency:fx.fx_target_currency,
    fx_engine_version:fx.fx_engine_version,
    snapshot_date:snapshot.snapshot_date,
    snapshot_created_at:now,
    account_currency:snapshot.account_currency,
    kpis:snapshot.kpis,
    purchase_journey:snapshot.purchase_journey,
    click_journey:snapshot.click_journey,
    performance_summary:snapshot.performance_summary
  };

  const {data,error}=await supabaseAdmin
    .from("dashboard_snapshots")
    .insert(row)
    .select("id,user_id,platform,platform_account_id,platform_base_currency,snapshot_version,source_job_id,date_preset,snapshot_period_start,snapshot_period_end,snapshot_scope,capture_reason,snapshot_class,platform_account_timezone,platform_business_date,platform_business_at,platform_business_hour,data_maturity_window_hours,server_time_utc,istanbul_time,platform_account_time,time_engine_version,fx_rate,fx_provider,fx_rate_timestamp,fx_rate_date,fx_source_currency,fx_target_currency,fx_engine_version,snapshot_date,snapshot_created_at,account_currency,kpis,purchase_journey,click_journey,performance_summary")
    .maybeSingle();
  if(error)throw error;

  let performance_spread_result=null;
  if(shouldSpreadSnapshotToPerformanceDataset(data)){
    try{
      performance_spread_result=await spreadSnapshotToPerformanceDataset(data);
    }catch(performanceSpreadError){
      performance_spread_result={ok:false,error:performanceSpreadError.message};
    }
  }else{
    performance_spread_result={ok:true,skipped:true,reason:"recovery_snapshot_not_written_to_dataset",snapshot_id:data.id};
  }

  return {mode:"insert",snapshot:data,row_counts:snapshot.performance_summary.counts,performance_spread_result};
}

async function handleMetaSnapshotWrite(req,res){
  let job=null;
  let stage="connection";
  try{
    const result=await requireConnection(req,res,"meta");
    if(!result)return;

    const {user,conn}=result;
    const requestedAdAccountId=req.body?.adAccountId||req.body?.ad_account_id||req.query.adAccountId||req.query.ad_account_id;

    stage="ownership";
    const resolved=await resolveMetaRefreshAccount(user,conn,requestedAdAccountId);
    const platformAccountId=normalizePlatformAccountId(resolved.platformAccountId);
    if(!platformAccountId)return res.status(400).json({ok:false,error:"Missing Meta ad account id",stage});

    const platformTimeZone=await getPlatformAccountTimezone(user.id,"meta",platformAccountId,conn,null);
    const adminTimeSync=resolveAdminTimeSync(new Date(),platformTimeZone);
    const datePreset="today";
    const snapshotDate=e2aSnapshotDate(req.body?.snapshot_date||req.query.snapshot_date,platformTimeZone);
    const limit=String(req.body?.limit||req.query.limit||"100");

    stage="job";
    job=await createRefreshJob(user.id,"meta",platformAccountId,{trigger:"manual",datePreset,snapshotDate,limit,captureReason:"manual_refresh",snapshotClass:"primary",...adminTimeSync,timeEngineVersion:TIME_ENGINE_VERSION});
    await setRefreshJobStatus(job.id,"running");

    stage="meta_api";
    const writeResult=await writeMetaSnapshotImmutable({user,conn,adAccountId:platformAccountId,datePreset,snapshotDate,limit,sourceJobId:job.id,captureReason:"manual_refresh",platformTimeZone,adminTimeSync,snapshotClass:"primary"});

    stage="snapshot";
    await setRefreshJobStatus(job.id,"completed",{snapshot_id:writeResult.snapshot?.id||null,metadata:{...(job.metadata||{}),performance_spread_result:writeResult.performance_spread_result||null}});

    res.json({
      ok:true,
      platform:"Meta",
      refresh_job:{id:job.id,status:"completed"},
      mode:writeResult.mode,
      snapshot_id:writeResult.snapshot?.id||null,
      snapshot_date:writeResult.snapshot?.snapshot_date||snapshotDate,
      snapshot_version:writeResult.snapshot?.snapshot_version||null,
      snapshot_class:writeResult.snapshot?.snapshot_class||null,
      platform_account_timezone:writeResult.snapshot?.platform_account_timezone||platformTimeZone,
      platform_business_date:writeResult.snapshot?.platform_business_date||null,
      platform_business_hour:writeResult.snapshot?.platform_business_hour??null,
      server_time_utc:writeResult.snapshot?.server_time_utc||adminTimeSync.server_time_utc,
      istanbul_time:writeResult.snapshot?.istanbul_time||adminTimeSync.istanbul_time,
      platform_account_time:writeResult.snapshot?.platform_account_time||adminTimeSync.platform_account_time,
      platform_account_id:writeResult.snapshot?.platform_account_id||platformAccountId,
      account_resolution_source:resolved.source,
      platform_base_currency:writeResult.snapshot?.platform_base_currency||null,
      account_currency:writeResult.snapshot?.account_currency||null,
      fx_rate:writeResult.snapshot?.fx_rate??null,
      fx_provider:writeResult.snapshot?.fx_provider||null,
      fx_rate_timestamp:writeResult.snapshot?.fx_rate_timestamp||null,
      fx_source_currency:writeResult.snapshot?.fx_source_currency||null,
      fx_target_currency:writeResult.snapshot?.fx_target_currency||null,
      fx_engine_version:writeResult.snapshot?.fx_engine_version||null,
      row_counts:writeResult.row_counts,
      performance_spread_result:writeResult.performance_spread_result||null,
      kpis:writeResult.snapshot?.kpis||{},
      purchase_journey:writeResult.snapshot?.purchase_journey||{},
      click_journey:writeResult.snapshot?.click_journey||{}
    });
  }catch(e){
    if(job?.id)await setRefreshJobStatus(job.id,"failed",{error_message:e.message}).catch(()=>null);
    res.status(e.status||500).json({ok:false,error:e.message,stage,job_id:job?.id||null});
  }
}
app.post("/api/snapshots/meta/write",handleMetaSnapshotWrite);

function makeRefreshCaptureRes(){
  return {
    statusCode:200,
    payload:null,
    headers:{},
    finished:false,
    status(code){this.statusCode=code;return this},
    json(data){this.payload=data;this.finished=true;return this},
    send(data){this.payload=data;this.finished=true;return this},
    redirect(url){this.statusCode=302;this.payload={redirect:url};this.finished=true;return this},
    set(field,value){this.headers[field]=value;return this}
  };
}

async function invokeRefreshHandler(handler,req){
  const capture=makeRefreshCaptureRes();
  await handler(req,capture);
  const ok=capture.statusCode>=200&&capture.statusCode<300&&capture.payload?.ok!==false;
  return {ok,status:capture.statusCode,data:capture.payload};
}


function makeSyntheticReqForUser(user,sourceReq={},extra={}){
  return {
    ...sourceReq,
    headers:sourceReq.headers||{},
    body:{...(sourceReq.body||{}),...extra.body},
    query:{...(sourceReq.query||{}),...extra.query},
    _resolvedUser:user
  };
}

async function getDefaultConnectionAccountId(userId,platform){
  const conn=await getConnection(userId,platform);
  const id=normalizePlatformAccountId(conn?.account_id||conn?.metadata?.selectedPlatformAccountId||conn?.metadata?.lastOwnedPlatformAccountId);
  if(id)return {conn,platformAccountId:id};
  const {data:ownership,error}=await supabaseAdmin
    .from("platform_account_ownerships")
    .select("platform_account_id,platform_account_name")
    .eq("owner_user_id",userId)
    .eq("platform",platform)
    .in("status",activeOwnershipStatuses())
    .order("updated_at",{ascending:false})
    .limit(1)
    .maybeSingle();
  if(error)throw error;
  const fallbackId=normalizePlatformAccountId(ownership?.platform_account_id);
  if(!fallbackId)throw new Error(`${platform} platform account id not found`);
  return {conn,platformAccountId:fallbackId};
}

async function runRefreshPlatform(platform,handler,req){
  try{
    return await invokeRefreshHandler(handler,req);
  }catch(e){
    return {ok:false,status:e.status||500,data:{ok:false,error:e.message,stage:e.stage||`${platform}_refresh`}};
  }
}

async function handleGlobalRefresh(req,res){
  const results={};
  const platforms=[
    ["meta",handleMetaSnapshotWrite],
    ["google",handleGoogleSnapshotWrite],
    ["tiktok",typeof handleTikTokSnapshotWrite==="function"?handleTikTokSnapshotWrite:null],
    ["klaviyo",typeof handleKlaviyoSnapshotWrite==="function"?handleKlaviyoSnapshotWrite:null]
  ];

  for(const [platform,handler] of platforms){
    if(!handler){
      results[platform]={ok:false,status:501,data:{ok:false,error:`${platform} refresh handler not implemented`,stage:"refresh_dispatcher"}};
      continue;
    }
    results[platform]=await runRefreshPlatform(platform,handler,req);
  }

  const completed=Object.entries(results).filter(([,r])=>r.ok).map(([platform])=>platform);
  const failed=Object.entries(results).filter(([,r])=>!r.ok).map(([platform,r])=>({platform,status:r.status,error:r.data?.error||"Refresh failed",stage:r.data?.stage||null}));
  const firstCompleted=completed[0]?results[completed[0]]?.data:{};

  res.status(completed.length?200:500).json({
    ok:completed.length>0,
    refresh_scope:"global",
    completed,
    failed,
    refresh_job:firstCompleted?.refresh_job||null,
    snapshot_id:firstCompleted?.snapshot_id||null,
    snapshot_date:firstCompleted?.snapshot_date||null,
    platforms:{
      meta:results.meta?.data||null,
      google:results.google?.data||null,
      tiktok:results.tiktok?.data||null,
      klaviyo:results.klaviyo?.data||null
    }
  });
}

app.post("/api/refresh/meta",handleGlobalRefresh);
app.post("/api/refresh/all",handleGlobalRefresh);
app.get("/api/refresh/all",handleGlobalRefresh);
app.get("/api/refresh/meta",handleGlobalRefresh);
app.get("/api/refresh/tiktok",handleTikTokSnapshotWrite);
app.post("/api/refresh/tiktok",handleTikTokSnapshotWrite);
app.get("/api/snapshots/tiktok/write",handleTikTokSnapshotWrite);
app.post("/api/snapshots/tiktok/write",handleTikTokSnapshotWrite);
app.get("/api/refresh/klaviyo",handleKlaviyoSnapshotWrite);
app.post("/api/refresh/klaviyo",handleKlaviyoSnapshotWrite);
app.get("/api/snapshots/klaviyo/write",handleKlaviyoSnapshotWrite);
app.post("/api/snapshots/klaviyo/write",handleKlaviyoSnapshotWrite);

app.get("/api/refresh/status",async(req,res)=>{
  try{
    const user=await requireUser(req,res);
    if(!user)return;
    let q=supabaseAdmin.from("snapshot_jobs").select("*").eq("user_id",user.id).order("created_at",{ascending:false}).limit(Number(req.query.limit||10));
    if(req.query.platform)q=q.eq("platform",String(req.query.platform));
    if(req.query.platform_account_id)q=q.eq("platform_account_id",String(req.query.platform_account_id));
    const {data,error}=await q;
    if(error)throw error;
    res.json({ok:true,jobs:data||[]});
  }catch(e){
    res.status(500).json({error:e.message});
  }
});

async function runQueuedBackfillJob(job){
  if(!job||job.job_type!=="backfill_30d")return {ok:true,skipped:true,reason:"not_backfill_job",job_id:job?.id||null};
  const platform=String(job.platform||"").toLowerCase();
  const platformAccountId=normalizePlatformAccountId(job.platform_account_id);
  const datePreset=String(job.metadata?.datePreset||job.metadata?.date_preset||"last_30d");
  const captureReason=String(job.capture_reason||job.metadata?.captureReason||"account_backfill_30d");
  const snapshotClass=String(job.metadata?.snapshotClass||"backfill");
  if(!platformAccountId)throw new Error("Queued backfill missing platform_account_id");

  const {data:user,error:userError}=await supabaseAdmin.from("users").select("*").eq("id",job.user_id).maybeSingle();
  if(userError)throw userError;
  if(!user)throw new Error("Queued backfill user not found");

  const ownership=await getOwnership(platform,platformAccountId);
  if(!ownership||ownership.owner_user_id!==job.user_id||!activeOwnershipStatuses().includes(ownership.status)){
    return {ok:true,skipped:true,reason:"ownership_not_active",job_id:job.id,platform,platform_account_id:platformAccountId,ownership_status:ownership?.status||null};
  }

  const {data:conn,error:connError}=await supabaseAdmin.from("platform_connections").select("*").eq("user_id",job.user_id).eq("platform",platform).eq("connected",true).maybeSingle();
  if(connError)throw connError;
  if(!conn)throw new Error(`Queued backfill ${platform} connection not found`);

  await setRefreshJobStatus(job.id,"running");
  try{
    let writeResult=null;
    if(platform==="meta"){
      const platformTimeZone=await getPlatformAccountTimezone(job.user_id,"meta",platformAccountId,conn,ownership);
      writeResult=await writeMetaSnapshotImmutable({user,conn,adAccountId:platformAccountId,datePreset,snapshotDate:null,limit:String(job.metadata?.limit||"100"),sourceJobId:job.id,captureReason,platformTimeZone,snapshotClass});
    }else if(platform==="google"){
      const resolved=await resolveGoogleRefreshAccount(user,platformAccountId,job.metadata?.loginCustomerId||conn.metadata?.loginCustomerId||conn.metadata?.login_customer_id||GOOGLE_SNAPSHOT_LOGIN_CUSTOMER_ID);
      writeResult=await writeGoogleSnapshotImmutable({user,customerId:normalizeCustomerId(resolved.customerId||platformAccountId),loginCustomerId:normalizeCustomerId(resolved.loginCustomerId||job.metadata?.loginCustomerId||GOOGLE_SNAPSHOT_LOGIN_CUSTOMER_ID||""),dateRange:datePreset,snapshotDate:null,sourceJobId:job.id,captureReason,snapshotClass});
    }else if(platform==="tiktok"){
      writeResult=await writeTikTokSnapshotImmutable({user,conn,platformAccountId,datePreset,snapshotDate:null,sourceJobId:job.id,captureReason,snapshotClass});
    }else if(platform==="klaviyo"){
      writeResult=await writeKlaviyoSnapshotImmutable({user,conn,platformAccountId,datePreset,snapshotDate:null,sourceJobId:job.id,captureReason,snapshotClass});
    }else{
      return {ok:true,skipped:true,reason:"unsupported_backfill_platform",job_id:job.id,platform};
    }
    await setRefreshJobStatus(job.id,"completed",{snapshot_id:writeResult?.snapshot?.id||null,metadata:{...(job.metadata||{}),processedBackfillAt:new Date().toISOString(),performance_spread_result:writeResult?.performance_spread_result||null,row_counts:writeResult?.row_counts||null}});
    return {ok:true,job_id:job.id,platform,platform_account_id:platformAccountId,snapshot_id:writeResult?.snapshot?.id||null,date_preset:datePreset,snapshot_class:snapshotClass};
  }catch(e){
    await setRefreshJobStatus(job.id,"failed",{error_message:e.message,metadata:{...(job.metadata||{}),failedBackfillAt:new Date().toISOString()}}).catch(()=>null);
    throw e;
  }
}

async function processQueuedBackfills({limit=10}={}){
  const {data:jobs,error}=await supabaseAdmin
    .from("snapshot_jobs")
    .select("*")
    .eq("status","queued")
    .eq("job_type","backfill_30d")
    .order("created_at",{ascending:true})
    .limit(Number(limit)||10);
  if(error)throw error;
  const results=[];
  for(const job of jobs||[]){
    try{results.push(await runQueuedBackfillJob(job));}
    catch(e){results.push({ok:false,job_id:job.id,platform:job.platform,platform_account_id:job.platform_account_id,error:e.message});}
  }
  return {ok:true,count:results.length,results};
}

app.post("/api/backfill/process",async(req,res)=>{
  try{
    const user=await requireUser(req,res);if(!user)return;
    const accessCheck=await requireAccess(req,res,user.id,"manualRefresh");if(!accessCheck)return;
    res.json(await processQueuedBackfills({limit:Number(req.body?.limit||req.query.limit||10)}));
  }catch(e){res.status(e.status||500).json({ok:false,error:e.message,stage:"queued_backfill_process"})}
});

async function runMetaAutoRefreshForSchedule(schedule){
  let job=null;
  const runDate=new Date();
  const limit=String(schedule.metadata?.limit||"100");

  const {data:user,error:userError}=await supabaseAdmin
    .from("users")
    .select("*")
    .eq("id",schedule.user_id)
    .maybeSingle();
  if(userError)throw userError;
  if(!user)throw new Error("Auto refresh user not found");

  const {data:conn,error:connError}=await supabaseAdmin
    .from("platform_connections")
    .select("*")
    .eq("user_id",schedule.user_id)
    .eq("platform","meta")
    .eq("connected",true)
    .maybeSingle();
  if(connError)throw connError;
  if(!conn)throw new Error("Auto refresh Meta connection not found");

  const platformAccountId=normalizePlatformAccountId(schedule.platform_account_id||conn.account_id);
  if(!platformAccountId)throw new Error("Auto refresh missing platform account id");

  if(schedule.active===false){
    return {ok:true,skipped:true,reason:"schedule_inactive",schedule_id:schedule.id,platform_account_id:platformAccountId};
  }

  const ownership=await getOwnership("meta",platformAccountId);
  if(!ownership||ownership.owner_user_id!==schedule.user_id||!activeOwnershipStatuses().includes(ownership.status)){
    return {ok:true,skipped:true,reason:"ownership_not_active",schedule_id:schedule.id,platform_account_id:platformAccountId,ownership_status:ownership?.status||null};
  }

  const platformTimeZone=await getPlatformAccountTimezone(schedule.user_id,"meta",platformAccountId,conn,ownership);
  const policy=resolveAutoRefreshPolicy({date:runDate,platformTimeZone,platform:"meta"});
  const snapshotDate=e2aSnapshotDate(null,platformTimeZone);

  if(!policy.isAutomationHour){
    return {
      ok:true,
      skipped:true,
      reason:"not_platform_automation_hour",
      schedule_id:schedule.id,
      platform_account_id:platformAccountId,
      platform_account_timezone:platformTimeZone,
      platform_business_hour:policy.platform_business_hour,
      platform_account_time:policy.platform_account_time,
      server_time_utc:policy.server_time_utc,
      istanbul_time:policy.istanbul_time,
      automation_hours:policy.automation_hours
    };
  }

  job=await createRefreshJob(schedule.user_id,"meta",platformAccountId,{
    trigger:"automation",
    datePreset:policy.datePreset,
    snapshotDate,
    limit,
    captureReason:policy.captureReason,
    snapshotClass:policy.snapshotClass,
    scheduleId:schedule.id,
    platformHour:policy.hour,
    platformBusinessHour:policy.platform_business_hour,
    dataMaturityWindowHours:policy.data_maturity_window_hours,
    server_time_utc:policy.server_time_utc,
    istanbul_time:policy.istanbul_time,
    platform_account_time:policy.platform_account_time,
    platform_account_timezone:policy.platform_account_timezone,
    platform_business_date:policy.platform_business_date,
    timeEngineVersion:TIME_ENGINE_VERSION
  });

  await setRefreshJobStatus(job.id,"running");

  try{
    const writeResult=await writeMetaSnapshotImmutable({
      user,
      conn,
      adAccountId:platformAccountId,
      datePreset:policy.datePreset,
      snapshotDate,
      limit,
      sourceJobId:job.id,
      captureReason:policy.captureReason,
      platformTimeZone,
      adminTimeSync:policy,
      snapshotClass:policy.snapshotClass
    });

    await setRefreshJobStatus(job.id,"completed",{snapshot_id:writeResult.snapshot?.id||null});

    let recovery_result=null;
    if(policy.shouldRunRecoverySnapshot){
      const recoveryJob=await createRefreshJob(schedule.user_id,"meta",platformAccountId,{
        trigger:"automation",
        datePreset:policy.recoveryDatePreset,
        snapshotDate,
        limit,
        captureReason:policy.recoveryCaptureReason,
        snapshotClass:policy.recoverySnapshotClass,
        scheduleId:schedule.id,
        pairedPrimaryJobId:job.id,
        timeEngineVersion:TIME_ENGINE_VERSION
      });
      await setRefreshJobStatus(recoveryJob.id,"running");
      try{
        const recoveryWrite=await writeMetaSnapshotImmutable({
          user,
          conn,
          adAccountId:platformAccountId,
          datePreset:policy.recoveryDatePreset,
          snapshotDate,
          limit,
          sourceJobId:recoveryJob.id,
          captureReason:policy.recoveryCaptureReason,
          platformTimeZone,
          adminTimeSync:policy,
          snapshotClass:policy.recoverySnapshotClass
        });
        await setRefreshJobStatus(recoveryJob.id,"completed",{snapshot_id:recoveryWrite.snapshot?.id||null});
        recovery_result={ok:true,job_id:recoveryJob.id,snapshot_id:recoveryWrite.snapshot?.id||null};
      }catch(recoveryError){
        await setRefreshJobStatus(recoveryJob.id,"failed",{error_message:recoveryError.message}).catch(()=>null);
        recovery_result={ok:false,job_id:recoveryJob.id,error:recoveryError.message};
      }
    }

    await supabaseAdmin
      .from("snapshot_schedules")
      .update({
        last_run_at:new Date().toISOString(),
        next_run_at:nextAutomationSlotUtc(),
        updated_at:new Date().toISOString()
      })
      .eq("id",schedule.id);

    return {ok:true,job_id:job.id,snapshot_id:writeResult.snapshot?.id||null,snapshot_version:writeResult.snapshot?.snapshot_version||null,snapshot_class:writeResult.snapshot?.snapshot_class||policy.snapshotClass,date_preset:policy.datePreset,capture_reason:policy.captureReason,platform_account_timezone:platformTimeZone,platform_account_time:policy.platform_account_time,server_time_utc:policy.server_time_utc,istanbul_time:policy.istanbul_time,fx_rate:writeResult.snapshot?.fx_rate??null,fx_provider:writeResult.snapshot?.fx_provider||null,fx_source_currency:writeResult.snapshot?.fx_source_currency||null,fx_target_currency:writeResult.snapshot?.fx_target_currency||null,fx_engine_version:writeResult.snapshot?.fx_engine_version||null,performance_spread_result:writeResult.performance_spread_result||null};
  }catch(e){
    await setRefreshJobStatus(job.id,"failed",{error_message:e.message}).catch(()=>null);
    throw e;
  }
}

app.get("/api/cron/auto-refresh",async(req,res)=>{
  const startedAt=new Date().toISOString();
  try{
    let fx_result=null;
    const fxNow=new Date();
    if(fxNow.getUTCHours()===5){
      try{
        fx_result=await upsertFxRatesDaily({rateDate:fxDateOnly(fxNow)});
      }catch(fxError){
        fx_result={ok:false,error:fxError.message,stage:"fx_rates_auto_refresh_05utc"};
      }
    }
    const queued_backfill_result=await processQueuedBackfills({limit:Number(req.query.backfill_limit||10)}).catch(e=>({ok:false,error:e.message,stage:"queued_backfill_cron"}));
    const nowIso=new Date().toISOString();
    const {data:schedules,error}=await supabaseAdmin
      .from("snapshot_schedules")
      .select("*")
      .eq("active",true)
      .in("platform",["meta","google","tiktok","klaviyo"])
      .or(`next_run_at.is.null,next_run_at.lte.${nowIso}`);

    if(error)throw error;

    const results=[];
    for(const schedule of schedules||[]){
      try{
        if(schedule.platform==="meta")results.push(await runMetaAutoRefreshForSchedule(schedule));
        else if(schedule.platform==="google")results.push(await runGoogleAutoRefreshForSchedule(schedule));
        else if(schedule.platform==="tiktok")results.push(await runTikTokAutoRefreshForSchedule(schedule));
        else if(schedule.platform==="klaviyo")results.push(await runKlaviyoAutoRefreshForSchedule(schedule));
        else results.push({ok:true,skipped:true,reason:"unsupported_platform",platform:schedule.platform,schedule_id:schedule.id});
      }catch(e){
        results.push({ok:false,platform:schedule.platform,schedule_id:schedule.id,error:e.message});
      }
    }

    res.json({ok:true,started_at:startedAt,fx_result,queued_backfill_result,count:results.length,results});
  }catch(e){
    res.status(500).json({ok:false,error:e.message,started_at:startedAt});
  }
});

// ===== END PHASE E.2A META SNAPSHOT WRITE =====
// ===== PHASE E.2C META SNAPSHOT READ =====
function toIsoDateOnly(value){
  const d=value instanceof Date?value:new Date(value);
  if(Number.isNaN(d.getTime()))return "";
  const y=d.getUTCFullYear();
  const m=String(d.getUTCMonth()+1).padStart(2,"0");
  const day=String(d.getUTCDate()).padStart(2,"0");
  return `${y}-${m}-${day}`;
}

function addUtcDays(isoDate,days){
  const parts=String(isoDate||"").split("-").map(Number);
  if(parts.length!==3||parts.some(n=>!Number.isFinite(n)))return "";
  const d=new Date(Date.UTC(parts[0],parts[1]-1,parts[2]));
  d.setUTCDate(d.getUTCDate()+days);
  return toIsoDateOnly(d);
}

function resolveSnapshotDateScope(query,platformTimeZone=DEFAULT_PLATFORM_TIMEZONE){
  const raw=String(query.date_filter||"latest").trim().toLowerCase();
  const dateFilter=raw.replace(/\s+/g,"_");
  const today=timePartsInZone(new Date(),platformTimeZone).date;

  if(dateFilter==="today"){
    return {dateFilter,start:today,end:today};
  }

  if(dateFilter==="yesterday"){
    const yesterday=addUtcDays(today,-1);
    return {dateFilter,start:yesterday,end:yesterday};
  }

  if(dateFilter==="last_7_days"){
    return {dateFilter,start:addUtcDays(today,-6),end:today};
  }

  if(dateFilter==="this_month"){
    return {dateFilter,start:today.slice(0,7)+"-01",end:today};
  }

  if(dateFilter==="custom"){
    const start=String(query.start_date||"").slice(0,10);
    const end=String(query.end_date||"").slice(0,10);
    return {dateFilter,start:start||null,end:end||null};
  }

  return {dateFilter:"latest",start:null,end:null};
}


function normalizeSnapshotForResponse(data){
  if(!data)return null;
  return {
    platform:data.platform||"meta",
    platform_account_id:data.platform_account_id||null,
    platform_base_currency:data.platform_base_currency||null,
    snapshot_version:data.snapshot_version||null,
    source_job_id:data.source_job_id||null,
    date_preset:data.date_preset||null,
    snapshot_period_start:data.snapshot_period_start||null,
    snapshot_period_end:data.snapshot_period_end||null,
    snapshot_scope:data.snapshot_scope||null,
    capture_reason:data.capture_reason||null,
    snapshot_class:data.snapshot_class||null,
    platform_account_timezone:data.platform_account_timezone||null,
    platform_business_date:data.platform_business_date||null,
    platform_business_hour:data.platform_business_hour??null,
    server_time_utc:data.server_time_utc||null,
    istanbul_time:data.istanbul_time||null,
    platform_account_time:data.platform_account_time||null,
    time_engine_version:data.time_engine_version||null,
    fx_rate:data.fx_rate??null,
    fx_provider:data.fx_provider||null,
    fx_rate_timestamp:data.fx_rate_timestamp||null,
    fx_source_currency:data.fx_source_currency||null,
    fx_target_currency:data.fx_target_currency||null,
    fx_engine_version:data.fx_engine_version||null,
    snapshot_date:data.snapshot_date,
    snapshot_created_at:data.snapshot_created_at||data.created_at||null,
    account_currency:data.account_currency,
    kpis:data.kpis||{},
    purchase_journey:data.purchase_journey||{},
    click_journey:data.click_journey||{},
    performance_summary:data.performance_summary||{rows:[],counts:{}}
  };
}

function pickLatestSnapshotPerDay(rows){
  const map=new Map();
  for(const row of rows||[]){
    if(row.snapshot_class==="recovery")continue;
    const key=String(row.snapshot_date||"");
    const current=map.get(key);
    const rowVersion=Number(row.snapshot_version||0);
    const currentVersion=Number(current?.snapshot_version||0);
    const rowCreated=new Date(row.snapshot_created_at||row.created_at||0).getTime();
    const currentCreated=new Date(current?.snapshot_created_at||current?.created_at||0).getTime();

    if(!current || rowVersion>currentVersion || (rowVersion===currentVersion && rowCreated>currentCreated)){
      map.set(key,row);
    }
  }
  return [...map.values()].sort((a,b)=>String(a.snapshot_date).localeCompare(String(b.snapshot_date)));
}

function aggregateSnapshots(rows,scope){
  const daily=pickLatestSnapshotPerDay(rows);
  if(!daily.length)return null;

  const kpis={impressions:0,reach:0,clicks:0,spend:0,sales:0,revenue:0,cpc:null,ctr:null,roas:null};
  const purchase_journey={add_to_cart:0,checkout:0,purchase:0,abandoned:0};
  const click_journey={ad_clicks:0,link_clicks:0,landing_page_views:0,real_cpc:null,traffic_score:null};
  const performanceRows=[];
  const counts={campaign:0,adset:0,ad:0,total:0};

  for(const snap of daily){
    const sK=snap.kpis||{};
    kpis.impressions+=numberOrZero(sK.impressions);
    kpis.reach+=numberOrZero(sK.reach);
    kpis.clicks+=numberOrZero(sK.clicks);
    kpis.spend+=numberOrZero(sK.spend);
    kpis.sales+=numberOrZero(sK.sales);
    kpis.revenue+=numberOrZero(sK.revenue);

    const pj=snap.purchase_journey||{};
    purchase_journey.add_to_cart+=numberOrZero(pj.add_to_cart);
    purchase_journey.checkout+=numberOrZero(pj.checkout);
    purchase_journey.purchase+=numberOrZero(pj.purchase);
    purchase_journey.abandoned+=numberOrZero(pj.abandoned);

    const cj=snap.click_journey||{};
    click_journey.ad_clicks+=numberOrZero(cj.ad_clicks);
    click_journey.link_clicks+=numberOrZero(cj.link_clicks);
    click_journey.landing_page_views+=numberOrZero(cj.landing_page_views);

    const ps=snap.performance_summary||{};
    const rows=Array.isArray(ps.rows)?ps.rows:[];
    performanceRows.push(...rows);
    const c=ps.counts||{};
    counts.campaign+=numberOrZero(c.campaign);
    counts.adset+=numberOrZero(c.adset);
    counts.ad+=numberOrZero(c.ad);
    counts.total+=numberOrZero(c.total||rows.length);
  }

  kpis.cpc=safeDivide(kpis.spend,kpis.clicks);
  kpis.ctr=safeDivide(kpis.clicks*100,kpis.impressions);
  kpis.roas=safeDivide(kpis.revenue,kpis.spend);
  click_journey.real_cpc=safeDivide(kpis.spend,click_journey.landing_page_views);
  click_journey.traffic_score=safeDivide(click_journey.landing_page_views*100,click_journey.ad_clicks);

  const latest=daily[daily.length-1];
  return {
    platform:latest.platform||"meta",
    platform_account_id:latest.platform_account_id||null,
    platform_base_currency:latest.platform_base_currency||null,
    snapshot_version:latest.snapshot_version||null,
    source_job_id:latest.source_job_id||null,
    date_preset:"aggregate",
    snapshot_class:"aggregate_primary",
    snapshot_period_start:scope.start,
    snapshot_period_end:scope.end,
    snapshot_scope:scope.dateFilter,
    capture_reason:"date_filter_aggregate",
    platform_account_timezone:latest.platform_account_timezone||null,
    platform_business_date:latest.platform_business_date||null,
    platform_business_hour:latest.platform_business_hour??null,
    fx_rate:latest.fx_rate??null,
    fx_provider:latest.fx_provider||null,
    fx_rate_timestamp:latest.fx_rate_timestamp||null,
    fx_source_currency:latest.fx_source_currency||null,
    fx_target_currency:latest.fx_target_currency||null,
    fx_engine_version:latest.fx_engine_version||null,
    snapshot_date:latest.snapshot_date,
    snapshot_created_at:latest.snapshot_created_at||latest.created_at||null,
    account_currency:latest.account_currency,
    kpis,
    purchase_journey,
    click_journey,
    performance_summary:{rows:performanceRows,counts},
    daily_snapshots:daily.map(s=>({
      snapshot_date:s.snapshot_date,
      snapshot_version:s.snapshot_version,
      snapshot_id:s.id||null,
      snapshot_created_at:s.snapshot_created_at||s.created_at||null,
      date_preset:s.date_preset||null,
      capture_reason:s.capture_reason||null,
      snapshot_class:s.snapshot_class||null,
      platform_account_timezone:s.platform_account_timezone||null,
      platform_business_date:s.platform_business_date||null,
      fx_rate:s.fx_rate??null,
      fx_provider:s.fx_provider||null,
      fx_source_currency:s.fx_source_currency||null,
      fx_target_currency:s.fx_target_currency||null,
      fx_engine_version:s.fx_engine_version||null
    }))
  };
}

app.get("/api/snapshots/meta/latest",async(req,res)=>{
  try{
    const user=await requireUser(req,res);
    if(!user)return;

    const requestedAdAccountId=req.query.adAccountId||req.query.ad_account_id||req.query.platform_account_id;
    let platformAccountId=normalizePlatformAccountId(requestedAdAccountId);
    let conn=null;
    let platformTimeZone=DEFAULT_PLATFORM_TIMEZONE;
    try{
      conn=await getConnection(user.id,"meta");
      platformAccountId=platformAccountId||normalizePlatformAccountId(conn?.account_id||conn?.metadata?.lastOwnedPlatformAccountId||conn?.metadata?.selectedPlatformAccountId);
      if(platformAccountId)platformTimeZone=await getPlatformAccountTimezone(user.id,"meta",platformAccountId,conn,null);
    }catch{}

    const scope=resolveSnapshotDateScope(req.query||{},platformTimeZone);
    const isRangeScope=["last_7_days","this_month","custom"].includes(scope.dateFilter);

    let snapshotQuery=supabaseAdmin
      .from("dashboard_snapshots")
      .select("id,platform,platform_account_id,platform_base_currency,snapshot_version,source_job_id,date_preset,snapshot_period_start,snapshot_period_end,snapshot_scope,capture_reason,snapshot_class,platform_account_timezone,platform_business_date,platform_business_at,platform_business_hour,server_time_utc,istanbul_time,platform_account_time,time_engine_version,fx_rate,fx_provider,fx_rate_timestamp,fx_rate_date,fx_source_currency,fx_target_currency,fx_engine_version,snapshot_date,snapshot_created_at,created_at,account_currency,kpis,purchase_journey,click_journey,performance_summary")
      .eq("user_id",user.id)
      .neq("snapshot_class","recovery");

    if(platformAccountId)snapshotQuery=snapshotQuery.eq("platform_account_id",platformAccountId);
    if(scope.start)snapshotQuery=snapshotQuery.gte("snapshot_date",scope.start);
    if(scope.end)snapshotQuery=snapshotQuery.lte("snapshot_date",scope.end);

    const {data,error}=await snapshotQuery
      .order("snapshot_date",{ascending:false})
      .order("snapshot_version",{ascending:false})
      .order("created_at",{ascending:false})
      .limit(isRangeScope?500:1);

    if(error)throw error;

    let snapshot=null;
    if(isRangeScope){
      snapshot=aggregateSnapshots(data||[],scope);
    }else{
      snapshot=normalizeSnapshotForResponse((data||[])[0]||null);
    }

    res.json({
      ok:true,
      platform:"Meta",
      date_scope:{
        ...scope,
        platform_account_timezone:platformTimeZone,
        platform_account_id:platformAccountId||null,
        server_time_utc:new Date().toISOString(),
        istanbul_time:timePartsInZone(new Date(),"Europe/Istanbul").text,
        platform_account_time:timePartsInZone(new Date(),platformTimeZone).text
      },
      snapshot
    });
  }catch(e){
    res.status(500).json({error:e.message});
  }
});


// ===== END PHASE E.2C META SNAPSHOT READ =====



app.get("/api/debug/time-sync",async(req,res)=>{
  try{
    const user=await requireUser(req,res);
    if(!user)return;
    const platform=String(req.query.platform||"meta");
    const conn=await getConnection(user.id,platform).catch(()=>null);
    const platformAccountId=normalizePlatformAccountId(req.query.platform_account_id||req.query.adAccountId||conn?.account_id||conn?.metadata?.lastOwnedPlatformAccountId||conn?.metadata?.selectedPlatformAccountId);
    const platformTimeZone=platformAccountId?await getPlatformAccountTimezone(user.id,platform,platformAccountId,conn,null):DEFAULT_PLATFORM_TIMEZONE;
    const sync=resolveAdminTimeSync(new Date(),platformTimeZone);
    res.json({ok:true,platform,platform_account_id:platformAccountId||null,...sync,automation_hours:AUTOMATION_PLATFORM_HOURS,data_maturity_window_hours:dataMaturityWindowHours(platform),time_engine_version:TIME_ENGINE_VERSION});
  }catch(e){
    res.status(500).json({ok:false,error:e.message});
  }
});

app.get("/api/unified/status",async(req,res)=>{const user=await requireUser(req,res);if(!user)return;const meta=await connectionStatus(user.id,"meta"),google=await connectionStatus(user.id,"google"),pinterest=await connectionStatus(user.id,"pinterest"),klaviyo=await connectionStatus(user.id,"klaviyo"),tiktok=await connectionStatus(user.id,"tiktok"),organic=await connectionStatus(user.id,"organic");res.json({meta:meta.connected,google:google.connected,pinterest:pinterest.connected,klaviyo:klaviyo.connected,tiktok:tiktok.connected,organic:organic.connected,sources:{meta:meta.source,google:google.source,pinterest:pinterest.source,klaviyo:klaviyo.source,tiktok:tiktok.source,organic:organic.source},updatedAt:{meta:meta.updatedAt,google:google.updatedAt,pinterest:pinterest.updatedAt,klaviyo:klaviyo.updatedAt,tiktok:tiktok.updatedAt,organic:organic.updatedAt},platformStatus:{pinterest:passiveLegacyPlatformStatus("pinterest"),organic:{platform:"organic",status:organic.connected?"oauth_connected":"skeleton",label:"Organic",message:organic.connected?"Organic OAuth connected. GA4 Property and Search Console Site discovery are available.":"Organic platform skeleton is available. Connect Organic to continue."}}})});
app.get("/api/debug/connections",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;const{data,error}=await supabaseAdmin.from("platform_connections").select("platform,connected,account_id,account_name,token_expires_at,metadata,updated_at").eq("user_id",user.id).order("updated_at",{ascending:false});if(error)throw error;res.json({connections:data||[]})}catch(e){res.status(500).json({error:e.message})}});
app.post("/api/connections/:platform/disconnect",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;const platform=req.params.platform;if(!["meta","google","pinterest","klaviyo","tiktok","organic"].includes(platform))return res.status(400).json({error:"Unsupported platform"});const result=await disconnectPlatformLifecycle(user.id,platform);res.json(result)}catch(e){res.status(e.status||500).json({error:e.message})}});
async function upsertAdAccount(userId,platform,account){
  if(!supabaseAdmin||!userId)return null;
  const row={
    user_id:userId,
    platform,
    platform_business_id:account.business_id||null,
    platform_account_id:normalizePlatformAccountId(account.id||account.customerId||account.account_id||account.platform_account_id),
    account_name:account.name||account.descriptiveName||account.account_name||null,
    currency:account.currency||account.currency_code||null,
    timezone:account.timezone_name||account.timezone||null,
    status:String(account.account_status||account.status||""),
    metadata:account,
    updated_at:new Date().toISOString()
  };
  if(!row.platform_account_id)return null;
  await supabaseAdmin.from("platform_ad_accounts").upsert(row,{onConflict:"user_id,platform,platform_account_id"});
  return row;
}
async function metaGraph(pathname,params,token){const url=new URL(`https://graph.facebook.com/${META_GRAPH_VERSION}${pathname}`);Object.entries(params||{}).forEach(([k,v])=>{if(v!==undefined&&v!==null&&v!=="")url.searchParams.set(k,v)});url.searchParams.set("access_token",token);const r=await fetch(url);const data=await r.json();if(!r.ok)throw new Error(data.error?.message||JSON.stringify(data));return data}
app.get("/api/meta/adaccounts",async(req,res)=>{try{const result=await requireConnection(req,res,"meta");if(!result)return;const{user,conn}=result;const data=await metaGraph("/me/adaccounts",{fields:"id,name,account_status,currency,timezone_name",limit:"100"},conn.access_token);const accounts=data.data||[];res.json({platform:"meta",accounts})}catch(e){res.status(500).json({error:e.message})}});
function actionValue(list,type){const f=Array.isArray(list)?list.find(x=>x.action_type===type):null;return f?Number(f.value||0):null}
function normalizeMetaInsight(row,level){const a=row.actions||[],c=row.cost_per_action_type||[],v=row.action_values||[];const addToCart=actionValue(a,"add_to_cart")??actionValue(a,"omni_add_to_cart");const checkout=actionValue(a,"initiate_checkout")??actionValue(a,"checkout")??actionValue(a,"omni_initiated_checkout");const purchase=actionValue(a,"purchase")??actionValue(a,"omni_purchase");const purchaseValue=actionValue(v,"purchase")??actionValue(v,"omni_purchase");const addToCartValue=actionValue(v,"add_to_cart")??actionValue(v,"omni_add_to_cart");const checkoutValue=actionValue(v,"initiate_checkout")??actionValue(v,"checkout")??actionValue(v,"omni_initiated_checkout");const abandoned=Math.max((checkout||0)-(purchase||0),0);const spend=Number(row.spend||0);const revenue=purchaseValue??null;return{platform:"Meta",level,campaign_id:row.campaign_id||null,campaign_name:row.campaign_name||null,campaign_status:row.campaign_status||null,adset_id:row.adset_id||null,adset_name:row.adset_name||null,ad_id:row.ad_id||null,ad_name:row.ad_name||null,currency:row.account_currency||null,impressions:Number(row.impressions||0),reach:Number(row.reach||0),clicks:Number(row.clicks||0),ctr:row.ctr!==undefined?Number(row.ctr):null,cpc:row.cpc!==undefined?Number(row.cpc):null,spend,link_clicks:actionValue(a,"link_click"),landing_page_views:actionValue(a,"landing_page_view"),omni_landing_page_views:actionValue(a,"omni_landing_page_view"),page_engagement:actionValue(a,"page_engagement"),post_engagement:actionValue(a,"post_engagement"),video_views:actionValue(a,"video_view"),add_to_cart:addToCart,checkout,purchase,purchases:purchase,abandoned,add_to_cart_value:addToCartValue,checkout_value:checkoutValue,purchase_value:purchaseValue,cost_per_link_click:actionValue(c,"link_click"),cost_per_landing_page_view:actionValue(c,"landing_page_view"),cost_per_page_engagement:actionValue(c,"page_engagement"),cost_per_video_view:actionValue(c,"video_view"),conversion_rate_ranking:row.conversion_rate_ranking||null,sales:revenue,revenue,roas:spend&&spend>0&&revenue!==null?revenue/spend:null,date_start:row.date_start||null,date_stop:row.date_stop||null,raw:row}}
app.get("/api/meta/insights",async(req,res)=>{try{const result=await requireConnection(req,res,"meta");if(!result)return;const{conn}=result;const adAccountId=req.query.adAccountId||req.query.ad_account_id;if(!adAccountId)return res.status(400).json({error:"Missing adAccountId"});const level=["campaign","adset","ad"].includes(String(req.query.level||"campaign"))?String(req.query.level||"campaign"):"campaign";const fields=["campaign_id","campaign_name","account_currency","impressions","reach","clicks","ctr","cpc","spend","actions","action_values","cost_per_action_type","conversion_rate_ranking"];if(level==="adset")fields.splice(2,0,"adset_id","adset_name");if(level==="ad")fields.splice(2,0,"adset_id","adset_name","ad_id","ad_name");const data=await metaGraph(`/${adAccountId}/insights`,{level,date_preset:req.query.date_preset||"last_7d",fields:fields.join(","),limit:req.query.limit||"100"},conn.access_token);res.json({platform:"Meta",level,date_preset:req.query.date_preset||"last_7d",rows:(data.data||[]).map(r=>normalizeMetaInsight(r,level)),paging:data.paging||null})}catch(e){res.status(500).json({error:e.message})}});
function normalizeCustomerId(id){return String(id||"").replace(/-/g,"").trim()}
function googleHeaders(token,loginCustomerId){const h={Authorization:`Bearer ${token}`,"developer-token":process.env.GOOGLE_DEVELOPER_TOKEN||"","Content-Type":"application/json"};if(loginCustomerId)h["login-customer-id"]=normalizeCustomerId(loginCustomerId);return h}
async function googleAdsSearch(userId,customerId,query,loginCustomerId){const token=await getFreshGoogleAccessToken(userId);const clean=normalizeCustomerId(customerId);const r=await fetch(`https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers/${clean}/googleAds:search`,{method:"POST",headers:googleHeaders(token,loginCustomerId),body:JSON.stringify({query})});const data=await r.json();if(!r.ok){const err=new Error(JSON.stringify(data));err.status=r.status;throw err}return data}
function googleDateClause(range){return range==="today"?"segments.date DURING TODAY":(range==="yesterday"||range==="day_close")?"segments.date DURING YESTERDAY":range==="last_30d"?"segments.date DURING LAST_30_DAYS":"segments.date DURING LAST_7_DAYS"}
function googleQuery(level,range){const d=googleDateClause(range);if(level==="adgroup")return `SELECT segments.date, customer.currency_code, campaign.id, campaign.name, ad_group.id, ad_group.name, ad_group.status, metrics.impressions, metrics.clicks, metrics.ctr, metrics.average_cpc, metrics.cost_micros, metrics.conversions, metrics.all_conversions, metrics.conversions_value, metrics.conversions_from_interactions_rate FROM ad_group WHERE ${d} ORDER BY metrics.cost_micros DESC LIMIT 100`;if(level==="ad")return `SELECT segments.date, customer.currency_code, campaign.id, campaign.name, ad_group.id, ad_group.name, ad_group_ad.ad.id, ad_group_ad.ad.name, ad_group_ad.status, metrics.impressions, metrics.clicks, metrics.ctr, metrics.average_cpc, metrics.cost_micros, metrics.conversions, metrics.all_conversions, metrics.conversions_value, metrics.conversions_from_interactions_rate FROM ad_group_ad WHERE ${d} ORDER BY metrics.cost_micros DESC LIMIT 100`;return `SELECT segments.date, customer.currency_code, campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, bidding_strategy.type, metrics.impressions, metrics.clicks, metrics.ctr, metrics.average_cpc, metrics.cost_micros, metrics.conversions, metrics.all_conversions, metrics.conversions_value, metrics.conversions_from_interactions_rate FROM campaign WHERE ${d} ORDER BY metrics.cost_micros DESC LIMIT 100`}
function googleConversionBreakdownQuery(level,range){const d=googleDateClause(range);if(level==="adgroup")return `SELECT campaign.id, campaign.name, ad_group.id, ad_group.name, segments.conversion_action, segments.conversion_action_name, segments.conversion_action_category, metrics.conversions, metrics.conversions_value FROM ad_group WHERE ${d} AND metrics.conversions > 0 LIMIT 1000`;if(level==="ad")return `SELECT campaign.id, campaign.name, ad_group.id, ad_group.name, ad_group_ad.ad.id, segments.conversion_action, segments.conversion_action_name, segments.conversion_action_category, metrics.conversions, metrics.conversions_value FROM ad_group_ad WHERE ${d} AND metrics.conversions > 0 LIMIT 1000`;return `SELECT campaign.id, campaign.name, segments.conversion_action, segments.conversion_action_name, segments.conversion_action_category, metrics.conversions, metrics.conversions_value FROM campaign WHERE ${d} AND metrics.conversions > 0 LIMIT 1000`}
function googleLandingPageViewQuery(level,range){const d=googleDateClause(range);if(level==="ad")return `SELECT segments.date, segments.campaign, segments.ad_group, landing_page_view.resource_name, landing_page_view.unexpanded_final_url, metrics.clicks FROM landing_page_view WHERE ${d} AND metrics.clicks > 0 LIMIT 1000`;if(level==="adgroup")return `SELECT segments.date, segments.campaign, segments.ad_group, landing_page_view.resource_name, landing_page_view.unexpanded_final_url, metrics.clicks FROM landing_page_view WHERE ${d} AND metrics.clicks > 0 LIMIT 1000`;return `SELECT segments.date, segments.campaign, landing_page_view.resource_name, landing_page_view.unexpanded_final_url, metrics.clicks FROM landing_page_view WHERE ${d} AND metrics.clicks > 0 LIMIT 1000`}
function googleResourceId(value){const s=String(value||"");const m=s.match(/\/(campaigns|adGroups)\/(\d+)$/);return m?m[2]:s}
function googleRowKey(row,level){const c=row.campaign||{},ag=row.adGroup||row.ad_group||{},aga=row.adGroupAd||row.ad_group_ad||{};if(level==="ad")return String(c.id||"")+"|"+String(ag.id||"")+"|"+String(nested(aga,"ad.id")||"");if(level==="adgroup")return String(c.id||"")+"|"+String(ag.id||"");return String(c.id||"")}
function googleLpvRowKey(row,level){const s=row.segments||{};const campaignId=googleResourceId(s.campaign||s.campaignResourceName||s.campaign_resource_name||nested(row,"campaign.resourceName")||nested(row,"campaign.resource_name")||nested(row,"campaign.id"));const adGroupId=googleResourceId(s.adGroup||s.ad_group||s.adGroupResourceName||s.ad_group_resource_name||nested(row,"adGroup.resourceName")||nested(row,"ad_group.resource_name")||nested(row,"adGroup.id")||nested(row,"ad_group.id"));if(level==="adgroup")return String(campaignId||"")+"|"+String(adGroupId||"");return String(campaignId||"")}
function mergeGoogleLandingPageViews(performanceRows,lpvRows,level){const mergeLevel=level==="ad"?"adgroup":level;const byKey=new Map();for(const r of performanceRows){const key=googleRowKey(r,mergeLevel);if(!byKey.has(key))byKey.set(key,[]);byKey.get(key).push(r)}for(const row of lpvRows||[]){const key=googleLpvRowKey(row,mergeLevel);const targets=byKey.get(key)||[];const clicks=googleMetricNumber(row,"clicks");for(const target of targets){target.__landing_page_views=(target.__landing_page_views||0)+clicks;target.__landing_page_view_rows=target.__landing_page_view_rows||[];target.__landing_page_view_rows.push(row)}}if(level==="ad"){for(const rows of byKey.values()){if(rows.length!==1){for(const row of rows){row.__landing_page_views=null;row.__landing_page_view_merge_status="not_merged_no_ad_level_lpv_segment"}}}}return performanceRows}
function googleConversionName(row){const s=row.segments||{},ca=row.conversionAction||row.conversion_action||{};return String(s.conversionActionName||s.conversion_action_name||ca.name||ca.resourceName||ca.resource_name||"").toLowerCase()}
function googleConversionCategory(row){const s=row.segments||{},ca=row.conversionAction||row.conversion_action||{};return String(s.conversionActionCategory||s.conversion_action_category||ca.category||"").toUpperCase()}
function googleMetricNumber(row,key){const m=row.metrics||{};return Number(m[key]??m[key.replace(/[A-Z]/g,x=>"_"+x.toLowerCase())]??0)}
function mergeGoogleConversionActions(performanceRows,breakdownRows,level){const byKey=new Map();for(const r of performanceRows)byKey.set(googleRowKey(r,level),r);for(const b of breakdownRows||[]){const key=googleRowKey(b,level);const target=byKey.get(key);if(!target)continue;target.__conversion_actions=target.__conversion_actions||[];target.__conversion_actions.push({name:googleConversionName(b),category:googleConversionCategory(b),conversions:googleMetricNumber(b,"conversions"),conversions_value:googleMetricNumber(b,"conversionsValue")});}return performanceRows}
function microsToMoney(v){return v===null||v===undefined||v===""?null:Number(v)/1000000}
function nested(o,p){return p.split(".").reduce((a,k)=>a&&a[k]!==undefined?a[k]:undefined,o)}
function googleMatchConversion(actions,kind){const list=Array.isArray(actions)?actions:[];const cfg={add_to_cart:{categories:["ADD_TO_CART"],names:["add_to_cart","add to cart","cart"]},checkout:{categories:["BEGIN_CHECKOUT"],names:["begin_checkout","checkout","start_checkout","started_checkout"]},purchase:{categories:["PURCHASE"],names:["purchase","placed_order","order","sale"]}}[kind];if(!cfg)return null;let total=0,value=0,found=false;for(const a of list){const name=String(a.name||"").toLowerCase();const cat=String(a.category||"").toUpperCase();const matched=cfg.categories.includes(cat)||cfg.names.some(n=>name.includes(n));if(matched){found=true;total+=Number(a.conversions||0);value+=Number(a.conversions_value||0)}}return found?{count:total,value}:null}
function normalizeGoogleInsight(row,level){const m=row.metrics||{},c=row.campaign||{},ag=row.adGroup||row.ad_group||{},aga=row.adGroupAd||row.ad_group_ad||{},cust=row.customer||{},seg=row.segments||{};const spend=microsToMoney(m.costMicros??m.cost_micros),cpc=microsToMoney(m.averageCpc??m.average_cpc),genericRevenue=Number(m.conversionsValue??m.conversions_value??0),genericConversions=Number(m.conversions??0),invalidClicks=Number(m.invalidClicks??m.invalid_clicks??0),clicks=Number(m.clicks||0),validClicks=Math.max(clicks-invalidClicks,0),landingPageViews=row.__landing_page_views===undefined?null:row.__landing_page_views;const actions=row.__conversion_actions||[];const atc=googleMatchConversion(actions,"add_to_cart"),chk=googleMatchConversion(actions,"checkout"),pur=googleMatchConversion(actions,"purchase");const addToCart=atc?atc.count:null,checkout=chk?chk.count:null,purchase=pur?pur.count:null,purchaseValue=pur?pur.value:genericRevenue||null;const abandoned=checkout!==null&&purchase!==null?Math.max((checkout||0)-(purchase||0),0):null;const sales=purchaseValue;const roas=spend&&spend>0&&sales!==null?sales/spend:null;const acos=sales&&sales>0&&spend!==null?(spend/sales)*100:null;return{platform:"Google",level,date:seg.date||null,campaign_id:c.id||null,campaign_name:c.name||null,campaign_status:c.status||null,channel_type:c.advertisingChannelType||c.advertising_channel_type||null,bidding_strategy_type:nested(row,"biddingStrategy.type")||nested(row,"bidding_strategy.type")||null,adgroup_id:ag.id||null,adgroup_name:ag.name||null,adgroup_status:ag.status||null,ad_id:nested(aga,"ad.id")||null,ad_name:nested(aga,"ad.name")||null,ad_status:aga.status||null,currency:cust.currencyCode||cust.currency_code||null,impressions:Number(m.impressions||0),clicks,ad_clicks:clicks,link_clicks:clicks,landing_page_views:landingPageViews,traffic_score:clicks>0&&landingPageViews!==null?(landingPageViews/clicks)*100:null,real_cpc:landingPageViews>0&&spend!==null?spend/landingPageViews:null,lpv_merge_status:row.__landing_page_view_merge_status||null,invalid_clicks:invalidClicks,valid_clicks:validClicks,ctr:m.ctr!==undefined?Number(m.ctr)*100:null,cpc,spend,conversions:genericConversions,all_conversions:Number(m.allConversions??m.all_conversions??0),add_to_cart:addToCart,checkout,purchase,purchases:purchase,abandoned,purchase_value:purchaseValue,revenue:sales,sales,conversion_rate:m.conversionsFromInteractionsRate!==undefined?Number(m.conversionsFromInteractionsRate)*100:m.conversions_from_interactions_rate!==undefined?Number(m.conversions_from_interactions_rate)*100:null,cvr:clicks&&purchase!==null?(purchase/clicks)*100:null,roas,acos,conversion_actions:actions,raw:{...row,landing_page_view_rows:row.__landing_page_view_rows||[]}}}
app.get("/api/google/customers",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;const token=await getFreshGoogleAccessToken(user.id);const r=await fetch(`https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers:listAccessibleCustomers`,{method:"GET",headers:googleHeaders(token)});const data=await r.json();if(!r.ok)return res.status(r.status).json({error:JSON.stringify(data),status:r.status});const customers=(data.resourceNames||[]).map(resourceName=>({resourceName,customerId:String(resourceName).replace("customers/","")}));res.json({customers})}catch(e){res.status(500).json({error:e.message})}});
app.get("/api/google/insights",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;const customerId=req.query.customerId||req.query.customer_id;if(!customerId)return res.status(400).json({error:"Missing customerId"});const level=["campaign","adgroup","ad"].includes(String(req.query.level||"campaign"))?String(req.query.level||"campaign"):"campaign";const dateRange=String(req.query.date_range||req.query.dateRange||"last_7d");const loginCustomerId=req.query.loginCustomerId||req.query.login_customer_id||"";const query=googleQuery(level,dateRange);const data=await googleAdsSearch(user.id,customerId,query,loginCustomerId);let breakdownData={results:[]},breakdownError=null;try{breakdownData=await googleAdsSearch(user.id,customerId,googleConversionBreakdownQuery(level,dateRange),loginCustomerId)}catch(err){breakdownError=err.message}const performanceRows=data.results||[];let lpvData={results:[]},lpvError=null;try{lpvData=await googleAdsSearch(user.id,customerId,googleLandingPageViewQuery(level,dateRange),loginCustomerId)}catch(err){lpvError=err.message}const withConversions=mergeGoogleConversionActions(performanceRows,breakdownData.results||[],level);const mergedRows=mergeGoogleLandingPageViews(withConversions,lpvData.results||[],level);res.json({platform:"Google",level,customerId:normalizeCustomerId(customerId),loginCustomerId:loginCustomerId?normalizeCustomerId(loginCustomerId):null,date_range:dateRange,rows:mergedRows.map(r=>normalizeGoogleInsight(r,level)),rawCount:mergedRows.length,conversionBreakdownCount:breakdownData.results?breakdownData.results.length:0,conversionBreakdownError:breakdownError,landingPageViewCount:lpvData.results?lpvData.results.length:0,landingPageViewError:lpvError,fieldMask:data.fieldMask||null,conversionFieldMask:breakdownData.fieldMask||null,landingPageViewFieldMask:lpvData.fieldMask||null,requestId:data.requestId||null,nextPageToken:data.nextPageToken||null})}catch(e){res.status(e.status||500).json({error:e.message})}});

// ===== GOOGLE SNAPSHOT WRITE v1 (Snapshot Layer only) =====
function googleDateRangeWindow(range,snapshotDate){
  const end=snapshotDate?new Date(`${snapshotDate}T00:00:00Z`):new Date();
  const start=new Date(end);
  if(range==="today"){}
  else if(range==="yesterday"||range==="day_close"){start.setDate(start.getDate()-1);end.setDate(end.getDate()-1)}
  else if(range==="last_30d")start.setDate(start.getDate()-29);
  else start.setDate(start.getDate()-6);
  const fmt=d=>d.toISOString().slice(0,10);
  return {start:fmt(start),end:fmt(end)};
}
function googleSafeNumber(value){const n=Number(value);return Number.isFinite(n)?n:0}
function googleNullableNumber(value){if(value===null||value===undefined||value==="")return null;const n=Number(value);return Number.isFinite(n)?n:null}
function googleSum(rows,field){return (rows||[]).reduce((t,r)=>t+googleSafeNumber(r?.[field]),0)}
function googleWeightedAverage(rows,valueField,weightField){let weighted=0,weight=0;for(const row of rows||[]){const value=googleNullableNumber(row?.[valueField]);const w=googleSafeNumber(row?.[weightField]);if(value!==null&&w>0){weighted+=value*w;weight+=w}}return weight>0?weighted/weight:null}
function googleSnapshotNullReasons(row){return {
  sales: googleSafeNumber(row.sales)>0?null:"Google purchase/revenue mapping is present but no validated value returned for this row.",
  revenue: googleSafeNumber(row.revenue)>0?null:"Google revenue field returned no positive value for this row.",
  roas: row.roas!==null&&row.roas!==undefined?null:"ROAS is null until revenue is validated and spend is greater than zero.",
  landing_page_views: row.landing_page_views!==null&&row.landing_page_views!==undefined?null:"Landing page view is unavailable unless the Google LPV query can be merged safely.",
  purchase_journey: "Purchase journey fields are kept in schema but remain zero/null when conversion-action mapping does not validate matching actions."
}}
function normalizeGoogleSnapshotRow(row){return {
  platform:"Google",
  level:row.level||null,
  campaign_id:row.campaign_id||null,
  campaign_name:row.campaign_name||null,
  campaign_status:row.campaign_status||null,
  channel_type:row.channel_type||null,
  bidding_strategy_type:row.bidding_strategy_type||null,
  adgroup_id:row.adgroup_id||null,
  adgroup_name:row.adgroup_name||null,
  adgroup_status:row.adgroup_status||null,
  ad_id:row.ad_id||null,
  ad_name:row.ad_name||null,
  ad_status:row.ad_status||null,
  currency:row.currency||null,
  spend:googleSafeNumber(row.spend),
  sales:googleSafeNumber(row.sales),
  revenue:googleSafeNumber(row.revenue),
  impressions:googleSafeNumber(row.impressions),
  clicks:googleSafeNumber(row.clicks),
  ctr:googleNullableNumber(row.ctr),
  cpc:googleNullableNumber(row.cpc),
  roas:googleNullableNumber(row.roas),
  conversions:googleSafeNumber(row.conversions),
  all_conversions:googleSafeNumber(row.all_conversions),
  conversion_rate:googleNullableNumber(row.conversion_rate),
  cvr:googleNullableNumber(row.cvr),
  acos:googleNullableNumber(row.acos),
  ad_clicks:googleSafeNumber(row.ad_clicks??row.clicks),
  link_clicks:googleSafeNumber(row.link_clicks??row.clicks),
  landing_page_views:row.landing_page_views===null||row.landing_page_views===undefined?0:googleSafeNumber(row.landing_page_views),
  traffic_score:googleNullableNumber(row.traffic_score),
  real_cpc:googleNullableNumber(row.real_cpc),
  invalid_clicks:googleSafeNumber(row.invalid_clicks),
  valid_clicks:googleSafeNumber(row.valid_clicks),
  add_to_cart:row.add_to_cart===null||row.add_to_cart===undefined?0:googleSafeNumber(row.add_to_cart),
  checkout:row.checkout===null||row.checkout===undefined?0:googleSafeNumber(row.checkout),
  purchase:row.purchase===null||row.purchase===undefined?0:googleSafeNumber(row.purchase),
  purchases:row.purchases===null||row.purchases===undefined?0:googleSafeNumber(row.purchases),
  purchase_value:row.purchase_value===null||row.purchase_value===undefined?0:googleSafeNumber(row.purchase_value),
  abandoned:row.abandoned===null||row.abandoned===undefined?0:googleSafeNumber(row.abandoned),
  source_confidence:{
    spend:"exact_google_ads_api",
    impressions:"exact_google_ads_api",
    clicks:"exact_google_ads_api",
    ctr:"google_ads_api_or_weighted",
    cpc:"google_ads_api_or_weighted",
    sales:googleSafeNumber(row.sales)>0?"google_conversions_value":"unavailable",
    revenue:googleSafeNumber(row.revenue)>0?"google_conversions_value":"unavailable",
    roas:row.roas!==null&&row.roas!==undefined?"calculated_from_revenue_and_spend":"unavailable",
    landing_page_views:row.landing_page_views!==null&&row.landing_page_views!==undefined?"google_lpv_merge":"unavailable"
  },
  null_reasons:googleSnapshotNullReasons(row),
  raw:row.raw||{}
}}
function googleEntityQuery(level){
  // Entity fallback must not depend on selected date range or delivery metrics.
  // Keep the query broad so empty-date accounts can still expose their campaign/adgroup/ad structure.
  if(level==="adgroup")return `SELECT customer.currency_code, campaign.id, campaign.name, campaign.status, ad_group.id, ad_group.name, ad_group.status FROM ad_group ORDER BY ad_group.id LIMIT 100`;
  if(level==="ad")return `SELECT customer.currency_code, campaign.id, campaign.name, campaign.status, ad_group.id, ad_group.name, ad_group.status, ad_group_ad.ad.id, ad_group_ad.ad.name, ad_group_ad.status FROM ad_group_ad ORDER BY ad_group_ad.ad.id LIMIT 100`;
  return `SELECT customer.currency_code, campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type FROM campaign ORDER BY campaign.id LIMIT 100`;
}
function googleEmptyAccountDiagnosticRow({customerId,loginCustomerId,level}){
  const base={
    customer:{currencyCode:null},
    campaign:{id:String(customerId),name:`Google customer ${customerId}`,status:"NO_ENTITY_RETURNED",advertisingChannelType:null},
    metrics:{},
    __entity_fallback:true,
    __diagnostic_empty_account:true
  };
  if(level==="adgroup"){
    base.campaign={id:String(customerId),name:`Google customer ${customerId}`,status:"NO_ENTITY_RETURNED"};
    base.adGroup={id:`${customerId}:no_adgroup`,name:"No Google adgroup rows returned",status:"NO_ENTITY_RETURNED"};
  }else if(level==="ad"){
    base.campaign={id:String(customerId),name:`Google customer ${customerId}`,status:"NO_ENTITY_RETURNED"};
    base.adGroup={id:`${customerId}:no_adgroup`,name:"No Google adgroup rows returned",status:"NO_ENTITY_RETURNED"};
    base.adGroupAd={ad:{id:`${customerId}:no_ad`,name:"No Google ad rows returned"},status:"NO_ENTITY_RETURNED"};
  }else{
    base.campaign={id:String(customerId),name:"No Google campaign rows returned",status:"NO_ENTITY_RETURNED",advertisingChannelType:null};
  }
  const out=googleZeroEntitySnapshotRow(base,level);
  out.raw={...(out.raw||{}),diagnostic_empty_account:true,customerId:String(customerId),loginCustomerId:loginCustomerId?String(loginCustomerId):null,reason:"Google entity query returned zero rows; diagnostic row keeps Performance Dataset pipeline visible with zero metrics."};
  return out;
}
function googleZeroEntitySnapshotRow(row,level){
  const normalized=normalizeGoogleInsight({...row,metrics:{},__entity_fallback:true},level);
  const out=normalizeGoogleSnapshotRow(normalized);
  out.raw={...(out.raw||{}),entity_fallback:true,reason:"No metrics row returned for selected date range; entity row preserved with zero/unknown metrics."};
  return out;
}
async function googleFetchEntityRowsForLevel({user,customerId,loginCustomerId,level}){
  try{
    const data=await googleAdsSearch(user.id,customerId,googleEntityQuery(level),loginCustomerId);
    const results=data.results||[];
    const rows=results.map(r=>googleZeroEntitySnapshotRow(r,level));
    if(!rows.length){
      rows.push(googleEmptyAccountDiagnosticRow({customerId,loginCustomerId,level}));
    }
    return {rows,requestId:data.requestId||null,rawCount:results.length,error:null,diagnosticFallback:results.length===0};
  }catch(err){
    return {rows:[googleEmptyAccountDiagnosticRow({customerId,loginCustomerId,level})],requestId:null,rawCount:0,error:err.message,diagnosticFallback:true};
  }
}
async function googleFetchInsightsForLevel({user,customerId,loginCustomerId,dateRange,level}){
  const data=await googleAdsSearch(user.id,customerId,googleQuery(level,dateRange),loginCustomerId);
  let breakdownData={results:[]},breakdownError=null;
  try{breakdownData=await googleAdsSearch(user.id,customerId,googleConversionBreakdownQuery(level,dateRange),loginCustomerId)}catch(err){breakdownError=err.message}
  let lpvData={results:[]},lpvError=null;
  try{lpvData=await googleAdsSearch(user.id,customerId,googleLandingPageViewQuery(level,dateRange),loginCustomerId)}catch(err){lpvError=err.message}
  const withConversions=mergeGoogleConversionActions(data.results||[],breakdownData.results||[],level);
  const mergedRows=mergeGoogleLandingPageViews(withConversions,lpvData.results||[],level);
  let rows=mergedRows.map(r=>normalizeGoogleSnapshotRow(normalizeGoogleInsight(r,level)));
  let entityFallback=false, entityRequestId=null, entityRawCount=0, entityResult=null;
  if(!rows.length){
    entityResult=await googleFetchEntityRowsForLevel({user,customerId,loginCustomerId,level});
    rows=entityResult.rows;
    entityFallback=rows.length>0;
    entityRequestId=entityResult.requestId;
    entityRawCount=entityResult.rawCount;
  }
  return {
    level,
    rows,
    rawCount:mergedRows.length,
    entityFallback,
    entityRawCount,
    entityRequestId,
    entityFallbackError:entityResult?.error||null,
    entityDiagnosticFallback:entityResult?.diagnosticFallback||false,
    conversionBreakdownCount:(breakdownData.results||[]).length,
    conversionBreakdownError:breakdownError,
    landingPageViewCount:(lpvData.results||[]).length,
    landingPageViewError:lpvError,
    requestId:data.requestId||null
  };
}
function buildGoogleSnapshotPayload({snapshotDate,accountCurrency,campaignRows,adgroupRows,adRows}){
  const rows=[...(campaignRows||[]),...(adgroupRows||[]),...(adRows||[])];
  const aggregateRows=(campaignRows&&campaignRows.length)?campaignRows:rows;
  const spend=googleSum(aggregateRows,"spend");
  const revenue=googleSum(aggregateRows,"revenue");
  const sales=googleSum(aggregateRows,"sales");
  const impressions=googleSum(aggregateRows,"impressions");
  const clicks=googleSum(aggregateRows,"clicks");
  const ctr=clicks>0&&impressions>0?(clicks/impressions)*100:googleWeightedAverage(aggregateRows,"ctr","impressions");
  const cpc=clicks>0?spend/clicks:googleWeightedAverage(aggregateRows,"cpc","clicks");
  const roas=spend>0&&revenue>0?revenue/spend:null;
  const addToCart=googleSum(aggregateRows,"add_to_cart");
  const checkout=googleSum(aggregateRows,"checkout");
  const purchase=googleSum(aggregateRows,"purchase");
  const purchaseValue=googleSum(aggregateRows,"purchase_value");
  const abandoned=Math.max(checkout-purchase,0);
  const linkClicks=googleSum(aggregateRows,"link_clicks")||clicks;
  const landingPageViews=googleSum(aggregateRows,"landing_page_views");
  return {
    snapshot_date:snapshotDate,
    account_currency:accountCurrency||null,
    kpis:{
      spend,
      sales,
      revenue,
      impressions,
      clicks,
      ctr,
      cpc,
      roas,
      source_confidence:{
        spend:"exact_google_ads_api",
        impressions:"exact_google_ads_api",
        clicks:"exact_google_ads_api",
        ctr:"google_ads_api_or_calculated",
        cpc:"google_ads_api_or_calculated",
        sales:sales>0?"google_conversions_value":"unavailable",
        revenue:revenue>0?"google_conversions_value":"unavailable",
        roas:roas!==null?"calculated_from_revenue_and_spend":"unavailable"
      },
      null_reasons:{
        sales:sales>0?null:"Google purchase/revenue mapping returned no positive validated value.",
        revenue:revenue>0?null:"Google revenue field returned no positive validated value.",
        roas:roas!==null?null:"ROAS is null until revenue is validated and spend is greater than zero."
      }
    },
    purchase_journey:{
      arrived:landingPageViews||0,
      add_to_cart:addToCart||0,
      checkout:checkout||0,
      purchase:purchase||0,
      purchase_count:purchase||0,
      purchase_value:purchaseValue||0,
      abandoned,
      source_confidence:addToCart||checkout||purchase?"google_conversion_action_mapping":"unavailable",
      null_reasons:{
        add_to_cart:addToCart?null:"Add-to-cart conversion action not validated or no value returned.",
        checkout:checkout?null:"Checkout conversion action not validated or no value returned.",
        purchase:purchase?null:"Purchase conversion action not validated or no value returned.",
        purchase_value:purchaseValue?null:"Purchase value not validated or no value returned."
      }
    },
    click_journey:{
      ad_clicks:clicks,
      link_clicks:linkClicks,
      landing_page_views:landingPageViews||0,
      traffic_score:linkClicks>0&&landingPageViews>0?(landingPageViews/linkClicks)*100:null,
      real_cpc:landingPageViews>0?spend/landingPageViews:null,
      source_confidence:{
        ad_clicks:"exact_google_ads_api",
        link_clicks:"fallback_to_clicks",
        landing_page_views:landingPageViews>0?"google_lpv_merge":"unavailable"
      },
      null_reasons:{
        landing_page_views:landingPageViews>0?null:"Landing page view unavailable or could not be safely merged."
      }
    },
    performance_summary:{
      rows,
      counts:{campaign:(campaignRows||[]).length,adgroup:(adgroupRows||[]).length,ad:(adRows||[]).length},
      source_confidence:"snapshot_layer_google_v1",
      null_policy:"Fields are present even when values are zero/null; null_reasons explain missing data."
    }
  };
}
async function writeGoogleSnapshotImmutable({user,customerId,loginCustomerId="",dateRange="today",snapshotDate,sourceJobId=null,captureReason="manual_refresh",snapshotClass="primary"}){
  const platformAccountId=normalizeCustomerId(customerId);
  if(!platformAccountId)throw new Error("Missing Google customerId");
  const platformTimeZone=await getPlatformAccountTimezone(user.id,"google",platformAccountId,null,null);
  const timeSync=resolveAdminTimeSync(new Date(),platformTimeZone);
  const effectiveSnapshotDate=e2aSnapshotDate(snapshotDate,platformTimeZone);
  const period=googleDateRangeWindow(dateRange,effectiveSnapshotDate);
  const [campaignResult,adgroupResult,adResult]=await Promise.all([
    googleFetchInsightsForLevel({user,customerId:platformAccountId,loginCustomerId,dateRange,level:"campaign"}),
    googleFetchInsightsForLevel({user,customerId:platformAccountId,loginCustomerId,dateRange,level:"adgroup"}),
    googleFetchInsightsForLevel({user,customerId:platformAccountId,loginCustomerId,dateRange,level:"ad"})
  ]);
  const allRows=[...campaignResult.rows,...adgroupResult.rows,...adResult.rows];
  const platformBaseCurrency=(allRows.find(r=>r.currency)?.currency)||null;
  const accountCurrency=await getUserAccountCurrency(user.id)||normalizeCurrency(platformBaseCurrency)||DEFAULT_REPORTING_CURRENCY;
  const fx=await resolveFxRate(platformBaseCurrency,accountCurrency,{rateDate:effectiveSnapshotDate});
  const rawSnapshot=buildGoogleSnapshotPayload({snapshotDate:effectiveSnapshotDate,accountCurrency:platformBaseCurrency||accountCurrency,campaignRows:campaignResult.rows,adgroupRows:adgroupResult.rows,adRows:adResult.rows});
  const snapshot=applyFxToSnapshotPayload(rawSnapshot,fx);
  const existingVersionResult=await supabaseAdmin
    .from("dashboard_snapshots")
    .select("snapshot_version")
    .eq("user_id",user.id)
    .eq("platform","google")
    .eq("platform_account_id",platformAccountId)
    .eq("snapshot_date",snapshot.snapshot_date)
    .order("snapshot_version",{ascending:false})
    .limit(1)
    .maybeSingle();
  if(existingVersionResult.error)throw existingVersionResult.error;
  const snapshotVersion=Number(existingVersionResult.data?.snapshot_version||0)+1;
  const now=new Date().toISOString();
  const row={
    user_id:user.id,
    platform:"google",
    platform_account_id:platformAccountId,
    platform_base_currency:platformBaseCurrency,
    snapshot_version:snapshotVersion,
    source_job_id:sourceJobId,
    date_preset:dateRange,
    snapshot_period_start:period.start,
    snapshot_period_end:period.end,
    snapshot_scope:dateRange,
    capture_reason:captureReason,
    snapshot_class:snapshotClass,
    platform_account_timezone:platformTimeZone,
    platform_business_date:timeSync.platform_business_date,
    platform_business_at:timeSync.platform_business_at||timeSync.server_time_utc,
    platform_business_hour:timeSync.platform_business_hour,
    data_maturity_window_hours:dataMaturityWindowHours("google"),
    server_time_utc:timeSync.server_time_utc,
    istanbul_time:timeSync.istanbul_time,
    platform_account_time:timeSync.platform_account_time,
    time_engine_version:TIME_ENGINE_VERSION,
    fx_rate:fx.fx_rate,
    fx_provider:fx.fx_provider,
    fx_rate_timestamp:fx.fx_rate_timestamp,
    fx_rate_date:fx.fx_rate_date||null,
    fx_source_currency:fx.fx_source_currency,
    fx_target_currency:fx.fx_target_currency,
    fx_engine_version:fx.fx_engine_version,
    snapshot_date:snapshot.snapshot_date,
    snapshot_created_at:now,
    account_currency:snapshot.account_currency,
    kpis:snapshot.kpis,
    purchase_journey:snapshot.purchase_journey,
    click_journey:snapshot.click_journey,
    performance_summary:snapshot.performance_summary
  };
  const {data,error}=await supabaseAdmin
    .from("dashboard_snapshots")
    .insert(row)
    .select("id,user_id,platform,platform_account_id,platform_base_currency,snapshot_version,source_job_id,date_preset,snapshot_period_start,snapshot_period_end,snapshot_scope,capture_reason,snapshot_class,platform_account_timezone,platform_business_date,platform_business_at,platform_business_hour,data_maturity_window_hours,server_time_utc,istanbul_time,platform_account_time,time_engine_version,fx_rate,fx_provider,fx_rate_timestamp,fx_rate_date,fx_source_currency,fx_target_currency,fx_engine_version,snapshot_date,snapshot_created_at,account_currency,kpis,purchase_journey,click_journey,performance_summary")
    .maybeSingle();
  if(error)throw error;
  let performance_spread_result=null;
  if(shouldSpreadSnapshotToPerformanceDataset(data)){
    try{
      performance_spread_result=await spreadSnapshotToPerformanceDataset(data);
    }catch(performanceSpreadError){
      performance_spread_result={ok:false,error:performanceSpreadError.message};
    }
  }else{
    performance_spread_result={ok:true,skipped:true,reason:"recovery_snapshot_not_written_to_dataset",snapshot_id:data.id};
  }
  return {mode:"insert",snapshot:data,row_counts:snapshot.performance_summary.counts,performance_spread_result,google_api:{campaign:campaignResult,adgroup:adgroupResult,ad:adResult}};
}

async function resolveGoogleRefreshAccount(user,requestedCustomerId=null){
  const requested=normalizeCustomerId(requestedCustomerId);
  const requestedLogin=normalizeCustomerId(
    arguments.length>2?arguments[2]:""
  );
  if(requested)return {customerId:requested,loginCustomerId:requestedLogin,source:"request"};

  // Google Snapshot must follow the same working account pair as Google Test:
  // loginCustomerId = Manager/MCC, customerId = test Ad Account.
  // Do not fall back to the first platform_ad_accounts row; it may select a non-test account.
  const snapshotCustomerId=normalizeCustomerId(GOOGLE_SNAPSHOT_CUSTOMER_ID);
  const snapshotLoginCustomerId=normalizeCustomerId(GOOGLE_SNAPSHOT_LOGIN_CUSTOMER_ID);
  if(snapshotCustomerId){
    return {customerId:snapshotCustomerId,loginCustomerId:snapshotLoginCustomerId,source:"google_snapshot_config"};
  }

  const conn=await getConnection(user.id,"google").catch(()=>null);
  const fromConn=normalizeCustomerId(
    conn?.metadata?.selectedCustomerId||
    conn?.metadata?.selected_customer_id||
    conn?.metadata?.customerId||
    conn?.metadata?.customer_id||
    conn?.account_id||
    conn?.metadata?.lastOwnedPlatformAccountId||
    conn?.metadata?.platform_account_id
  );
  if(fromConn)return {customerId:fromConn,loginCustomerId:conn?.metadata?.loginCustomerId||conn?.metadata?.login_customer_id||"",source:"platform_connections"};

  const err=new Error("Missing Google customerId and no configured Google snapshot account found");
  err.status=400;
  err.stage="input";
  throw err;
}

async function ensureGoogleSnapshotLifecycle(user,platformAccountId,loginCustomerId="",metadata={}){
  const normalized=normalizeCustomerId(platformAccountId);
  if(!normalized)throw new Error("Google platform account id is required for lifecycle");

  const account={
    id:normalized,
    platform_account_id:normalized,
    customerId:normalized,
    account_name:metadata.accountName||metadata.name||`Google customer ${normalized}`,
    name:metadata.accountName||metadata.name||`Google customer ${normalized}`,
    currency:metadata.currency||metadata.currency_code||null,
    timezone:metadata.timezone||metadata.timezone_name||DEFAULT_PLATFORM_TIMEZONE,
    loginCustomerId:loginCustomerId||null,
    accountResolutionSource:metadata.accountResolutionSource||"google_snapshot_config",
    ...metadata
  };

  let ownership=await getOwnership("google",normalized);
  if(ownership&&ownership.owner_user_id!==user.id&&activeOwnershipStatuses().includes(ownership.status)){
    const err=new Error("Platform account already owned by another user");
    err.status=409;
    throw err;
  }

  if(!ownership||ownership.owner_user_id!==user.id||!activeOwnershipStatuses().includes(ownership.status)){
    // No silent reuse/fallback is allowed. If the user has reached the Google account
    // limit, ensurePlatformOwnership must reject the new account instead of mutating an
    // existing ownership row.
    ownership=await ensurePlatformOwnership(user.id,"google",account);
  }

  const schedule=await ensureSnapshotSchedule(user.id,"google",normalized,{loginCustomerId:loginCustomerId||null,source:"google_snapshot_lifecycle",accountResolutionSource:"google_snapshot_config",timeEngineVersion:TIME_ENGINE_VERSION});
  return {ownership,schedule};
}

async function runGoogleAutoRefreshForSchedule(schedule){
  let job=null;
  const runDate=new Date();

  const {data:user,error:userError}=await supabaseAdmin
    .from("users")
    .select("*")
    .eq("id",schedule.user_id)
    .maybeSingle();
  if(userError)throw userError;
  if(!user)throw new Error("Auto refresh user not found");

  const {data:conn,error:connError}=await supabaseAdmin
    .from("platform_connections")
    .select("*")
    .eq("user_id",schedule.user_id)
    .eq("platform","google")
    .eq("connected",true)
    .maybeSingle();
  if(connError)throw connError;
  if(!conn)throw new Error("Auto refresh Google connection not found");

  const resolved=await resolveGoogleRefreshAccount(user,schedule.platform_account_id||conn.account_id||GOOGLE_SNAPSHOT_CUSTOMER_ID,schedule.metadata?.loginCustomerId||conn.metadata?.loginCustomerId||conn.metadata?.login_customer_id||GOOGLE_SNAPSHOT_LOGIN_CUSTOMER_ID);
  const platformAccountId=normalizeCustomerId(schedule.platform_account_id||resolved.customerId);
  const loginCustomerId=normalizeCustomerId(schedule.metadata?.loginCustomerId||resolved.loginCustomerId||GOOGLE_SNAPSHOT_LOGIN_CUSTOMER_ID||"");
  if(!platformAccountId)throw new Error("Auto refresh missing Google customer id");

  if(schedule.active===false){
    return {ok:true,skipped:true,reason:"schedule_inactive",schedule_id:schedule.id,platform_account_id:platformAccountId};
  }

  let ownership=await getOwnership("google",platformAccountId);
  if(!ownership||ownership.owner_user_id!==schedule.user_id||!activeOwnershipStatuses().includes(ownership.status)){
    const lifecycle=await ensureGoogleSnapshotLifecycle(user,platformAccountId,loginCustomerId,{accountName:conn.account_name||`Google customer ${platformAccountId}`,source:"google_auto_refresh"});
    ownership=lifecycle.ownership;
  }
  if(!ownership||ownership.owner_user_id!==schedule.user_id||!activeOwnershipStatuses().includes(ownership.status)){
    return {ok:true,skipped:true,reason:"ownership_not_active",schedule_id:schedule.id,platform_account_id:platformAccountId,ownership_status:ownership?.status||null};
  }

  const platformTimeZone=await getPlatformAccountTimezone(schedule.user_id,"google",platformAccountId,conn,ownership);
  const policy=resolveAutoRefreshPolicy({date:runDate,platformTimeZone,platform:"google"});
  const snapshotDate=e2aSnapshotDate(null,platformTimeZone);

  if(!policy.isAutomationHour){
    return {
      ok:true,
      skipped:true,
      reason:"not_platform_automation_hour",
      schedule_id:schedule.id,
      platform_account_id:platformAccountId,
      platform_account_timezone:platformTimeZone,
      platform_business_hour:policy.platform_business_hour,
      platform_account_time:policy.platform_account_time,
      server_time_utc:policy.server_time_utc,
      istanbul_time:policy.istanbul_time,
      automation_hours:policy.automation_hours
    };
  }

  job=await createRefreshJob(schedule.user_id,"google",platformAccountId,{
    trigger:"automation",
    dateRange:policy.datePreset,
    datePreset:policy.datePreset,
    snapshotDate,
    captureReason:policy.captureReason,
    snapshotClass:policy.snapshotClass,
    scheduleId:schedule.id,
    loginCustomerId,
    platformHour:policy.hour,
    platformBusinessHour:policy.platform_business_hour,
    dataMaturityWindowHours:policy.data_maturity_window_hours,
    server_time_utc:policy.server_time_utc,
    istanbul_time:policy.istanbul_time,
    platform_account_time:policy.platform_account_time,
    platform_account_timezone:policy.platform_account_timezone,
    platform_business_date:policy.platform_business_date,
    timeEngineVersion:TIME_ENGINE_VERSION
  });

  await setRefreshJobStatus(job.id,"running");

  try{
    const writeResult=await writeGoogleSnapshotImmutable({
      user,
      customerId:platformAccountId,
      loginCustomerId,
      dateRange:policy.datePreset,
      snapshotDate,
      sourceJobId:job.id,
      captureReason:policy.captureReason,
      snapshotClass:policy.snapshotClass
    });

    await setRefreshJobStatus(job.id,"completed",{snapshot_id:writeResult.snapshot?.id||null,metadata:{...(job.metadata||{}),row_counts:writeResult.row_counts||null,performance_spread_result:writeResult.performance_spread_result||null}});

    let recovery_result=null;
    if(policy.shouldRunRecoverySnapshot){
      const recoveryJob=await createRefreshJob(schedule.user_id,"google",platformAccountId,{
        trigger:"automation",
        dateRange:policy.recoveryDatePreset,
        datePreset:policy.recoveryDatePreset,
        snapshotDate,
        captureReason:policy.recoveryCaptureReason,
        snapshotClass:policy.recoverySnapshotClass,
        scheduleId:schedule.id,
        loginCustomerId,
        pairedPrimaryJobId:job.id,
        timeEngineVersion:TIME_ENGINE_VERSION
      });
      await setRefreshJobStatus(recoveryJob.id,"running");
      try{
        const recoveryWrite=await writeGoogleSnapshotImmutable({
          user,
          customerId:platformAccountId,
          loginCustomerId,
          dateRange:policy.recoveryDatePreset,
          snapshotDate,
          sourceJobId:recoveryJob.id,
          captureReason:policy.recoveryCaptureReason,
          snapshotClass:policy.recoverySnapshotClass
        });
        await setRefreshJobStatus(recoveryJob.id,"completed",{snapshot_id:recoveryWrite.snapshot?.id||null,metadata:{row_counts:recoveryWrite.row_counts||null,performance_spread_result:recoveryWrite.performance_spread_result||null}});
        recovery_result={ok:true,job_id:recoveryJob.id,snapshot_id:recoveryWrite.snapshot?.id||null};
      }catch(recoveryError){
        await setRefreshJobStatus(recoveryJob.id,"failed",{error_message:recoveryError.message}).catch(()=>null);
        recovery_result={ok:false,job_id:recoveryJob.id,error:recoveryError.message};
      }
    }

    await supabaseAdmin
      .from("snapshot_schedules")
      .update({
        last_run_at:new Date().toISOString(),
        next_run_at:nextAutomationSlotUtc(),
        updated_at:new Date().toISOString()
      })
      .eq("id",schedule.id);

    return {ok:true,job_id:job.id,snapshot_id:writeResult.snapshot?.id||null,snapshot_version:writeResult.snapshot?.snapshot_version||null,snapshot_class:writeResult.snapshot?.snapshot_class||policy.snapshotClass,date_preset:policy.datePreset,capture_reason:policy.captureReason,platform_account_timezone:platformTimeZone,platform_account_time:policy.platform_account_time,server_time_utc:policy.server_time_utc,istanbul_time:policy.istanbul_time,row_counts:writeResult.row_counts||null,performance_spread_result:writeResult.performance_spread_result||null};
  }catch(e){
    await setRefreshJobStatus(job.id,"failed",{error_message:e.message}).catch(()=>null);
    throw e;
  }
}

async function handleGoogleSnapshotWrite(req,res){
  let job=null;
  let stage="auth";
  try{
    const user=await requireUser(req,res);
    if(!user)return;
    const accessCheck=await requireAccess(req,res,user.id,"manualRefresh");
    if(!accessCheck)return;
    const requestedCustomerId=req.body?.customerId||req.body?.customer_id||req.query.customerId||req.query.customer_id;
    const requestedLoginCustomerId=req.body?.loginCustomerId||req.body?.login_customer_id||req.query.loginCustomerId||req.query.login_customer_id||"";
    const resolvedGoogleAccount=await resolveGoogleRefreshAccount(user,requestedCustomerId,requestedLoginCustomerId);
    const platformAccountId=normalizeCustomerId(resolvedGoogleAccount.customerId);
    const loginCustomerId=normalizeCustomerId(requestedLoginCustomerId||resolvedGoogleAccount.loginCustomerId||"");
    const dateRange=String(req.body?.date_range||req.body?.dateRange||req.query.date_range||req.query.dateRange||"today");
    const snapshotDate=e2aSnapshotDate(req.body?.snapshot_date||req.query.snapshot_date,DEFAULT_PLATFORM_TIMEZONE);
    stage="lifecycle";
    await ensureGoogleSnapshotLifecycle(user,platformAccountId,loginCustomerId,{accountName:`Google customer ${platformAccountId}`,source:"manual_google_refresh"});
    stage="job";
    job=await createRefreshJob(user.id,"google",platformAccountId,{trigger:"manual",dateRange,snapshotDate,captureReason:"manual_refresh",snapshotClass:"primary",timeEngineVersion:TIME_ENGINE_VERSION,accountResolutionSource:resolvedGoogleAccount.source,loginCustomerId});
    await setRefreshJobStatus(job.id,"running");
    stage="google_api";
    const writeResult=await writeGoogleSnapshotImmutable({user,customerId:platformAccountId,loginCustomerId,dateRange,snapshotDate,sourceJobId:job.id,captureReason:"manual_refresh",snapshotClass:"primary"});
    stage="snapshot";
    await setRefreshJobStatus(job.id,"completed",{snapshot_id:writeResult.snapshot?.id||null,metadata:{...(job.metadata||{}),row_counts:writeResult.row_counts,performance_spread_result:writeResult.performance_spread_result||null,google_api:{campaign:{rawCount:writeResult.google_api.campaign.rawCount,effectiveRows:writeResult.google_api.campaign.rows?.length||0,entityFallback:writeResult.google_api.campaign.entityFallback||false,entityRawCount:writeResult.google_api.campaign.entityRawCount||0,entityFallbackError:writeResult.google_api.campaign.entityFallbackError||null,entityDiagnosticFallback:writeResult.google_api.campaign.entityDiagnosticFallback||false,conversionBreakdownError:writeResult.google_api.campaign.conversionBreakdownError,landingPageViewError:writeResult.google_api.campaign.landingPageViewError},adgroup:{rawCount:writeResult.google_api.adgroup.rawCount,effectiveRows:writeResult.google_api.adgroup.rows?.length||0,entityFallback:writeResult.google_api.adgroup.entityFallback||false,entityRawCount:writeResult.google_api.adgroup.entityRawCount||0,entityFallbackError:writeResult.google_api.adgroup.entityFallbackError||null,entityDiagnosticFallback:writeResult.google_api.adgroup.entityDiagnosticFallback||false,conversionBreakdownError:writeResult.google_api.adgroup.conversionBreakdownError,landingPageViewError:writeResult.google_api.adgroup.landingPageViewError},ad:{rawCount:writeResult.google_api.ad.rawCount,effectiveRows:writeResult.google_api.ad.rows?.length||0,entityFallback:writeResult.google_api.ad.entityFallback||false,entityRawCount:writeResult.google_api.ad.entityRawCount||0,entityFallbackError:writeResult.google_api.ad.entityFallbackError||null,entityDiagnosticFallback:writeResult.google_api.ad.entityDiagnosticFallback||false,conversionBreakdownError:writeResult.google_api.ad.conversionBreakdownError,landingPageViewError:writeResult.google_api.ad.landingPageViewError}}}});
    res.json({
      ok:true,
      platform:"Google",
      refresh_job:{id:job.id,status:"completed"},
      mode:writeResult.mode,
      snapshot_id:writeResult.snapshot?.id||null,
      snapshot_date:writeResult.snapshot?.snapshot_date||snapshotDate,
      snapshot_version:writeResult.snapshot?.snapshot_version||null,
      snapshot_class:writeResult.snapshot?.snapshot_class||null,
      platform_account_id:writeResult.snapshot?.platform_account_id||platformAccountId,
      platform_base_currency:writeResult.snapshot?.platform_base_currency||null,
      account_currency:writeResult.snapshot?.account_currency||null,
      row_counts:writeResult.row_counts,
      kpis:writeResult.snapshot?.kpis||{},
      purchase_journey:writeResult.snapshot?.purchase_journey||{},
      click_journey:writeResult.snapshot?.click_journey||{},
      performance_summary_counts:writeResult.snapshot?.performance_summary?.counts||{}
    });
  }catch(e){
    if(job?.id)await setRefreshJobStatus(job.id,"failed",{error_message:e.message}).catch(()=>null);
    res.status(e.status||500).json({ok:false,error:e.message,stage,refresh_job_id:job?.id||null});
  }
}
app.get("/api/snapshots/google/write",handleGoogleSnapshotWrite);
app.post("/api/snapshots/google/write",handleGoogleSnapshotWrite);
app.get("/api/refresh/google",handleGoogleSnapshotWrite);
app.post("/api/refresh/google",handleGoogleSnapshotWrite);
// ===== END GOOGLE SNAPSHOT WRITE v1 =====

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

app.get("/api/pinterest/adaccounts",async(req,res)=>{try{const result=await requireConnection(req,res,"pinterest");if(!result)return;const{user,conn}=result;const data=await pinterestFetch(conn,"/ad_accounts");const accounts=data.items||[];res.json(data)}catch(e){res.status(500).json({error:e.message})}});
app.get("/api/accounts",async(req,res)=>{try{const user=await requireUser(req,res);if(!user)return;const{data,error}=await supabaseAdmin.from("platform_ad_accounts").select("*").eq("user_id",user.id).order("platform",{ascending:true});if(error)throw error;res.json({accounts:data||[]})}catch(e){res.status(500).json({error:e.message})}});

app.post("/api/accounts/select",async(req,res)=>{
  try{
    const user=await requireUser(req,res);if(!user)return;
    const platform=String(req.body?.platform||req.query.platform||"").toLowerCase();
    const selectedAccounts=req.body?.accounts||req.body?.selectedAccounts||[];
    const result=await selectPlatformAccountsForLifecycle(user.id,platform,selectedAccounts);
    res.json(result);
  }catch(e){res.status(e.status||500).json({ok:false,error:e.message,code:e.code||null,limit:e.limit||null,activeCount:e.activeCount||null,selectedCount:e.selectedCount||null})}
});

app.get("/api/accounts/selection-status",async(req,res)=>{
  try{
    const user=await requireUser(req,res);if(!user)return;
    const platform=String(req.query.platform||"").toLowerCase();
    const platforms=platform?[platform]:Object.keys(PHASE1_PLATFORM_LIMITS);
    const rows=[];
    for(const p of platforms){
      const limit=PHASE1_PLATFORM_LIMITS[p]||3;
      const activeCount=await countActiveOwnerships(user.id,p);
      rows.push({platform:p,limit,active_count:activeCount,remaining:Math.max(limit-activeCount,0),platform_status:passiveLegacyPlatformStatus(p),message:passiveLegacyPlatform(p)?passiveLegacyPlatform(p).message:(activeCount>=limit?accountSelectionLimitMessage(p,limit,limit):null)});
    }
    res.json({ok:true,platforms:rows});
  }catch(e){res.status(e.status||500).json({ok:false,error:e.message})}
});


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
    const result=await disconnectPlatformLifecycle(user.id,"meta");
    res.json({state:"NOT_CONNECTED",...result});
  }catch(e){
    res.status(e.status||500).json({error:e.message});
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
    const result=await disconnectPlatformLifecycle(user.id,"google");
    res.json({state:"NOT_CONNECTED",...result});
  }catch(e){
    res.status(e.status||500).json({error:e.message});
  }
});
// ===== END D.2A.2 GOOGLE CONNECT / DISCONNECT =====


// ===== D.2A.3 ORGANIC CONNECT / DISCONNECT SKELETON =====
app.get("/api/platform/organic/status",async(req,res)=>{
  try{
    const user=await requireUser(req,res);
    if(!user)return;

    const conn=await getConnection(user.id,"organic");
    const ownershipCount=await countActiveOwnerships(user.id,"organic");
    res.json({
      platform:"organic",
      label:"Organic",
      state: conn ? "CONNECTED" : "NOT_CONNECTED",
      connected:Boolean(conn),
      account_limit:PHASE1_PLATFORM_LIMITS.organic,
      active_ownership_count:ownershipCount,
      setup_stage:conn?.metadata?.setupStage||"skeleton",
      oauth_connected:Boolean(conn&&(conn.access_token||conn.refresh_token)),
      ga4_property_selection_required:Boolean(conn?.metadata?.ga4PropertySelectionRequired),
      search_console_site_selection_required:Boolean(conn?.metadata?.searchConsoleSiteSelectionRequired),
      ga4_connected:false,
      search_console_connected:false,
      message:conn?"Organic OAuth connected. GA4 Property and Search Console Site discovery are available.":"Organic platform skeleton is ready. Connect Organic to discover GA4 Properties and Search Console Sites."
    });
  }catch(e){
    res.status(500).json({error:e.message});
  }
});

app.post("/api/platform/organic/disconnect",async(req,res)=>{
  try{
    const user=await requireUser(req,res);
    if(!user)return;
    const result=await disconnectPlatformLifecycle(user.id,"organic");
    res.json({state:"NOT_CONNECTED",...result});
  }catch(e){
    res.status(e.status||500).json({error:e.message});
  }
});
// ===== END D.2A.3 ORGANIC CONNECT / DISCONNECT SKELETON =====

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
    const result=await disconnectPlatformLifecycle(user.id,"klaviyo");
    res.json({state:"NOT_CONNECTED",...result});
  }catch(e){
    res.status(e.status||500).json({error:e.message});
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


// ===== TIKTOK READ LAYER (OAuth + Test Reads Only) =====
function tiktokClientId(){return process.env.TIKTOK_CLIENT_ID||process.env.TIKTOK_APP_ID||""}
function tiktokClientSecret(){return process.env.TIKTOK_CLIENT_SECRET||process.env.TIKTOK_SECRET||process.env.TIKTOK_APP_SECRET||""}
function tiktokRedirectUri(){return process.env.TIKTOK_REDIRECT_URI||""}
function parseTikTokExpiry(value){return value?new Date(Date.now()+Number(value)*1000).toISOString():null}
const TIKTOK_TRUTH_CONTRACT_VERSION="v1";
const TIKTOK_TRUTH_FIELDS=[
  {field:"campaign_name",category:"documented",default_behavior:null,null_reason:"Campaign detail/read response has not provided campaign_name for this row."},
  {field:"campaign_status",category:"documented",default_behavior:null,null_reason:"Campaign detail/read response has not provided campaign_status for this row."},
  {field:"adgroup_name",category:"documented",default_behavior:null,null_reason:"AdGroup detail/read response has not provided adgroup_name for this row."},
  {field:"adgroup_status",category:"documented",default_behavior:null,null_reason:"AdGroup detail/read response has not provided adgroup_status for this row."},
  {field:"ad_name",category:"documented",default_behavior:null,null_reason:"Ad detail/read response has not provided ad_name for this row."},
  {field:"ad_status",category:"documented",default_behavior:null,null_reason:"Ad detail/read response has not provided ad_status for this row."},
  {field:"currency",category:"advertiser_validation_required",default_behavior:"N/A",null_reason:"Advertiser/account currency has not been validated for this row."},
  {field:"destination_click",category:"advertiser_validation_required",default_behavior:"N/A",null_reason:"TikTok response did not provide destination_click for this advertiser/report configuration."},
  {field:"landing_page_click",category:"advertiser_validation_required",default_behavior:"N/A",null_reason:"TikTok response did not provide landing_page_click for this advertiser/report configuration."},
  {field:"landing_page_view",category:"tracking_dependent",default_behavior:0,null_reason:"Pixel/website tracking did not provide landing_page_view."},
  {field:"add_to_cart",category:"tracking_dependent",default_behavior:0,null_reason:"Pixel event add_to_cart is not available for this response."},
  {field:"checkout",category:"tracking_dependent",default_behavior:0,null_reason:"Checkout event is not available for this response."},
  {field:"initiate_checkout",category:"tracking_dependent",default_behavior:0,null_reason:"Initiate checkout event is not available for this response."},
  {field:"complete_payment_count",category:"advertiser_validation_required",default_behavior:null,null_reason:"Complete payment count has not been validated from advertiser reporting."},
  {field:"complete_payment_value",category:"advertiser_validation_required",default_behavior:null,null_reason:"Complete payment value/revenue has not been validated from advertiser reporting."},
  {field:"roas",category:"calculated",default_behavior:null,null_reason:"ROAS is disabled until complete_payment_value is validated."},
  {field:"acos",category:"calculated",default_behavior:null,null_reason:"ACOS is disabled until complete_payment_value is validated."},
  {field:"cvr",category:"calculated",default_behavior:null,null_reason:"CVR is disabled until complete_payment_count is validated."},
  {field:"traffic_score",category:"calculated",default_behavior:null,null_reason:"Traffic score is disabled until landing_page_view and link click family are validated."},
  {field:"real_cpc",category:"calculated",default_behavior:null,null_reason:"Real CPC is disabled until landing_page_view is validated."},
  {field:"abandoned",category:"calculated",default_behavior:null,null_reason:"Abandoned is disabled until checkout and complete_payment_count are validated."}
];
function tiktokTruthContract(){return {version:TIKTOK_TRUTH_CONTRACT_VERSION,fields:TIKTOK_TRUTH_FIELDS,hard_rules:{conversion_cannot_equal_purchase:true,roas_requires_validated_complete_payment_value:true,tracking_events_cannot_be_inferred:true,snapshot_write:false,production_write:false}}}
function tiktokTruthMetaFor(field){return TIKTOK_TRUTH_FIELDS.find(x=>x.field===field)||null}
function tiktokFirstValue(sources,keys){for(const src of sources){if(!src||typeof src!=="object")continue;for(const key of keys){if(src[key]!==undefined&&src[key]!==null&&src[key]!=="")return src[key]}}return undefined}
function tiktokApplyTruthField(output,sources,field,keys){const meta=tiktokTruthMetaFor(field);const found=tiktokFirstValue(sources,keys||[field]);const value=found!==undefined?found:meta?.default_behavior??null;output[field]=value;output.truth[field]={category:meta?.category||"unknown",value_source:found!==undefined?"api_response":"default",default_behavior:meta?.default_behavior??null,null_reason:found!==undefined?null:(meta?.null_reason||null)};}
function normalizeTikTokRows(data,level="campaign"){
  const list=Array.isArray(data?.data?.list)?data.data.list:[];
  return list.map(item=>{
    const dimensions=item.dimensions||{};
    const metrics=item.metrics||{};
    const sources=[dimensions,metrics,item];
    const row={dimensions,metrics,raw:item,truth_contract_version:TIKTOK_TRUTH_CONTRACT_VERSION,truth:{}};
    tiktokApplyTruthField(row,sources,"campaign_name",["campaign_name","campaignName"]);
    tiktokApplyTruthField(row,sources,"campaign_status",["campaign_status","campaignStatus","campaign_operation_status","operation_status"]);
    tiktokApplyTruthField(row,sources,"adgroup_name",["adgroup_name","ad_group_name","adgroupName"]);
    tiktokApplyTruthField(row,sources,"adgroup_status",["adgroup_status","ad_group_status","adgroupStatus","operation_status"]);
    tiktokApplyTruthField(row,sources,"ad_name",["ad_name","adName"]);
    tiktokApplyTruthField(row,sources,"ad_status",["ad_status","adStatus","operation_status"]);
    tiktokApplyTruthField(row,sources,"currency",["currency","currency_code"]);
    tiktokApplyTruthField(row,sources,"destination_click",["destination_click","destination_clicks"]);
    tiktokApplyTruthField(row,sources,"landing_page_click",["landing_page_click","landing_page_clicks"]);
    tiktokApplyTruthField(row,sources,"landing_page_view",["landing_page_view","landing_page_views"]);
    tiktokApplyTruthField(row,sources,"add_to_cart",["add_to_cart","add_to_cart_count"]);
    tiktokApplyTruthField(row,sources,"checkout",["checkout","checkout_count"]);
    tiktokApplyTruthField(row,sources,"initiate_checkout",["initiate_checkout","initiate_checkout_count"]);
    tiktokApplyTruthField(row,sources,"complete_payment_count",["complete_payment_count","complete_payment","complete_payment_events"]);
    tiktokApplyTruthField(row,sources,"complete_payment_value",["complete_payment_value","complete_payment_value_onsite","total_complete_payment_rate_value"]);
    for(const field of ["roas","acos","cvr","traffic_score","real_cpc","abandoned"]){tiktokApplyTruthField(row,sources,field,[field])}
    row.hard_rules={conversion_is_purchase:false,calculated_fields_disabled:true,level};
    return row;
  })
}
function tiktokDateWindow(range,startDate,endDate){
  const end=endDate?new Date(`${endDate}T00:00:00Z`):new Date();
  const start=startDate?new Date(`${startDate}T00:00:00Z`):new Date(end);
  if(!startDate){
    if(range==="today"){}
    else if(range==="yesterday"){start.setDate(start.getDate()-1);end.setDate(end.getDate()-1)}
    else if(range==="last_30d")start.setDate(start.getDate()-29);
    else start.setDate(start.getDate()-6);
  }
  const fmt=d=>d.toISOString().slice(0,10);
  return {start:fmt(start),end:fmt(end)};
}
function resolveTikTokReportLevel(level){
  const l=String(level||"campaign").toLowerCase();
  if(l==="adgroup"||l==="ad_group")return {level:"adgroup",dataLevel:"AUCTION_ADGROUP",dimension:"adgroup_id"};
  if(l==="ad")return {level:"ad",dataLevel:"AUCTION_AD",dimension:"ad_id"};
  return {level:"campaign",dataLevel:"AUCTION_CAMPAIGN",dimension:"campaign_id"};
}
async function tiktokApiFetch({base=TIKTOK_API_BASE,endpoint,token,headers={},params={}}){
  const cleanBase=String(base).replace(/\/+$/,"");
  const cleanEndpoint=String(endpoint||"").startsWith("/")?endpoint:`/${endpoint}`;
  const url=new URL(`${cleanBase}${cleanEndpoint}`);
  for(const [k,v] of Object.entries(params||{})){
    if(v===undefined||v===null||v==="")continue;
    url.searchParams.set(k,Array.isArray(v)||typeof v==="object"?JSON.stringify(v):String(v));
  }
  const r=await fetch(url,{headers:{...headers}});
  const text=await r.text();
  let data;try{data=text?JSON.parse(text):{}}catch{data={raw:text}}
  if(!r.ok)throw new Error(data.message||data.error?.message||text||`TikTok API error ${r.status}`);
  return data;
}

async function bootstrapTikTokFromReport(userId,conn,advertiserId,context={}){
  const normalized=normalizePlatformAccountId(advertiserId);
  if(!normalized)throw new Error("TikTok advertiser_id is required for report bootstrap");
  const now=new Date().toISOString();
  const account={
    id:normalized,
    advertiser_id:normalized,
    account_name:context.account_name||conn?.account_name||`TikTok Advertiser ${normalized}`,
    name:context.account_name||conn?.account_name||`TikTok Advertiser ${normalized}`,
    status:"active",
    currency:context.currency||conn?.metadata?.baseCurrency||null,
    timezone:context.timezone||conn?.metadata?.timezone||DEFAULT_PLATFORM_TIMEZONE,
    bootstrap_source:"report",
    report_base:context.base||TIKTOK_API_BASE,
    report_endpoint:context.endpoint||"/v1.3/report/integrated/get/",
    report_level:context.level||null,
    report_date:context.date||null,
    token_source:context.tokenSource||"platform_connections.access_token",
    bootstrapped_at:now
  };
  const ownership=await ensurePlatformOwnership(userId,"tiktok",{platform_account_id:normalized,account_name:account.account_name,name:account.name,currency:account.currency,metadata:account});
  const schedule=await ensureSnapshotSchedule(userId,"tiktok",normalized,{
    engine:"vercel_cron_auto_refresh",
    account_type:phase1ReportableAccountType("tiktok"),
    accountResolutionSource:"tiktok_report_bootstrap",
    bootstrapSource:"report",
    reportBase:context.base||TIKTOK_API_BASE,
    reportEndpoint:context.endpoint||"/v1.3/report/integrated/get/",
    tokenSource:context.tokenSource||"platform_connections.access_token",
    lifecycleVersion:DISCONNECT_LIFECYCLE_VERSION,
    bootstrappedAt:now
  });
  await saveConnection(userId,"tiktok",{
    accountId:normalized,
    accountName:account.account_name,
    metadata:{
      ...(conn?.metadata||{}),
      lastOwnedPlatformAccountId:normalized,
      selectedPlatformAccountId:normalized,
      accountResolutionSource:"tiktok_report_bootstrap",
      bootstrapSource:"report",
      bootstrapAt:now,
      reportBase:context.base||TIKTOK_API_BASE,
      tokenSource:context.tokenSource||"platform_connections.access_token"
    }
  });
  return {ok:true,platform:"tiktok",platform_account_id:normalized,ownership_id:ownership?.id||null,schedule_id:schedule?.id||null,accountResolutionSource:"tiktok_report_bootstrap"};
}


app.get("/auth/tiktok",async(req,res)=>{
  try{
    const accessCheck=await requireConnectAccessForOAuth(req,res);if(!accessCheck)return;
    const userId=accessCheck.userId;

    // TIKTOK_AUTH_GUARD_FIX_v1
    // If TikTok is already connected and lifecycle-bound to an account, do not restart OAuth.
    const existingTikTokConnection=await getConnection(userId,"tiktok").catch(()=>null);
    if(existingTikTokConnection?.access_token&&normalizePlatformAccountId(existingTikTokConnection.account_id)){
      return res.redirect("/dashboard?tiktok_already_connected=1");
    }

    if(!tiktokClientId()||!tiktokRedirectUri())throw new Error("Missing TikTok OAuth env");
    const state=Math.random().toString(36).slice(2);
    req.session.tiktokOAuthState=state;
    req.session.oauthUserId=userId;
    const p=new URLSearchParams({app_id:tiktokClientId(),redirect_uri:tiktokRedirectUri(),state});
    res.redirect(`${TIKTOK_AUTH_BASE}?${p}`);
  }catch(e){res.status(500).send(e.message)}
});

app.get("/auth/tiktok/callback",async(req,res)=>{
  try{
    const {state,error,error_description}=req.query;
    const authCode=req.query.auth_code||req.query.code;
    if(error)return res.redirect(`/dashboard?tiktok_error=${encodeURIComponent(error_description||error)}`);
    if(!authCode)return res.redirect("/dashboard?tiktok_error=missing_code");
    if(!state||state!==req.session.tiktokOAuthState)return res.redirect("/dashboard?tiktok_error=invalid_state");
    const userId=req.session.oauthUserId;
    if(!userId)return res.redirect("/dashboard?tiktok_error=missing_user_id");
    if(!tiktokClientId()||!tiktokClientSecret())throw new Error("Missing TikTok token env");
    const r=await fetch(`${TIKTOK_API_BASE}/v1.3/oauth2/access_token/`,{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({app_id:tiktokClientId(),secret:tiktokClientSecret(),auth_code:String(authCode)})
    });
    const data=await r.json().catch(()=>({}));
    if(!r.ok||data.code!==0||!data.data?.access_token)throw new Error(data.message||data.error?.message||"TikTok token exchange failed");
    await saveConnection(userId,"tiktok",{
      accessToken:data.data.access_token,
      refreshToken:data.data.refresh_token||null,
      tokenExpiresAt:parseTikTokExpiry(data.data.expires_in),
      metadata:{scope:data.data.scope||null,openId:data.data.open_id||null,expiresIn:data.data.expires_in||null,tokenType:data.data.token_type||null}
    });
    req.session.tiktokOAuthState=null;
    res.redirect("/dashboard?tiktok_connected=1&account_selection_required=1");
  }catch(e){res.redirect(`/dashboard?tiktok_error=${encodeURIComponent(e.message)}`)}
});

app.get("/api/tiktok/status",async(req,res)=>{
  try{
    const user=await requireUser(req,res);if(!user)return;
    const conn=await getConnection(user.id,"tiktok");
    res.json({
      connected:Boolean(conn&&(conn.access_token||conn.refresh_token)),
      account_id:conn?.account_id||null,
      account_name:conn?.account_name||null,
      token_expires_at:conn?.token_expires_at||null,
      metadata:conn?.metadata||{}
    });
  }catch(e){res.status(500).json({error:e.message})}
});

app.get("/api/tiktok/truth-contract",async(req,res)=>{
  try{
    const user=await requireUser(req,res);if(!user)return;
    res.json({platform:"tiktok",truth_contract:tiktokTruthContract()});
  }catch(e){res.status(500).json({error:e.message})}
});

app.get("/api/tiktok/advertisers",async(req,res)=>{
  try{
    const result=await requireConnection(req,res,"tiktok");if(!result)return;
    const {conn}=result;
    if(!conn?.access_token)return res.status(400).json({error:"TikTok access token is required for advertiser resolution"});
    const data=await tiktokApiFetch({
      base:TIKTOK_API_BASE,
      endpoint:"/v1.3/oauth2/advertiser/get/",
      headers:{"Access-Token":conn.access_token},
      params:{app_id:tiktokClientId(),secret:tiktokClientSecret()}
    });
    const list=Array.isArray(data?.data?.list)?data.data.list:[];
    const advertisers=list.map(a=>({
      advertiser_id:a.advertiser_id||a.id||null,
      advertiser_name:a.advertiser_name||a.name||null,
      status:a.status||a.advertiser_status||null,
      currency:a.currency||a.currency_code||null
    }));
    res.json({platform:"tiktok",advertisers,raw:data});
  }catch(e){res.status(e.status||500).json({error:e.message})}
});

app.get("/api/tiktok/report",async(req,res)=>{
  try{
    const user=await requireUser(req,res);if(!user)return;
    const sandbox=String(req.query.sandbox||"false").toLowerCase()==="true";
    let token=null,tokenSource=null;
    if(sandbox){
      token=String(req.query.sandbox_access_token||req.headers["x-sandbox-access-token"]||"").trim();
      tokenSource="manual_sandbox_access_token";
      if(!token)return res.status(400).json({error:"sandbox_access_token is required when sandbox=true"});
    }else{
      const conn=await getConnection(user.id,"tiktok");
      if(!conn?.access_token)return res.status(404).json({error:"tiktok not connected"});
      token=conn.access_token;
      tokenSource="platform_connections.access_token";
    }
    const advertiserId=String(req.query.advertiser_id||req.query.advertiserId||"").trim();
    if(!advertiserId)return res.status(400).json({error:"advertiser_id is required"});
    const levelInfo=resolveTikTokReportLevel(req.query.level);
    const date=String(req.query.date||req.query.date_range||"last_7d");
    const w=tiktokDateWindow(date,req.query.start_date,req.query.end_date);
    const metrics=["spend","impressions","clicks","ctr","cpc","conversion"];
    const base=sandbox?TIKTOK_SANDBOX_API_BASE:TIKTOK_API_BASE;
    const endpoint="/v1.3/report/integrated/get/";
    const headers={"Access-Token":token};
    const params={
      report_type:"BASIC",
      data_level:levelInfo.dataLevel,
      advertiser_id:advertiserId,
      start_date:w.start,
      end_date:w.end,
      dimensions:[levelInfo.dimension],
      metrics,
      page:1,
      page_size:20
    };
    const data=await tiktokApiFetch({base,endpoint,headers,params});
    let bootstrap=null;
    if(!sandbox&&data?.code===0){
      const conn=await getConnection(user.id,"tiktok");
      bootstrap=await bootstrapTikTokFromReport(user.id,conn,advertiserId,{base,endpoint,level:levelInfo.level,date,tokenSource});
    }
    res.json({
      platform:"tiktok",
      sandbox,
      advertiser_id:advertiserId,
      level:levelInfo.level,
      date,
      rows:normalizeTikTokRows(data,levelInfo.level),
      bootstrap,
      request:{sandbox,base:base.endsWith("/")?base:`${base}/`,endpoint,advertiser_id:advertiserId,level:levelInfo.level,date,start_date:w.start,end_date:w.end,data_level:levelInfo.dataLevel,dimensions:[levelInfo.dimension],metrics,token_source:tokenSource,truth_contract_version:TIKTOK_TRUTH_CONTRACT_VERSION},
      truth_contract:tiktokTruthContract(),
      raw:data
    });
  }catch(e){res.status(e.status||500).json({error:e.message})}
});


function aggregateRows(rows){
  const total=(field)=>rows.reduce((sum,row)=>sum+Number(row[field]||0),0);
  const spend=total("spend"), impressions=total("impressions"), clicks=total("clicks"), sales=total("sales"), revenue=total("revenue");
  return {spend,impressions,clicks,sales,revenue};
}

function tiktokNumber(v){const n=Number(v);return Number.isFinite(n)?n:0}
function tiktokNullableNumber(v){if(v===null||v===undefined||v===""||v==="N/A")return null;const n=Number(v);return Number.isFinite(n)?n:null}
function tiktokSnapshotRow(row,level,platformAccountId,synthetic=false){
  const dimensions=row?.dimensions||row?.raw?.dimensions||{};
  const id=dimensions.campaign_id||dimensions.adgroup_id||dimensions.ad_id||row?.campaign_id||row?.adgroup_id||row?.ad_id||platformAccountId;
  const spend=tiktokNumber(row?.spend);
  const clicks=tiktokNumber(row?.clicks);
  const impressions=tiktokNumber(row?.impressions);
  const conversions=tiktokNullableNumber(row?.complete_payment_count??row?.conversion)??0;
  const revenue=tiktokNullableNumber(row?.complete_payment_value)??0;
  return {
    platform:"TikTok",
    level,
    id:String(id),
    id_in_platform:String(id),
    campaign_id:level==="campaign"?String(id):(dimensions.campaign_id||row?.campaign_id||null),
    campaign_name:row?.campaign_name||row?.name||(synthetic?`TikTok Sandbox ${platformAccountId}`:null),
    campaign_status:row?.campaign_status||row?.status||(synthetic?"sandbox_empty_report":null),
    adgroup_id:level==="adgroup"?String(id):(dimensions.adgroup_id||row?.adgroup_id||null),
    adgroup_name:row?.adgroup_name||null,
    adgroup_status:row?.adgroup_status||null,
    ad_id:level==="ad"?String(id):(dimensions.ad_id||row?.ad_id||null),
    ad_name:row?.ad_name||null,
    ad_status:row?.ad_status||null,
    currency:row?.currency&&row.currency!=="N/A"?row.currency:null,
    spend,
    impressions,
    reach:null,
    clicks,
    ctr:tiktokNullableNumber(row?.ctr),
    cpc:tiktokNullableNumber(row?.cpc),
    sales:revenue,
    revenue,
    roas:spend>0&&revenue>0?revenue/spend:null,
    conversions,
    conversion_value:revenue,
    ad_clicks:clicks,
    link_clicks:tiktokNullableNumber(row?.destination_click??row?.landing_page_click)??clicks,
    landing_page_views:tiktokNullableNumber(row?.landing_page_view)??0,
    add_to_cart:tiktokNullableNumber(row?.add_to_cart)??0,
    checkout:tiktokNullableNumber(row?.checkout??row?.initiate_checkout)??0,
    purchase:conversions,
    purchases:conversions,
    purchase_value:revenue,
    abandoned:null,
    source_confidence:synthetic?"sandbox_empty_report_fallback":"tiktok_report_api",
    raw:{...((row&&typeof row.raw==="object")?row.raw:row),synthetic,zero_null_policy:"0 is measured zero; null is unknown/unavailable/not computable"}
  };
}

async function fetchTikTokSnapshotRows(conn,platformAccountId,datePreset){
  const sandboxToken=process.env.TIKTOK_SANDBOX_ACCESS_TOKEN||process.env.TIKTOK_TEST_ACCESS_TOKEN||"";
  const useSandbox=Boolean(sandboxToken&&(conn?.metadata?.tokenSource==="manual_sandbox_access_token"||conn?.metadata?.reportBase===TIKTOK_SANDBOX_API_BASE||process.env.TIKTOK_FORCE_SANDBOX_REPORTS==="1"));
  const token=useSandbox?sandboxToken:conn.access_token;
  const base=useSandbox?TIKTOK_SANDBOX_API_BASE:TIKTOK_API_BASE;
  const endpoint="/v1.3/report/integrated/get/";
  const headers={"Access-Token":token};
  const w=tiktokDateWindow(datePreset||"today");
  const metrics=["spend","impressions","clicks","ctr","cpc","conversion"];
  const levels=["campaign","adgroup","ad"];
  const result={rows:[],raw:{},counts:{campaign:0,adgroup:0,ad:0},tokenSource:useSandbox?"manual_sandbox_access_token":"platform_connections.access_token",base};
  for(const level of levels){
    const levelInfo=resolveTikTokReportLevel(level);
    const data=await tiktokApiFetch({base,endpoint,headers,params:{report_type:"BASIC",data_level:levelInfo.dataLevel,advertiser_id:platformAccountId,start_date:w.start,end_date:w.end,dimensions:[levelInfo.dimension],metrics,page:1,page_size:100}});
    const normalized=normalizeTikTokRows(data,levelInfo.level);
    const rows=normalized.map(r=>tiktokSnapshotRow(r,levelInfo.level,platformAccountId,false));
    result.raw[level]=data;
    result.counts[levelInfo.level]=rows.length;
    result.rows.push(...rows);
  }
  const shouldCreateFallbackRows=useSandbox||levels.some(level=>Number(result.counts[level]||0)===0);
  if(shouldCreateFallbackRows){
    const fallbackBase={raw:{fallback_reason:useSandbox?"sandbox_empty_report":"empty_report_level_fallback",token_source:result.tokenSource}};
    const fallbackRows=[
      {
        level:"campaign",
        row:{...fallbackBase,campaign_id:platformAccountId,name:`TikTok Campaign ${platformAccountId}`,campaign_name:`TikTok Campaign ${platformAccountId}`,campaign_status:"empty_period_fallback"}
      },
      {
        level:"adgroup",
        row:{...fallbackBase,campaign_id:platformAccountId,adgroup_id:`${platformAccountId}_adgroup_fallback`,adgroup_name:`TikTok AdGroup ${platformAccountId}`,adgroup_status:"empty_period_fallback"}
      },
      {
        level:"ad",
        row:{...fallbackBase,campaign_id:platformAccountId,adgroup_id:`${platformAccountId}_adgroup_fallback`,ad_id:`${platformAccountId}_ad_fallback`,ad_name:`TikTok Ad ${platformAccountId}`,ad_status:"empty_period_fallback"}
      }
    ];
    for(const fallback of fallbackRows){
      if(Number(result.counts[fallback.level]||0)>0)continue;
      result.rows.push(tiktokSnapshotRow(fallback.row,fallback.level,platformAccountId,true));
      result.counts[fallback.level]=1;
    }
  }
  return result;
}

function buildSnapshotPayloadFromPerformanceRows({platform,snapshotDate,accountCurrency,rows,counts,sourceConfidence,truthContract=null}){
  const totals=aggregateRows(rows);
  const addToCart=rows.reduce((sum,row)=>sum+Number(row.add_to_cart||0),0);
  const checkout=rows.reduce((sum,row)=>sum+Number(row.checkout||0),0);
  const purchase=rows.reduce((sum,row)=>sum+Number(row.purchase||row.purchases||0),0);
  const lpv=rows.reduce((sum,row)=>sum+Number(row.landing_page_views||0),0);
  const linkClicks=rows.reduce((sum,row)=>sum+Number(row.link_clicks||row.clicks||0),0);
  return {
    platform,
    snapshot_date:snapshotDate,
    account_currency:accountCurrency,
    kpis:{spend:totals.spend,sales:totals.sales,revenue:totals.revenue,impressions:totals.impressions,clicks:totals.clicks,ctr:totals.impressions>0?totals.clicks/totals.impressions*100:null,cpc:totals.clicks>0?totals.spend/totals.clicks:null,roas:totals.spend>0?totals.revenue/totals.spend:null},
    purchase_journey:{add_to_cart:addToCart,checkout,abandoned:checkout&&purchase!==null?Math.max(checkout-purchase,0):0,purchase,purchases:purchase,purchase_value:totals.revenue},
    click_journey:{ad_clicks:totals.clicks,link_clicks:linkClicks,landing_page_views:lpv,traffic_score:linkClicks>0&&lpv>0?lpv/linkClicks*100:null,real_cpc:lpv>0?totals.spend/lpv:null},
    performance_summary:{rows,counts,truth_contract:truthContract,source_confidence:sourceConfidence,null_policy:"Fields are present even when values are zero/null; fallback rows are explicitly marked in raw/source_confidence."}
  };
}

async function insertSnapshotAndSpread({user,platform,platformAccountId,platformBaseCurrency,snapshot,datePreset,period,sourceJobId,captureReason,snapshotClass,platformTimeZone,timeSync}){
  const targetCurrency=await getUserAccountCurrency(user.id)||normalizeCurrency(platformBaseCurrency)||normalizeCurrency(snapshot.account_currency)||DEFAULT_REPORTING_CURRENCY;
  const sourceCurrency=normalizeCurrency(platformBaseCurrency)||normalizeCurrency(snapshot.account_currency)||targetCurrency;
  const fx=await resolveFxRate(sourceCurrency,targetCurrency,{rateDate:snapshot.snapshot_date});
  const convertedSnapshot=applyFxToSnapshotPayload(snapshot,fx);

  const existingVersionResult=await supabaseAdmin.from("dashboard_snapshots").select("snapshot_version").eq("user_id",user.id).eq("platform",platform).eq("platform_account_id",platformAccountId).eq("snapshot_date",convertedSnapshot.snapshot_date).order("snapshot_version",{ascending:false}).limit(1).maybeSingle();
  if(existingVersionResult.error)throw existingVersionResult.error;
  const snapshotVersion=Number(existingVersionResult.data?.snapshot_version||0)+1;
  const now=new Date().toISOString();
  const row={user_id:user.id,platform,platform_account_id:platformAccountId,platform_base_currency:sourceCurrency,snapshot_version:snapshotVersion,source_job_id:sourceJobId,date_preset:datePreset,snapshot_period_start:period.start,snapshot_period_end:period.end,snapshot_scope:period.scope||datePreset,capture_reason:captureReason,snapshot_class:snapshotClass,platform_account_timezone:platformTimeZone,platform_business_date:timeSync.platform_business_date,platform_business_at:timeSync.platform_business_at||timeSync.server_time_utc,platform_business_hour:timeSync.platform_business_hour,data_maturity_window_hours:dataMaturityWindowHours(platform),server_time_utc:timeSync.server_time_utc,istanbul_time:timeSync.istanbul_time,platform_account_time:timeSync.platform_account_time,time_engine_version:TIME_ENGINE_VERSION,fx_rate:fx.fx_rate,fx_provider:fx.fx_provider,fx_rate_timestamp:fx.fx_rate_timestamp,fx_rate_date:fx.fx_rate_date||null,fx_source_currency:fx.fx_source_currency,fx_target_currency:fx.fx_target_currency,fx_engine_version:fx.fx_engine_version,snapshot_date:convertedSnapshot.snapshot_date,snapshot_created_at:now,account_currency:convertedSnapshot.account_currency,kpis:convertedSnapshot.kpis,purchase_journey:convertedSnapshot.purchase_journey,click_journey:convertedSnapshot.click_journey,performance_summary:convertedSnapshot.performance_summary};
  const {data,error}=await supabaseAdmin.from("dashboard_snapshots").insert(row).select("id,user_id,platform,platform_account_id,platform_base_currency,snapshot_version,source_job_id,date_preset,snapshot_period_start,snapshot_period_end,snapshot_scope,capture_reason,snapshot_class,platform_account_timezone,platform_business_date,platform_business_at,platform_business_hour,data_maturity_window_hours,server_time_utc,istanbul_time,platform_account_time,time_engine_version,fx_rate,fx_provider,fx_rate_timestamp,fx_rate_date,fx_source_currency,fx_target_currency,fx_engine_version,snapshot_date,snapshot_created_at,account_currency,kpis,purchase_journey,click_journey,performance_summary").maybeSingle();
  if(error)throw error;
  let performance_spread_result=null;
  if(shouldSpreadSnapshotToPerformanceDataset(data)){
    try{performance_spread_result=await spreadSnapshotToPerformanceDataset(data)}catch(e){performance_spread_result={ok:false,error:e.message}}
  }else{
    performance_spread_result={ok:true,skipped:true,reason:"recovery_snapshot_not_written_to_dataset",snapshot_id:data.id};
  }
  return {mode:"insert",snapshot:data,row_counts:convertedSnapshot.performance_summary.counts,performance_spread_result};
}
async function writeTikTokSnapshotImmutable({user,conn,platformAccountId,datePreset="today",snapshotDate,sourceJobId=null,captureReason="manual_refresh",snapshotClass="primary"}){
  const normalized=normalizePlatformAccountId(platformAccountId||conn?.account_id||conn?.metadata?.selectedPlatformAccountId);
  if(!normalized)throw new Error("Missing TikTok advertiser id");
  await requireActiveOwnership(user.id,"tiktok",normalized);
  const platformTimeZone=await getPlatformAccountTimezone(user.id,"tiktok",normalized,conn,null);
  const effectiveSnapshotDate=e2aSnapshotDate(snapshotDate,platformTimeZone);
  const period=resolveSnapshotCapturePeriod(datePreset,effectiveSnapshotDate,platformTimeZone,new Date());
  const timeSync=resolveAdminTimeSync(new Date(),platformTimeZone);
  const fetched=await fetchTikTokSnapshotRows(conn,normalized,period.datePreset);
  const platformBaseCurrency=fetched.rows.find(r=>r.currency)?.currency||conn?.metadata?.baseCurrency||null;
  const accountCurrency=await getUserAccountCurrency(user.id)||normalizeCurrency(platformBaseCurrency)||DEFAULT_REPORTING_CURRENCY;
  const snapshot=buildSnapshotPayloadFromPerformanceRows({platform:"tiktok",snapshotDate:effectiveSnapshotDate,accountCurrency:platformBaseCurrency||accountCurrency,rows:fetched.rows.map(r=>({...r,currency:r.currency||platformBaseCurrency||accountCurrency})),counts:fetched.counts,sourceConfidence:"snapshot_layer_tiktok_v2",truthContract:tiktokTruthContract()});
  snapshot.performance_summary.raw_report=fetched.raw;
  snapshot.performance_summary.token_source=fetched.tokenSource;
  return insertSnapshotAndSpread({user,platform:"tiktok",platformAccountId:normalized,platformBaseCurrency,snapshot,datePreset:period.datePreset,period,sourceJobId,captureReason,snapshotClass,platformTimeZone,timeSync});
}

async function handleTikTokSnapshotWrite(req,res){
  let job=null,stage="connection";
  try{
    const result=await requireConnection(req,res,"tiktok");if(!result)return;
    const {user,conn}=result;
    const requested=req.body?.advertiser_id||req.body?.advertiserId||req.body?.platform_account_id||req.query.advertiser_id||req.query.advertiserId||req.query.platform_account_id;
    const platformAccountId=normalizePlatformAccountId(requested||conn.account_id||conn.metadata?.selectedPlatformAccountId||conn.metadata?.lastOwnedPlatformAccountId);
    if(!platformAccountId)return res.status(400).json({ok:false,error:"Missing TikTok advertiser id",stage});
    const datePreset=String(req.body?.date_preset||req.body?.dateRange||req.query.date_preset||req.query.dateRange||"today");
    const platformTimeZone=await getPlatformAccountTimezone(user.id,"tiktok",platformAccountId,conn,null);
    const snapshotDate=e2aSnapshotDate(req.body?.snapshot_date||req.query.snapshot_date,platformTimeZone);
    stage="job";
    job=await createRefreshJob(user.id,"tiktok",platformAccountId,{trigger:"manual",datePreset,snapshotDate,captureReason:"manual_refresh",snapshotClass:"primary"});
    await setRefreshJobStatus(job.id,"running");
    stage="snapshot";
    const writeResult=await writeTikTokSnapshotImmutable({user,conn,platformAccountId,datePreset,snapshotDate,sourceJobId:job.id,captureReason:"manual_refresh",snapshotClass:"primary"});
    await setRefreshJobStatus(job.id,"completed",{snapshot_id:writeResult.snapshot?.id||null,metadata:{...(job.metadata||{}),performance_spread_result:writeResult.performance_spread_result||null}});
    res.json({ok:true,platform:"TikTok",refresh_job:{id:job.id,status:"completed"},snapshot_id:writeResult.snapshot?.id||null,snapshot_date:writeResult.snapshot?.snapshot_date||snapshotDate,platform_account_id:platformAccountId,row_counts:writeResult.row_counts,performance_spread_result:writeResult.performance_spread_result});
  }catch(e){if(job?.id)await setRefreshJobStatus(job.id,"failed",{error_message:e.message}).catch(()=>null);res.status(e.status||500).json({ok:false,error:e.message,stage,job_id:job?.id||null})}
}

async function writeKlaviyoSnapshotImmutable({user,conn,platformAccountId,datePreset="today",snapshotDate,sourceJobId=null,captureReason="manual_refresh",snapshotClass="primary"}){
  const normalized=normalizePlatformAccountId(platformAccountId||conn?.account_id||conn?.metadata?.selectedPlatformAccountId);
  if(!normalized)throw new Error("Missing Klaviyo account id");
  await requireActiveOwnership(user.id,"klaviyo",normalized);
  const platformTimeZone=await getPlatformAccountTimezone(user.id,"klaviyo",normalized,conn,null);
  const effectiveSnapshotDate=e2aSnapshotDate(snapshotDate,platformTimeZone);
  const period=resolveSnapshotCapturePeriod(datePreset,effectiveSnapshotDate,platformTimeZone,new Date());
  const timeSync=resolveAdminTimeSync(new Date(),platformTimeZone);
  const w=klaviyoDateWindow(period.datePreset);
  let campaigns=[];
  try{
    const filter=`equals(messages.channel,'email'),greater-or-equal(scheduled_at,${w.start}),less-or-equal(scheduled_at,${w.end})`;
    const campaignsData=await klaviyoFetch(conn,`/api/campaigns/?filter=${encodeURIComponent(filter)}`);
    campaigns=Array.isArray(campaignsData.data)?campaignsData.data:[];
  }catch(e){campaigns=[];}
  let placedOrderMetricId=process.env.KLAVIYO_PLACED_ORDER_METRIC_ID||null;
  if(!placedOrderMetricId){try{placedOrderMetricId=await getKlaviyoMetricId(conn,["Placed Order","Placed order","Order Placed"])}catch{} }
  const rows=[];
  for(const campaign of campaigns.slice(0,50)){
    let report=null;
    try{
      if(placedOrderMetricId){
        const body={data:{type:"campaign-values-report",attributes:{timeframe:{start:w.start,end:w.end},conversion_metric_id:placedOrderMetricId,filter:`equals(campaign_id,"${campaign.id}")`,statistics:["delivered","opens","clicks","click_rate","conversion_value","conversions"]}}};
        report=await klaviyoFetch(conn,"/api/campaign-values-reports/",{method:"POST",body:JSON.stringify(body)});
      }
    }catch{}
    const n=normalizeKlaviyoInsight({campaign,report,settings:conn.metadata||{},window:w});
    rows.push({platform:"Klaviyo",level:"campaign",id:String(n.campaign_id||campaign.id),id_in_platform:String(n.campaign_id||campaign.id),campaign_id:String(n.campaign_id||campaign.id),campaign_name:n.campaign_name,campaign_status:n.campaign_status,currency:n.currency||conn.metadata?.spendCurrency||null,spend:n.spend,impressions:n.impressions,reach:null,clicks:n.clicks,ctr:n.ctr,cpc:n.cpc,sales:n.sales,revenue:n.revenue,roas:n.roas,conversions:n.purchase,purchase:n.purchase,purchases:n.purchase,conversion_value:n.revenue,ad_clicks:n.opened_email||n.clicks,link_clicks:n.link_clicks,landing_page_views:n.landing_page_views||0,add_to_cart:n.add_to_cart||0,checkout:n.checkout||0,purchase_value:n.revenue,abandoned:n.abandoned,source_confidence:"klaviyo_api_or_estimated_spend",raw:n.raw});
  }
  if(!rows.length){
    const spend=Number(conn.metadata?.estimatedPeriodSpend||0)||0;
    rows.push({platform:"Klaviyo",level:"campaign",id:normalized,id_in_platform:normalized,campaign_id:normalized,campaign_name:conn.account_name||`Klaviyo Account ${normalized}`,campaign_status:"empty_period_fallback",currency:conn.metadata?.spendCurrency||null,spend,impressions:0,clicks:0,ctr:null,cpc:null,sales:0,revenue:0,roas:null,conversions:0,purchase:0,purchases:0,conversion_value:0,ad_clicks:0,link_clicks:0,landing_page_views:0,add_to_cart:0,checkout:0,purchase_value:0,abandoned:0,source_confidence:"klaviyo_empty_period_fallback",raw:{fallback_reason:"no_campaign_rows_for_period"}});
  }
  const platformBaseCurrency=rows.find(r=>r.currency)?.currency||conn.metadata?.spendCurrency||null;
  const accountCurrency=await getUserAccountCurrency(user.id)||normalizeCurrency(platformBaseCurrency)||DEFAULT_REPORTING_CURRENCY;
  const snapshot=buildSnapshotPayloadFromPerformanceRows({platform:"klaviyo",snapshotDate:effectiveSnapshotDate,accountCurrency:platformBaseCurrency||accountCurrency,rows:rows.map(r=>({...r,currency:r.currency||platformBaseCurrency||accountCurrency})),counts:{campaign:rows.length,adgroup:0,ad:0},sourceConfidence:"snapshot_layer_klaviyo_v1"});
  return insertSnapshotAndSpread({user,platform:"klaviyo",platformAccountId:normalized,platformBaseCurrency,snapshot,datePreset:period.datePreset,period,sourceJobId,captureReason,snapshotClass,platformTimeZone,timeSync});
}

async function handleKlaviyoSnapshotWrite(req,res){
  let job=null,stage="connection";
  try{
    const result=await requireConnection(req,res,"klaviyo");if(!result)return;
    const {user,conn}=result;
    if(conn.metadata?.requiresSetup)return res.status(400).json({ok:false,error:"Klaviyo setup required. Please enter estimated monthly spend and currency.",stage:"settings"});
    const platformAccountId=normalizePlatformAccountId(req.body?.platform_account_id||req.query.platform_account_id||conn.account_id||conn.metadata?.selectedPlatformAccountId||conn.metadata?.lastOwnedPlatformAccountId);
    if(!platformAccountId)return res.status(400).json({ok:false,error:"Missing Klaviyo account id",stage});
    const datePreset=String(req.body?.date_preset||req.body?.dateRange||req.query.date_preset||req.query.dateRange||"today");
    const platformTimeZone=await getPlatformAccountTimezone(user.id,"klaviyo",platformAccountId,conn,null);
    const snapshotDate=e2aSnapshotDate(req.body?.snapshot_date||req.query.snapshot_date,platformTimeZone);
    stage="job";
    job=await createRefreshJob(user.id,"klaviyo",platformAccountId,{trigger:"manual",datePreset,snapshotDate,captureReason:"manual_refresh",snapshotClass:"primary"});
    await setRefreshJobStatus(job.id,"running");
    stage="snapshot";
    const writeResult=await writeKlaviyoSnapshotImmutable({user,conn,platformAccountId,datePreset,snapshotDate,sourceJobId:job.id,captureReason:"manual_refresh",snapshotClass:"primary"});
    await setRefreshJobStatus(job.id,"completed",{snapshot_id:writeResult.snapshot?.id||null,metadata:{...(job.metadata||{}),performance_spread_result:writeResult.performance_spread_result||null}});
    res.json({ok:true,platform:"Klaviyo",refresh_job:{id:job.id,status:"completed"},snapshot_id:writeResult.snapshot?.id||null,snapshot_date:writeResult.snapshot?.snapshot_date||snapshotDate,platform_account_id:platformAccountId,row_counts:writeResult.row_counts,performance_spread_result:writeResult.performance_spread_result});
  }catch(e){if(job?.id)await setRefreshJobStatus(job.id,"failed",{error_message:e.message}).catch(()=>null);res.status(e.status||500).json({ok:false,error:e.message,stage,job_id:job?.id||null})}
}

async function runTikTokAutoRefreshForSchedule(schedule){
  const {data:user,error:userError}=await supabaseAdmin.from("users").select("*").eq("id",schedule.user_id).maybeSingle();
  if(userError)throw userError;if(!user)throw new Error("Auto refresh user not found");
  const conn=await getConnection(schedule.user_id,"tiktok");if(!conn)throw new Error("Auto refresh TikTok connection not found");
  const platformAccountId=normalizePlatformAccountId(schedule.platform_account_id||conn.account_id);if(!platformAccountId)throw new Error("Auto refresh missing TikTok advertiser id");
  const platformTimeZone=await getPlatformAccountTimezone(schedule.user_id,"tiktok",platformAccountId,conn,null);
  const policy=resolveAutoRefreshPolicy({date:new Date(),platformTimeZone,platform:"tiktok"});
  if(!policy.isAutomationHour)return {ok:true,skipped:true,platform:"tiktok",reason:"not_platform_automation_hour",schedule_id:schedule.id};
  const snapshotDate=e2aSnapshotDate(null,platformTimeZone);
  const job=await createRefreshJob(schedule.user_id,"tiktok",platformAccountId,{trigger:"automation",datePreset:policy.datePreset,snapshotDate,captureReason:policy.captureReason,snapshotClass:policy.snapshotClass,scheduleId:schedule.id});
  await setRefreshJobStatus(job.id,"running");
  try{
    const writeResult=await writeTikTokSnapshotImmutable({user,conn,platformAccountId,datePreset:policy.datePreset,snapshotDate,sourceJobId:job.id,captureReason:policy.captureReason,snapshotClass:policy.snapshotClass});
    await setRefreshJobStatus(job.id,"completed",{snapshot_id:writeResult.snapshot?.id||null});
    let recovery_result=null;
    if(policy.shouldRunRecoverySnapshot){
      const recoveryJob=await createRefreshJob(schedule.user_id,"tiktok",platformAccountId,{trigger:"automation",datePreset:policy.recoveryDatePreset,snapshotDate,captureReason:policy.recoveryCaptureReason,snapshotClass:policy.recoverySnapshotClass,scheduleId:schedule.id,pairedPrimaryJobId:job.id});
      await setRefreshJobStatus(recoveryJob.id,"running");
      try{
        const recoveryWrite=await writeTikTokSnapshotImmutable({user,conn,platformAccountId,datePreset:policy.recoveryDatePreset,snapshotDate,sourceJobId:recoveryJob.id,captureReason:policy.recoveryCaptureReason,snapshotClass:policy.recoverySnapshotClass});
        await setRefreshJobStatus(recoveryJob.id,"completed",{snapshot_id:recoveryWrite.snapshot?.id||null});
        recovery_result={ok:true,job_id:recoveryJob.id,snapshot_id:recoveryWrite.snapshot?.id||null};
      }catch(recoveryError){
        await setRefreshJobStatus(recoveryJob.id,"failed",{error_message:recoveryError.message}).catch(()=>null);
        recovery_result={ok:false,job_id:recoveryJob.id,error:recoveryError.message};
      }
    }
    await supabaseAdmin.from("snapshot_schedules").update({last_run_at:new Date().toISOString(),next_run_at:nextAutomationSlotUtc(),updated_at:new Date().toISOString()}).eq("id",schedule.id);
    return {ok:true,platform:"tiktok",job_id:job.id,snapshot_id:writeResult.snapshot?.id||null,row_counts:writeResult.row_counts,recovery_result};
  }catch(e){await setRefreshJobStatus(job.id,"failed",{error_message:e.message}).catch(()=>null);throw e}
}

async function runKlaviyoAutoRefreshForSchedule(schedule){
  const {data:user,error:userError}=await supabaseAdmin.from("users").select("*").eq("id",schedule.user_id).maybeSingle();
  if(userError)throw userError;if(!user)throw new Error("Auto refresh user not found");
  const conn=await getConnection(schedule.user_id,"klaviyo");if(!conn)throw new Error("Auto refresh Klaviyo connection not found");
  const platformAccountId=normalizePlatformAccountId(schedule.platform_account_id||conn.account_id);if(!platformAccountId)throw new Error("Auto refresh missing Klaviyo account id");
  const platformTimeZone=await getPlatformAccountTimezone(schedule.user_id,"klaviyo",platformAccountId,conn,null);
  const policy=resolveAutoRefreshPolicy({date:new Date(),platformTimeZone,platform:"klaviyo"});
  if(!policy.isAutomationHour)return {ok:true,skipped:true,platform:"klaviyo",reason:"not_platform_automation_hour",schedule_id:schedule.id};
  const snapshotDate=e2aSnapshotDate(null,platformTimeZone);
  const job=await createRefreshJob(schedule.user_id,"klaviyo",platformAccountId,{trigger:"automation",datePreset:policy.datePreset,snapshotDate,captureReason:policy.captureReason,snapshotClass:policy.snapshotClass,scheduleId:schedule.id});
  await setRefreshJobStatus(job.id,"running");
  try{
    const writeResult=await writeKlaviyoSnapshotImmutable({user,conn,platformAccountId,datePreset:policy.datePreset,snapshotDate,sourceJobId:job.id,captureReason:policy.captureReason,snapshotClass:policy.snapshotClass});
    await setRefreshJobStatus(job.id,"completed",{snapshot_id:writeResult.snapshot?.id||null});
    let recovery_result=null;
    if(policy.shouldRunRecoverySnapshot){
      const recoveryJob=await createRefreshJob(schedule.user_id,"klaviyo",platformAccountId,{trigger:"automation",datePreset:policy.recoveryDatePreset,snapshotDate,captureReason:policy.recoveryCaptureReason,snapshotClass:policy.recoverySnapshotClass,scheduleId:schedule.id,pairedPrimaryJobId:job.id});
      await setRefreshJobStatus(recoveryJob.id,"running");
      try{
        const recoveryWrite=await writeKlaviyoSnapshotImmutable({user,conn,platformAccountId,datePreset:policy.recoveryDatePreset,snapshotDate,sourceJobId:recoveryJob.id,captureReason:policy.recoveryCaptureReason,snapshotClass:policy.recoverySnapshotClass});
        await setRefreshJobStatus(recoveryJob.id,"completed",{snapshot_id:recoveryWrite.snapshot?.id||null});
        recovery_result={ok:true,job_id:recoveryJob.id,snapshot_id:recoveryWrite.snapshot?.id||null};
      }catch(recoveryError){
        await setRefreshJobStatus(recoveryJob.id,"failed",{error_message:recoveryError.message}).catch(()=>null);
        recovery_result={ok:false,job_id:recoveryJob.id,error:recoveryError.message};
      }
    }
    await supabaseAdmin.from("snapshot_schedules").update({last_run_at:new Date().toISOString(),next_run_at:nextAutomationSlotUtc(),updated_at:new Date().toISOString()}).eq("id",schedule.id);
    return {ok:true,platform:"klaviyo",job_id:job.id,snapshot_id:writeResult.snapshot?.id||null,row_counts:writeResult.row_counts,recovery_result};
  }catch(e){await setRefreshJobStatus(job.id,"failed",{error_message:e.message}).catch(()=>null);throw e}
}

// ===== END TIKTOK READ LAYER =====

if(process.env.VERCEL!=="1") app.listen(PORT,()=>console.log(`AdsTable server running on ${PORT}`));
module.exports=app;
