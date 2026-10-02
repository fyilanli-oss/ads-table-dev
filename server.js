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
function getLifecycleAccess(status){const s=normalizeAccountStatus(status);const full=["trial","active"].includes(s);const readonly=s==="expired";const blocked=["suspended","deleted"].includes(s);return{status:s||null,login:full||readonly,dashboard:full||readonly,snapshots:full||readonly,insightHistory:full||readonly,connect:full,manualRefresh:full,refresh:full,dailySync:full,export:full,aiInsights:full,blocked}}
async function getSubscriptionForLifecycle(userId){await expireTrialsIfNeeded();const{data,error}=await supabaseAdmin.from("subscriptions").select("status,trial_end_date").eq("user_id",userId).maybeSingle();if(error)throw error;return data}
registerAccountStatusRoutes({app,requireUser,getSubscription:getSubscriptionForLifecycle,getLifecycleAccess});
app.post("/api/account/request-delete",async(req,res)=>{try{const result=await requireLifecycleAccess(req,res,"dashboard");if(!result)return;const token=crypto.randomBytes(32).toString("hex");const expiresAt=new Date(Date.now()+30*60*1000).toISOString();const{error}=await supabaseAdmin.from("subscriptions").update({deletion_token:token,deletion_token_expires_at:expiresAt,updated_at:new Date().toISOString()}).eq("user_id",result.user.id).in("status",["trial","active","expired"]);if(error)throw error;const proto=req.headers["x-forwarded-proto"]||req.protocol||"https";const host=req.headers["x-forwarded-host"]||req.headers.host;const confirmationUrl=`${proto}://${host}/api/account/confirm-delete?token=${encodeURIComponent(token)}`;res.json({message:"Delete confirmation ready",confirmationUrl,tokenExpiresAt:expiresAt})}catch(e){res.status(500).json({error:e.message})}});
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
  const maxAttempts=3;
  for(let attempt=1;attempt<=maxAttempts;attempt+=1){
    const r=await fetch(url,{headers:{...headers}});
    const text=await r.text();
    let data,parsed=true;try{data=text?JSON.parse(text):{}}catch{parsed=false;data={}}
    const providerCode=Number(data?.code??0);
    const qpsLimited=providerCode===40100||r.status===429;
    if(r.ok&&parsed&&(data?.code===undefined||providerCode===0))return data;
    if(qpsLimited&&attempt<maxAttempts){await new Promise(resolve=>setTimeout(resolve,1100*attempt));continue;}
    const error=new Error(data.message||data.error?.message||(parsed?`TikTok API error ${r.status}`:'TikTok API returned malformed JSON'));
    error.status=qpsLimited?429:(r.ok?502:r.status);
    error.safe_stage="provider_fetch";
    throw error;
  }
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


const {start:handleTikTokOAuthStart,callback:handleTikTokOAuthCallback}=createTikTokOAuthHandlers({
  config:{clientId:tiktokClientId(),clientSecret:tiktokClientSecret(),redirectUri:tiktokRedirectUri(),authorizationBase:TIKTOK_AUTH_BASE},requireConnectAccess:requireConnectAccessForOAuth,createTransaction:createOAuthTransaction,consumeTransaction:consumeOAuthTransaction,sendAuthorizationResponse:sendOAuthAuthorizationResponse,getConnection,normalizeAccountId:normalizePlatformAccountId,
  exchangeToken:async({code,clientId,clientSecret})=>{const response=await fetch(`${TIKTOK_API_BASE}/v1.3/oauth2/access_token/`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({app_id:clientId,secret:clientSecret,auth_code:String(code)})});const payload=await response.json().catch(()=>({}));if(!response.ok||payload.code!==0||!payload.data?.access_token)throw new Error(payload.message||payload.error?.message||"TikTok token exchange failed");return payload.data},saveConnection,parseExpiry:parseTikTokExpiry
});
registerOAuthProviderRoutes({app,provider:"tiktok",startHandler:handleTikTokOAuthStart,callbackHandler:handleTikTokOAuthCallback,frozen:isStandaloneOAuthRouteFrozen("tiktok")});
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
    const {conn}=result,sandboxAccount=sandboxAdvertiser({productionConfig,advertiserId:TIKTOK_SANDBOX_ADVERTISER_ID,advertiserName:TIKTOK_SANDBOX_ADVERTISER_NAME,sandboxBase:TIKTOK_SANDBOX_API_BASE});if(sandboxAccount)return res.json({platform:"tiktok",advertisers:[sandboxAccount],advertiser_source:"non_production_sandbox",sandbox:true});
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

    // TikTok Review/Test routing:
    // OAuth account discovery can return no accessible advertisers even though
    // the approved test advertiser is queryable with the connected token.
    // Surface that advertiser in the existing account-selection flow.
    if(!advertisers.length&&productionConfig.tiktokReviewFallbackEnabled&&TIKTOK_SANDBOX_ADVERTISER_ID){
      advertisers.push({
        advertiser_id:TIKTOK_SANDBOX_ADVERTISER_ID,
        advertiser_name:TIKTOK_SANDBOX_ADVERTISER_NAME,
        status:"active",
        currency:null,
        review_fallback:true,sandbox:true,reportBase:TIKTOK_SANDBOX_API_BASE,tokenSource:"server_review_access_token"
      });
    }

    res.json({
      platform:"tiktok",
      advertisers,
      advertiser_source:list.length?"oauth_accessible_advertisers":"review_fallback",
      raw:data
    });
  }catch(e){res.status(e.status||500).json({error:e.message})}
});


app.get("/api/tiktok/campaigns",async(req,res)=>{
  try{
    const user=await requireUser(req,res);if(!user)return;
    if(Object.prototype.hasOwnProperty.call(req.query,"sandbox_access_token"))return res.status(400).json({error:"sandbox_access_token query parameter is not supported"});
    const sandbox=String(req.query.sandbox||"false").toLowerCase()==="true";
    const advertiserId=String(req.query.advertiser_id||req.query.advertiserId||"").trim();
    if(!advertiserId)return res.status(400).json({error:"advertiser_id is required"});

    let token=null,tokenSource=null;
    if(sandbox){
      if(!productionConfig.tiktokSandboxEnabled)return res.status(404).json({error:"TikTok sandbox mode is disabled"});
      token=String(req.headers["x-sandbox-access-token"]||"").trim();
      tokenSource="manual_sandbox_access_token";
      if(!token)return res.status(400).json({error:"sandbox_access_token is required when sandbox=true"});
    }else{
      const conn=await getConnection(user.id,"tiktok");
      if(!conn?.access_token)return res.status(404).json({error:"tiktok not connected"});
      token=conn.access_token;
      tokenSource="platform_connections.access_token";
    }

    const base=sandbox?TIKTOK_SANDBOX_API_BASE:TIKTOK_API_BASE;
    const endpoint="/v1.3/campaign/get/";
    const headers={"Access-Token":token};
    const params={
      advertiser_id:advertiserId,
      page:1,
      page_size:100
    };

    const data=await tiktokApiFetch({base,endpoint,headers,params});
    const list=Array.isArray(data?.data?.list)?data.data.list:[];
    const campaigns=list.map(item=>({
      campaign_id:item.campaign_id||item.id||null,
      campaign_name:item.campaign_name||item.name||null,
      campaign_status:item.secondary_status||item.operation_status||item.status||null,
      objective_type:item.objective_type||null,
      budget:item.budget??null,
      budget_mode:item.budget_mode||null,
      create_time:item.create_time||null,
      modify_time:item.modify_time||null
    }));

    res.json({
      platform:"tiktok",
      sandbox,
      advertiser_id:advertiserId,
      campaign_count:campaigns.length,
      campaigns,
      request:{
        sandbox,
        base:base.endsWith("/")?base:`${base}/`,
        endpoint,
        advertiser_id:advertiserId,
        page:1,
        page_size:100,
        token_source:tokenSource
      },
      raw:data
    });
  }catch(e){res.status(e.status||500).json({error:e.message})}
});

app.get("/api/tiktok/report",async(req,res)=>{
  try{
    const user=await requireUser(req,res);if(!user)return;
    if(Object.prototype.hasOwnProperty.call(req.query,"sandbox_access_token"))return res.status(400).json({error:"sandbox_access_token query parameter is not supported"});
    const sandbox=String(req.query.sandbox||"false").toLowerCase()==="true";
    let token=null,tokenSource=null;
    if(sandbox){
      if(!productionConfig.tiktokSandboxEnabled)return res.status(404).json({error:"TikTok sandbox mode is disabled"});
      token=String(req.headers["x-sandbox-access-token"]||"").trim();
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
    const bootstrap=null;
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
  const sandboxToken=productionConfig.tiktokSandboxEnabled?TIKTOK_SANDBOX_ACCESS_TOKEN:"";
  const useSandbox=Boolean(productionConfig.tiktokSandboxEnabled&&sandboxToken&&(conn?.metadata?.tokenSource==="manual_sandbox_access_token"||conn?.metadata?.reportBase===TIKTOK_SANDBOX_API_BASE||productionConfig.tiktokForceSandboxReports));const useReviewBridge=Boolean(productionConfig.tiktokReviewFallbackEnabled&&TIKTOK_SANDBOX_ACCESS_TOKEN&&conn?.metadata?.tokenSource==="server_review_access_token"&&conn?.metadata?.reportBase===TIKTOK_SANDBOX_API_BASE);
  const token=useReviewBridge?TIKTOK_SANDBOX_ACCESS_TOKEN:(useSandbox?sandboxToken:conn.access_token);
  const base=useReviewBridge||useSandbox?TIKTOK_SANDBOX_API_BASE:TIKTOK_API_BASE;
  const endpoint="/v1.3/report/integrated/get/";
  const headers={"Access-Token":token};
  const w=tiktokDateWindow(datePreset||"today");
  const metrics=["spend","impressions","clicks","ctr","cpc","conversion"];
  const levels=["campaign","adgroup","ad"];
  const result={rows:[],reportEvidence:{},counts:{campaign:0,adgroup:0,ad:0},tokenSource:useReviewBridge?"server_review_access_token":(useSandbox?"manual_sandbox_access_token":"platform_connections.access_token"),base};
  let previousRequestStartedAt=0;
  for(const level of levels){
    const waitMs=Math.max(0,1100-(Date.now()-previousRequestStartedAt));
    if(waitMs)await new Promise(resolve=>setTimeout(resolve,waitMs));
    previousRequestStartedAt=Date.now();
    const levelInfo=resolveTikTokReportLevel(level);
    const data=await tiktokApiFetch({base,endpoint,headers,params:{report_type:"BASIC",data_level:levelInfo.dataLevel,advertiser_id:platformAccountId,start_date:w.start,end_date:w.end,dimensions:[levelInfo.dimension],metrics,page:1,page_size:100}});
    const normalized=normalizeTikTokRows(data,levelInfo.level);
    const rows=normalized.map(r=>tiktokSnapshotRow(r,levelInfo.level,platformAccountId,false));
    result.reportEvidence[level]={status:"ok",row_count:rows.length,empty:rows.length===0};
    result.counts[levelInfo.level]=rows.length;
    result.rows.push(...rows);
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
    performance_summary:{rows,counts,truth_contract:truthContract,source_confidence:sourceConfidence,empty_result:rows.length===0,null_policy:"A successful empty provider report remains an empty row set; measured zero is never synthesized."}
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
  snapshot.performance_summary.report_evidence=fetched.reportEvidence;
  snapshot.performance_summary.token_source=fetched.tokenSource;
  return insertSnapshotAndSpread({user,platform:"tiktok",platformAccountId:normalized,platformBaseCurrency,snapshot,datePreset:period.datePreset,period,sourceJobId,captureReason,snapshotClass,platformTimeZone,timeSync});
}

async function handleTikTokSnapshotWrite(req,res){
  let job=null,stage="connection";
  try{
    const result=await requireRefreshConnection(req,res,"tiktok");if(!result)return;
    const {user,conn}=result;
    const requested=req.body?.advertiser_id||req.body?.advertiserId||req.body?.platform_account_id||req.query.advertiser_id||req.query.advertiserId||req.query.platform_account_id;
    const platformAccountId=normalizePlatformAccountId(requested||conn.account_id||conn.metadata?.selectedPlatformAccountId||conn.metadata?.lastOwnedPlatformAccountId);
    if(!platformAccountId)return res.status(400).json({ok:false,error:"Missing TikTok advertiser id",stage});
    const datePreset=String(req.body?.date_preset||req.body?.dateRange||req.query.date_preset||req.query.dateRange||"today");
    const platformTimeZone=await getPlatformAccountTimezone(user.id,"tiktok",platformAccountId,conn,null);
    const snapshotDate=e2aSnapshotDate(req.body?.snapshot_date||req.query.snapshot_date,platformTimeZone);
    stage="job";
    const execution=await manualSnapshotOrchestrator.run({userId:user.id,platform:"tiktok",platformAccountId,datePreset,snapshotDate,complete:(result,currentJob)=>{const legacy=result.legacy_result||result;return{snapshot_id:legacy.snapshot?.id||null,metadata:{...(currentJob.metadata||{}),performance_spread_result:legacy.performance_spread_result||null,tiktok_shadow_evidence:result.shadow_evidence||null}}},write:async jobContext=>{stage="snapshot";if(!tiktokV2LiveShadow)return writeTikTokSnapshotImmutable({user,conn,platformAccountId,datePreset,snapshotDate,...jobContext});const timezone=await getPlatformAccountTimezone(user.id,"tiktok",platformAccountId,conn,null),targetCurrency=await getUserAccountCurrency(user.id)||DEFAULT_REPORTING_CURRENCY,sourceCurrency=normalizeCurrency(conn?.metadata?.baseCurrency)||targetCurrency;return tiktokV2LiveShadow.run({request:{userId:user.id,advertiserId:platformAccountId,providerDate:snapshotDate},advertiser:{id:platformAccountId,currency:sourceCurrency,timezone},targetCurrency,sourceJobId:jobContext.sourceJobId,legacyWrite:()=>writeTikTokSnapshotImmutable({user,conn,platformAccountId,datePreset,snapshotDate,...jobContext})})}});
    job=execution.job;
    const shadowEvidence=execution.result.shadow_evidence||null,writeResult=execution.result.legacy_result||execution.result;
    const googleSheetsSync=req._skipGoogleSheetsAutoSync?{attempted:false,ok:true,skipped:true,reason:"global_refresh_deferred"}:await maybeAutoSyncGoogleSheets(user.id);
    res.json({ok:true,platform:"TikTok",refresh_job:{id:job.id,status:"completed"},snapshot_id:writeResult.snapshot?.id||null,snapshot_date:writeResult.snapshot?.snapshot_date||snapshotDate,platform_account_id:platformAccountId,row_counts:writeResult.row_counts,performance_spread_result:writeResult.performance_spread_result,tiktok_shadow:shadowEvidence,google_sheets_sync:googleSheetsSync});
  }catch(e){job=e.refreshJob||job;res.status(e.status||500).json({ok:false,error:e.message,stage,job_id:job?.id||null})}
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
  const platformBaseCurrency=rows.find(r=>r.currency)?.currency||conn.metadata?.spendCurrency||null;
  const accountCurrency=await getUserAccountCurrency(user.id)||normalizeCurrency(platformBaseCurrency)||DEFAULT_REPORTING_CURRENCY;
  const snapshot=buildSnapshotPayloadFromPerformanceRows({platform:"klaviyo",snapshotDate:effectiveSnapshotDate,accountCurrency:platformBaseCurrency||accountCurrency,rows:rows.map(r=>({...r,currency:r.currency||platformBaseCurrency||accountCurrency})),counts:{campaign:rows.length,adgroup:0,ad:0},sourceConfidence:"snapshot_layer_klaviyo_v1"});
  snapshot.performance_summary.empty_result=rows.length===0;
  return insertSnapshotAndSpread({user,platform:"klaviyo",platformAccountId:normalized,platformBaseCurrency,snapshot,datePreset:period.datePreset,period,sourceJobId,captureReason,snapshotClass,platformTimeZone,timeSync});
}

async function handleKlaviyoSnapshotWrite(req,res){
  let job=null,stage="connection";
  try{
    const result=await requireRefreshConnection(req,res,"klaviyo");if(!result)return;
    const {user,conn}=result;
    if(conn.metadata?.requiresSetup)return res.status(400).json({ok:false,error:"Klaviyo setup required. Please enter estimated monthly spend and currency.",stage:"settings"});
    const platformAccountId=normalizePlatformAccountId(req.body?.platform_account_id||req.query.platform_account_id||conn.account_id||conn.metadata?.selectedPlatformAccountId||conn.metadata?.lastOwnedPlatformAccountId);
    if(!platformAccountId)return res.status(400).json({ok:false,error:"Missing Klaviyo account id",stage});
    const datePreset=String(req.body?.date_preset||req.body?.dateRange||req.query.date_preset||req.query.dateRange||"today");
    const platformTimeZone=await getPlatformAccountTimezone(user.id,"klaviyo",platformAccountId,conn,null);
    const snapshotDate=e2aSnapshotDate(req.body?.snapshot_date||req.query.snapshot_date,platformTimeZone);
    stage="job";
    const execution=await manualSnapshotOrchestrator.run({userId:user.id,platform:"klaviyo",platformAccountId,datePreset,snapshotDate,write:jobContext=>{stage="snapshot";return writeKlaviyoSnapshotImmutable({user,conn,platformAccountId,datePreset,snapshotDate,...jobContext})}});
    job=execution.job;
    const writeResult=execution.result;
    const googleSheetsSync=req._skipGoogleSheetsAutoSync?{attempted:false,ok:true,skipped:true,reason:"global_refresh_deferred"}:await maybeAutoSyncGoogleSheets(user.id);
    res.json({ok:true,platform:"Klaviyo",refresh_job:{id:job.id,status:"completed"},snapshot_id:writeResult.snapshot?.id||null,snapshot_date:writeResult.snapshot?.snapshot_date||snapshotDate,platform_account_id:platformAccountId,row_counts:writeResult.row_counts,performance_spread_result:writeResult.performance_spread_result,google_sheets_sync:googleSheetsSync});
  }catch(e){job=e.refreshJob||job;res.status(e.status||500).json({ok:false,error:e.message,stage,job_id:job?.id||null})}
}



async function ensureConfiguredOrganicSchedules(){
  const {data:connections,error}=await supabaseAdmin
    .from("platform_connections")
    .select("user_id,account_id,metadata")
    .eq("platform","organic")
    .eq("connected",true);
  if(error)throw error;

  const results=[];
  for(const conn of connections||[]){
    if(!conn.metadata?.configured)continue;
    const property=conn.metadata.selectedGa4Property||{};
    const platformAccountId=normalizePlatformAccountId(conn.account_id||property.property_id||conn.metadata.selectedPlatformAccountId);
    if(!platformAccountId)continue;
    const ownership=await getOwnership("organic",platformAccountId);
    if(!ownership||ownership.owner_user_id!==conn.user_id||!activeOwnershipStatuses().includes(ownership.status))continue;
    const schedule=await ensureSnapshotSchedule(conn.user_id,"organic",platformAccountId,{
      organicAutomationVersion:"v1",
      ensuredBy:"auto_refresh_cron",
      account_type:phase1ReportableAccountType("organic")
    });
    results.push({user_id:conn.user_id,platform_account_id:platformAccountId,schedule_id:schedule?.id||null});
  }
  return {ok:true,count:results.length,results};
}

async function runOrganicAutoRefreshForSchedule(schedule){if(!ORGANIC_GA4_INGEST_ENABLED)return {ok:true,skipped:true,platform:"organic",reason:ORGANIC_GA4_PARK_REASON,schedule_id:schedule.id};
  const {data:user,error:userError}=await supabaseAdmin.from("users").select("*").eq("id",schedule.user_id).maybeSingle();
  if(userError)throw userError;
  if(!user)throw new Error("Auto refresh user not found");

  const conn=await getConnection(schedule.user_id,"organic");
  if(!conn)throw new Error("Auto refresh Organic connection not found");
  if(!conn.metadata?.configured)throw new Error("Organic GA4 property binding is required before automation");

  const property=conn.metadata.selectedGa4Property||{};
  const platformAccountId=normalizePlatformAccountId(schedule.platform_account_id||conn.account_id||property.property_id||conn.metadata.selectedPlatformAccountId);
  if(!platformAccountId)throw new Error("Auto refresh missing Organic GA4 property id");

  if(schedule.active===false){
    return {ok:true,skipped:true,platform:"organic",reason:"schedule_inactive",schedule_id:schedule.id,platform_account_id:platformAccountId};
  }

  const ownership=await getOwnership("organic",platformAccountId);
  if(!ownership||ownership.owner_user_id!==schedule.user_id||!activeOwnershipStatuses().includes(ownership.status)){
    return {ok:true,skipped:true,platform:"organic",reason:"ownership_not_active",schedule_id:schedule.id,platform_account_id:platformAccountId,ownership_status:ownership?.status||null};
  }

  const platformTimeZone=await getPlatformAccountTimezone(schedule.user_id,"organic",platformAccountId,conn,ownership);
  const policy=resolveAutoRefreshPolicy({date:new Date(),platformTimeZone,platform:"organic"});
  if(!policy.isAutomationHour){
    return {
      ok:true,
      skipped:true,
      platform:"organic",
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

  const snapshotDate=e2aSnapshotDate(null,platformTimeZone);
  const execution=await automationSnapshotOrchestrator.run({
    userId:schedule.user_id,
    platform:"organic",
    platformAccountId,
    snapshotDate,
    scheduleId:schedule.id,
    policy,
    primaryMetadata:{platformBusinessHour:policy.platform_business_hour,dataMaturityWindowHours:policy.data_maturity_window_hours,server_time_utc:policy.server_time_utc,istanbul_time:policy.istanbul_time,platform_account_time:policy.platform_account_time,platform_account_timezone:policy.platform_account_timezone,platform_business_date:policy.platform_business_date,timeEngineVersion:TIME_ENGINE_VERSION},
    recoveryMetadata:{timeEngineVersion:TIME_ENGINE_VERSION},
    write:jobContext=>writeOrganicSnapshotV1({user,...jobContext})
  });

  await supabaseAdmin.from("snapshot_schedules").update({
    last_run_at:new Date().toISOString(),
    next_run_at:nextAutomationSlotUtc(),
    updated_at:new Date().toISOString()
  }).eq("id",schedule.id);

  return {
    ok:true,
    platform:"organic",
    job_id:execution.job.id,
    snapshot_id:execution.result.snapshot?.id||null,
    row_counts:execution.result.row_counts,
    performance_spread_result:execution.result.performance_spread_result||null,
    recovery_result:execution.recoveryResult
  };
}

async function runAutomationSnapshotForSchedule({schedule,platform,missingConnectionError,missingAccountError,writeSnapshot}){
  const {data:user,error:userError}=await supabaseAdmin.from("users").select("*").eq("id",schedule.user_id).maybeSingle();
  if(userError)throw userError;if(!user)throw new Error("Auto refresh user not found");
  const conn=await getConnection(schedule.user_id,platform);if(!conn)throw new Error(missingConnectionError);
  const platformAccountId=normalizePlatformAccountId(schedule.platform_account_id||conn.account_id);if(!platformAccountId)throw new Error(missingAccountError);
  const platformTimeZone=await getPlatformAccountTimezone(schedule.user_id,platform,platformAccountId,conn,null);
  const policy=resolveAutoRefreshPolicy({date:new Date(),platformTimeZone,platform});
  if(!policy.isAutomationHour)return {ok:true,skipped:true,platform,reason:"not_platform_automation_hour",schedule_id:schedule.id};
  const snapshotDate=e2aSnapshotDate(null,platformTimeZone);
  const execution=await automationSnapshotOrchestrator.run({userId:schedule.user_id,platform,platformAccountId,snapshotDate,scheduleId:schedule.id,policy,write:jobContext=>writeSnapshot({user,conn,platformAccountId,...jobContext})});
  await supabaseAdmin.from("snapshot_schedules").update({last_run_at:new Date().toISOString(),next_run_at:nextAutomationSlotUtc(),updated_at:new Date().toISOString()}).eq("id",schedule.id);
  return {ok:true,platform,job_id:execution.job.id,snapshot_id:execution.result.snapshot?.id||null,row_counts:execution.result.row_counts,recovery_result:execution.recoveryResult};
}

function runTikTokAutoRefreshForSchedule(schedule){
  return runAutomationSnapshotForSchedule({schedule,platform:"tiktok",missingConnectionError:"Auto refresh TikTok connection not found",missingAccountError:"Auto refresh missing TikTok advertiser id",writeSnapshot:writeTikTokSnapshotImmutable});
}

function runKlaviyoAutoRefreshForSchedule(schedule){
  return runAutomationSnapshotForSchedule({schedule,platform:"klaviyo",missingConnectionError:"Auto refresh Klaviyo connection not found",missingAccountError:"Auto refresh missing Klaviyo account id",writeSnapshot:writeKlaviyoSnapshotImmutable});
}

// ===== END TIKTOK READ LAYER =====

installErrorBoundary(app);

if(process.env.VERCEL!=="1")startApplication(app,{port:runtimeConfig.port});
module.exports=app;
