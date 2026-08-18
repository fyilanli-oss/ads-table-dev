"use strict";

class ProductionConfigError extends Error{
  constructor(message,variables=[]){
    super(message);
    this.name="ProductionConfigError";
    this.code="UNSAFE_PRODUCTION_CONFIG";
    this.variables=Object.freeze([...variables]);
  }
}

function parseExplicitBoolean(value,defaultValue=false,name="configuration flag"){
  if(value===undefined||value===null||String(value).trim()==="")return defaultValue;
  const normalized=String(value).trim().toLowerCase();
  if(normalized==="true"||normalized==="1")return true;
  if(normalized==="false"||normalized==="0")return false;
  throw new ProductionConfigError(`Invalid boolean configuration: ${name}`,[name]);
}

function isProductionRuntime(env={}){
  const vercelEnv=String(env.VERCEL_ENV||"").trim().toLowerCase();
  if(vercelEnv)return vercelEnv==="production";
  return String(env.NODE_ENV||"").trim().toLowerCase()==="production";
}

function isPresent(value){return value!==undefined&&value!==null&&String(value).trim()!=="";}

function createRuntimeFlags(env={}){
  const production=isProductionRuntime(env);
  const googleReviewHardRouteEnabled=parseExplicitBoolean(env.GOOGLE_REVIEW_HARD_ROUTE_ENABLED,false,"GOOGLE_REVIEW_HARD_ROUTE_ENABLED");
  const tiktokReviewFallbackEnabled=parseExplicitBoolean(env.TIKTOK_REVIEW_FALLBACK_ENABLED,false,"TIKTOK_REVIEW_FALLBACK_ENABLED");
  const tiktokSandboxEnabled=parseExplicitBoolean(env.TIKTOK_SANDBOX_ENABLED,false,"TIKTOK_SANDBOX_ENABLED");
  const tiktokForceSandboxReports=parseExplicitBoolean(env.TIKTOK_FORCE_SANDBOX_REPORTS,false,"TIKTOK_FORCE_SANDBOX_REPORTS");
  return Object.freeze({
    production,
    googleReviewHardRouteEnabled,
    tiktokReviewFallbackEnabled,
    tiktokSandboxEnabled,
    tiktokForceSandboxReports:tiktokSandboxEnabled&&tiktokForceSandboxReports,
    tiktokTestPageEnabled:!production&&tiktokSandboxEnabled
  });
}

function validateProductionConfig(env={}){
  const flags=createRuntimeFlags(env);
  const unsafe=[];
  if(flags.production){
    if(flags.googleReviewHardRouteEnabled)unsafe.push("GOOGLE_REVIEW_HARD_ROUTE_ENABLED");
    if(isPresent(env.GOOGLE_TEST_CUSTOMER_ID))unsafe.push("GOOGLE_TEST_CUSTOMER_ID");
    if(isPresent(env.GOOGLE_TEST_LOGIN_CUSTOMER_ID))unsafe.push("GOOGLE_TEST_LOGIN_CUSTOMER_ID");
    if(flags.tiktokReviewFallbackEnabled)unsafe.push("TIKTOK_REVIEW_FALLBACK_ENABLED");
    if(isPresent(env.TIKTOK_REVIEW_ADVERTISER_ID))unsafe.push("TIKTOK_REVIEW_ADVERTISER_ID");
    if(isPresent(env.TIKTOK_REVIEW_ADVERTISER_NAME))unsafe.push("TIKTOK_REVIEW_ADVERTISER_NAME");
    if(flags.tiktokSandboxEnabled)unsafe.push("TIKTOK_SANDBOX_ENABLED");
    if(isPresent(env.TIKTOK_SANDBOX_ACCESS_TOKEN))unsafe.push("TIKTOK_SANDBOX_ACCESS_TOKEN");
    if(isPresent(env.TIKTOK_TEST_ACCESS_TOKEN))unsafe.push("TIKTOK_TEST_ACCESS_TOKEN");
    if(parseExplicitBoolean(env.TIKTOK_FORCE_SANDBOX_REPORTS,false,"TIKTOK_FORCE_SANDBOX_REPORTS"))unsafe.push("TIKTOK_FORCE_SANDBOX_REPORTS");
  }else{
    if(flags.googleReviewHardRouteEnabled){
      if(!isPresent(env.GOOGLE_TEST_CUSTOMER_ID))unsafe.push("GOOGLE_TEST_CUSTOMER_ID");
      if(!isPresent(env.GOOGLE_TEST_LOGIN_CUSTOMER_ID))unsafe.push("GOOGLE_TEST_LOGIN_CUSTOMER_ID");
      if(isPresent(env.GOOGLE_TEST_CUSTOMER_ID)&&String(env.GOOGLE_TEST_CUSTOMER_ID).replace(/\D/g,"")===String(env.GOOGLE_TEST_LOGIN_CUSTOMER_ID).replace(/\D/g,""))unsafe.push("GOOGLE_TEST_CUSTOMER_ID","GOOGLE_TEST_LOGIN_CUSTOMER_ID");
    }
    if(flags.tiktokReviewFallbackEnabled&&!isPresent(env.TIKTOK_REVIEW_ADVERTISER_ID))unsafe.push("TIKTOK_REVIEW_ADVERTISER_ID");
    if(parseExplicitBoolean(env.TIKTOK_FORCE_SANDBOX_REPORTS,false,"TIKTOK_FORCE_SANDBOX_REPORTS")&&!flags.tiktokSandboxEnabled)unsafe.push("TIKTOK_FORCE_SANDBOX_REPORTS","TIKTOK_SANDBOX_ENABLED");
  }
  const variables=[...new Set(unsafe)].sort();
  if(variables.length)throw new ProductionConfigError(`Unsafe production configuration: ${variables.join(", ")}`,variables);
  return flags;
}

function loadProductionConfig(env=process.env){return validateProductionConfig(env);}

module.exports={ProductionConfigError,parseExplicitBoolean,isProductionRuntime,createRuntimeFlags,validateProductionConfig,loadProductionConfig};
