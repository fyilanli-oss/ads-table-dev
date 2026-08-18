"use strict";

function enabled(value){
  return String(value||"").trim().toLowerCase()==="true"||String(value||"").trim()==="1";
}

function createProductionConfig(env={}){
  const nodeEnv=String(env.NODE_ENV||"development").trim().toLowerCase();
  const production=nodeEnv==="production";
  const googleReviewHardRouteEnabled=enabled(env.GOOGLE_REVIEW_HARD_ROUTE_ENABLED);
  const tiktokReviewFallbackEnabled=enabled(env.TIKTOK_REVIEW_FALLBACK_ENABLED);
  const tiktokSandboxEnabled=enabled(env.TIKTOK_SANDBOX_ENABLED);

  return Object.freeze({
    nodeEnv,
    production,
    googleReviewHardRouteEnabled,
    tiktokReviewFallbackEnabled,
    tiktokSandboxEnabled,
    tiktokTestPageEnabled:!production&&tiktokSandboxEnabled
  });
}

function assertSafeProductionConfig(config){
  if(!config.production)return config;
  const unsafe=[];
  if(config.googleReviewHardRouteEnabled)unsafe.push("Google review hard-routing");
  if(config.tiktokReviewFallbackEnabled)unsafe.push("TikTok review fallback");
  if(config.tiktokSandboxEnabled)unsafe.push("TikTok sandbox mode");
  if(unsafe.length)throw new Error(`Unsafe production configuration: ${unsafe.join(", ")} must be disabled`);
  return config;
}

function loadProductionConfig(env=process.env){
  return assertSafeProductionConfig(createProductionConfig(env));
}

module.exports={createProductionConfig,assertSafeProductionConfig,loadProductionConfig};
