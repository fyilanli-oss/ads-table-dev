"use strict";

const INCLUDED_PLATFORMS=Object.freeze(["meta","google","tiktok","klaviyo"]);
const PARKED_PLATFORMS=Object.freeze(["pinterest","organic"]);
const ACCOUNT_LIMIT=3;
const DATE_WINDOWS=Object.freeze([
  Object.freeze({date_key:"yesterday",finality:"finalized",priority:1}),
  Object.freeze({date_key:"today",finality:"provisional",priority:2})
]);

function clean(value){return typeof value==="string"?value.trim():"";}
function createOnboardingBackfillPlan({accounts=[]}={}){
  if(!Array.isArray(accounts))throw new TypeError("accounts must be an array");
  const units=[],excluded=[],seen=new Set(),counts=new Map();
  for(const account of accounts){
    const platform=clean(account?.platform).toLowerCase(),accountId=clean(account?.platform_account_id||account?.account_id);
    if(PARKED_PLATFORMS.includes(platform)){excluded.push(Object.freeze({platform,platform_account_id:accountId||null,reason:"parked"}));continue}
    if(!INCLUDED_PLATFORMS.includes(platform)){excluded.push(Object.freeze({platform:platform||null,platform_account_id:accountId||null,reason:"unsupported"}));continue}
    if(!accountId||account?.ownership_status!=="active"||account?.connected!==true){excluded.push(Object.freeze({platform,platform_account_id:accountId||null,reason:"inactive_or_unowned"}));continue}
    const identity=`${platform}:${accountId}`;
    if(seen.has(identity))continue;
    const count=counts.get(platform)||0;
    if(count>=ACCOUNT_LIMIT){excluded.push(Object.freeze({platform,platform_account_id:accountId,reason:"account_limit"}));continue}
    seen.add(identity);counts.set(platform,count+1);
    for(const window of DATE_WINDOWS)units.push(Object.freeze({platform,platform_account_id:accountId,...window}));
  }
  return Object.freeze({contract_version:"e9-t1-v1",account_limit:ACCOUNT_LIMIT,included_platforms:INCLUDED_PLATFORMS,parked_platforms:PARKED_PLATFORMS,date_windows:DATE_WINDOWS,older_history_automatic:false,day_14_behavior:"continue_daily_accumulation",units:Object.freeze(units),excluded:Object.freeze(excluded)});
}

module.exports=Object.freeze({INCLUDED_PLATFORMS,PARKED_PLATFORMS,ACCOUNT_LIMIT,DATE_WINDOWS,createOnboardingBackfillPlan});
