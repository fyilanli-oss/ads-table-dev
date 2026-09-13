'use strict';

const {evaluateTikTokDeliveryParity}=require('./delivery-parity');

const SAFE_SHADOW_STAGES=Object.freeze(['TIKTOK_ADVERTISER_METADATA','TIKTOK_PROVIDER_REPORT','TIKTOK_TARGET_CURRENCY','TIKTOK_FX_LOOKUP','TIKTOK_DELIVERY_ADAPTER','TIKTOK_DATASET_V2_WRITE','UNCLASSIFIED']);
function required(value,field){if(typeof value!=='string'||!value.trim())throw new Error(`${field} is required`);return value.trim();}
function safeStage(error){return SAFE_SHADOW_STAGES.includes(error?.safe_stage)?error.safe_stage:'UNCLASSIFIED';}
function unchanged(value,before,field){if(JSON.stringify(value)!==before)throw new Error(`${field} was mutated during shadow dual-write`);}

function createTikTokShadowDualWrite({legacyWrite,v2Run,loadLegacyRows,loadV2Rows,evaluateParity=evaluateTikTokDeliveryParity}={}){
  for(const [name,fn] of Object.entries({legacyWrite,v2Run,loadLegacyRows,loadV2Rows,evaluateParity}))if(typeof fn!=='function')throw new TypeError(`${name} is required`);
  return Object.freeze({async run(request={}){
    const userId=required(request.userId,'userId'),advertiserId=required(request.advertiserId,'advertiserId'),providerDate=required(request.providerDate,'providerDate'),before=JSON.stringify(request);
    let legacyResult;try{legacyResult=await legacyWrite(structuredClone(request));}catch(error){if(error&&typeof error==='object'&&Object.isExtensible(error)&&!error.safe_stage)Object.defineProperty(error,'safe_stage',{value:'TIKTOK_LEGACY_WRITE',enumerable:false});throw error;}
    unchanged(request,before,'request');
    let v2Execution;try{v2Execution=await v2Run({userId,advertiserId,providerDate});}catch(error){return Object.freeze({legacy_result:legacyResult,shadow_evidence:Object.freeze({evidence_version:'e6-tiktok-shadow-v1',status:'FAIL',legacy_completed:true,v2_completed:false,parity_evaluated:false,parity_passed:false,failure_stage:safeStage(error),production_activation:false})});}
    const legacyRows=await loadLegacyRows({userId,advertiserId,providerDate,legacyResult}),v2Rows=await loadV2Rows({userId,advertiserId,providerDate,v2Execution});
    const parity=evaluateParity({legacyRows,v2Rows}),passed=parity?.status==='PASS';
    unchanged(request,before,'request');
    return Object.freeze({legacy_result:legacyResult,shadow_evidence:Object.freeze({evidence_version:'e6-tiktok-shadow-v1',status:passed?'PASS':'FAIL',legacy_completed:true,v2_completed:true,parity_evaluated:true,parity_passed:passed,failure_stage:null,production_activation:false,parity})});
  }});
}

module.exports=Object.freeze({SAFE_SHADOW_STAGES,createTikTokShadowDualWrite,safeStage});
