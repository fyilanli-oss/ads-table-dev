'use strict';

const {validateCanonicalRow}=require('../../../funnel-core/canonical-contract');
const {buildEntityKey,validateEntityHierarchy}=require('../../../funnel-core/entity-hierarchy');
const {normalizeCurrencyCode,normalizeMonetaryRawFields}=require('../../../funnel-core/fx-service');
const {normalizeBusinessDate}=require('../../../funnel-core/time-service');
const {mapKlaviyoMessage}=require('./mapper');

const SAFE_STAGES=Object.freeze(['KLAVIYO_FX_LOOKUP','KLAVIYO_ADAPTER','KLAVIYO_DATASET_V2_WRITE','UNCLASSIFIED']);
function required(value,field){if(typeof value!=='string'||!value.trim())throw new Error(`${field} is required`);return value.trim();}
function mark(error,stage){if(error&&typeof error==='object'&&Object.isExtensible(error)&&!error.safe_stage)Object.defineProperty(error,'safe_stage',{value:stage,enumerable:false});return error;}
function safeStage(error){return SAFE_STAGES.includes(error?.safe_stage)?error.safe_stage:'UNCLASSIFIED';}

function normalizeKlaviyoTimeFxMessage(input,context={}){
  const account=context.account;if(!account||typeof account!=='object'||Array.isArray(account))throw new TypeError('context.account is required');
  const accountId=required(account.id,'context.account.id');if(accountId!==required(context.accountId,'context.accountId'))throw new Error('Klaviyo account identity mismatch');
  const sourceCurrency=normalizeCurrencyCode(account.currency,'context.account.currency'),sourceTimezone=required(account.timezone,'context.account.timezone');
  const time=normalizeBusinessDate({providerDate:context.providerDate,sourceTimezone});
  const mapped=mapKlaviyoMessage(input,{...context,accountId,businessDate:time.business_date,sourceCurrency,targetCurrency:sourceCurrency,sourceTimezone,timeEngineVersion:time.time_engine_version});
  const targetCurrency=normalizeCurrencyCode(context.targetCurrency??sourceCurrency,'context.targetCurrency'),crossCurrency=targetCurrency!==sourceCurrency;
  const row=normalizeMonetaryRawFields(mapped.row,{sourceCurrency,targetCurrency,fxRate:context.fxRate??null,fxRateDate:context.fxRateDate??time.business_date,fxProvider:crossCurrency?required(context.fxProvider,'context.fxProvider'):(context.fxProvider??'same_currency')});
  row.identity.date=time.business_date;row.time=time;validateCanonicalRow(row);validateEntityHierarchy(row.identity,row.entity);
  return Object.freeze({row:Object.freeze(row),entityKey:buildEntityKey(row.identity,row.entity)});
}

function createKlaviyoDatasetWriter({writeBoundary,resolveFxRate}={}){
  if(!writeBoundary||typeof writeBoundary.write!=='function')throw new TypeError('canonical write boundary is required');if(typeof resolveFxRate!=='function')throw new TypeError('FX resolver is required');
  return Object.freeze({async ingest(input={}){
    const context=input.context||{},rows=input.rows;if(!Array.isArray(rows))throw new TypeError('Klaviyo provider rows must be an array');
    const userId=required(context.userId,'context.userId'),accountId=required(input.accountId,'accountId'),account=context.account,targetCurrency=required(context.targetCurrency,'context.targetCurrency'),providerDate=required(context.providerDate,'context.providerDate');
    if(!account||required(account.id,'context.account.id')!==accountId)throw new Error('Klaviyo write account ownership mismatch');
    let fx;try{fx=await resolveFxRate(required(account.currency,'context.account.currency'),targetCurrency,{rateDate:providerDate});}catch(error){throw mark(error,'KLAVIYO_FX_LOOKUP');}
    let mapped;try{mapped=rows.map(row=>normalizeKlaviyoTimeFxMessage(row,{...context,accountId,providerDate,targetCurrency,fxRate:fx.fx_rate,fxRateDate:fx.fx_rate_date||providerDate,fxProvider:fx.fx_provider}));}catch(error){throw mark(error,'KLAVIYO_ADAPTER');}
    const keys=new Set(),canonical=mapped.map(result=>{const row=result.row,key=`${row.identity.date}:${result.entityKey}`;if(keys.has(key))throw mark(new Error('Duplicate normalized Klaviyo message would double-count facts'),'KLAVIYO_ADAPTER');keys.add(key);if(row.identity.user_id!==userId||row.identity.platform_account_id!==accountId||row.identity.platform!=='klaviyo')throw mark(new Error('Klaviyo canonical row ownership mismatch'),'KLAVIYO_ADAPTER');return row;});
    let persisted;try{persisted=await writeBoundary.write(canonical);}catch(error){throw mark(error,'KLAVIYO_DATASET_V2_WRITE');}if(!Array.isArray(persisted)||persisted.length!==canonical.length)throw mark(new Error('Klaviyo Dataset V2 write result cardinality mismatch'),'KLAVIYO_DATASET_V2_WRITE');
    return Object.freeze({attempted:canonical.length,persisted:persisted.length,empty_provider_result:rows.length===0,rows:persisted});
  }});
}

function evaluateKlaviyoParity({legacyRows,v2Rows}={}){
  if(!Array.isArray(legacyRows)||!Array.isArray(v2Rows))throw new TypeError('legacyRows and v2Rows must be arrays');
  const index=rows=>{const map=new Map();for(const row of rows){validateCanonicalRow(row);validateEntityHierarchy(row.identity,row.entity);if(row.identity.platform!=='klaviyo')throw new Error('Klaviyo parity accepts only Klaviyo rows');const key=buildEntityKey(row.identity,row.entity);if(map.has(key))throw new Error('Duplicate Klaviyo parity row');map.set(key,row);}return map;},legacy=index(legacyRows),v2=index(v2Rows);
  const entitySetMatch=legacy.size===v2.size&&[...legacy.keys()].every(key=>v2.has(key));
  const factsMatch=entitySetMatch&&[...legacy].every(([key,left])=>{const right=v2.get(key);return JSON.stringify(left.raw_metrics)===JSON.stringify(right.raw_metrics)&&JSON.stringify(left.metric_support)===JSON.stringify(right.metric_support);});
  const branchChannelMatch=entitySetMatch&&[...legacy].every(([key,left])=>{const right=v2.get(key);return left.identity.channel===right.identity.channel&&left.entity.root_entity_type===right.entity.root_entity_type&&left.entity.entity_type===right.entity.entity_type;});
  const normalizationMatch=entitySetMatch&&[...legacy].every(([key,left])=>{const right=v2.get(key);return left.identity.date===right.identity.date&&JSON.stringify(left.currency)===JSON.stringify(right.currency)&&JSON.stringify(left.time)===JSON.stringify(right.time);});
  const nonEmptyEvidence=legacy.size>0&&v2.size>0,passed=nonEmptyEvidence&&entitySetMatch&&factsMatch&&branchChannelMatch&&normalizationMatch;
  return Object.freeze({evidence_version:'e7-klaviyo-parity-v1',status:passed?'PASS':'FAIL',legacy_rows:legacy.size,v2_rows:v2.size,non_empty_evidence:nonEmptyEvidence,entity_set_match:entitySetMatch,facts_match:factsMatch,branch_channel_match:branchChannelMatch,normalization_match:normalizationMatch});
}

function createKlaviyoShadowDualWrite({legacyWrite,v2Run,loadLegacyRows,loadV2Rows}={}){
  for(const[name,fn]of Object.entries({legacyWrite,v2Run,loadLegacyRows,loadV2Rows}))if(typeof fn!=='function')throw new TypeError(`${name} is required`);
  return Object.freeze({async run(request={}){const before=JSON.stringify(request);let legacyResult;try{legacyResult=await legacyWrite(structuredClone(request));}catch(error){throw mark(error,'KLAVIYO_LEGACY_WRITE');}if(JSON.stringify(request)!==before)throw new Error('Klaviyo shadow request was mutated');let v2Execution;try{v2Execution=await v2Run(structuredClone(request));}catch(error){return Object.freeze({legacy_result:legacyResult,shadow_evidence:Object.freeze({status:'FAIL',legacy_completed:true,v2_completed:false,parity_passed:false,failure_stage:safeStage(error),production_activation:false})});}const parity=evaluateKlaviyoParity({legacyRows:await loadLegacyRows({request,legacyResult}),v2Rows:await loadV2Rows({request,v2Execution})});return Object.freeze({legacy_result:legacyResult,shadow_evidence:Object.freeze({status:parity.status,legacy_completed:true,v2_completed:true,parity_passed:parity.status==='PASS',failure_stage:null,production_activation:false,parity})});}});
}

module.exports=Object.freeze({SAFE_STAGES,createKlaviyoDatasetWriter,createKlaviyoShadowDualWrite,evaluateKlaviyoParity,normalizeKlaviyoTimeFxMessage,safeStage});
