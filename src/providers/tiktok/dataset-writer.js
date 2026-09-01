'use strict';

const {validateCanonicalRow}=require('../../../funnel-core/canonical-contract');
const {buildEntityKey,validateEntityHierarchy}=require('../../../funnel-core/entity-hierarchy');
const {mapTikTokProductionRowsWithTimeFx}=require('./time-fx-normalization');

function required(value,field){if(typeof value!=='string'||!value.trim())throw new Error(`${field} is required`);return value.trim();}
function mark(error,stage){if(error&&typeof error==='object'&&Object.isExtensible(error)&&!error.safe_stage)Object.defineProperty(error,'safe_stage',{value:stage,enumerable:false});return error;}

function createTikTokDatasetWriter({writeBoundary,resolveFxRate}={}){
  if(!writeBoundary||typeof writeBoundary.write!=='function')throw new TypeError('canonical write boundary is required');
  if(typeof resolveFxRate!=='function')throw new TypeError('FX resolver is required');
  return Object.freeze({async ingest(input={}){
    const context=input.context||{},userId=required(context.userId,'context.userId'),advertiserId=required(input.advertiserId,'advertiserId'),advertiser=context.advertiser,targetCurrency=required(context.targetCurrency,'context.targetCurrency');
    if(!advertiser||required(advertiser.id,'context.advertiser.id')!==advertiserId)throw new Error('TikTok write advertiser ownership mismatch');
    const providerDate=required(context.providerDate,'context.providerDate');let fx;
    try{fx=await resolveFxRate(required(advertiser.currency,'context.advertiser.currency'),targetCurrency,{rateDate:providerDate});}catch(error){throw mark(error,'TIKTOK_FX_LOOKUP');}
    let normalized;
    try{normalized=mapTikTokProductionRowsWithTimeFx(input.rows,{...context,advertiserId,providerDate,targetCurrency,fxRate:fx.fx_rate,fxRateDate:fx.fx_rate_date||providerDate,fxProvider:fx.fx_provider});}catch(error){throw mark(error,'TIKTOK_DELIVERY_ADAPTER');}
    const rows=normalized.mapped.map(result=>{const row=result.row;validateCanonicalRow(row);validateEntityHierarchy(row.identity,row.entity);if(row.identity.user_id!==userId||row.identity.platform_account_id!==advertiserId||row.identity.platform!=='tiktok')throw new Error('TikTok canonical row ownership mismatch');if(result.entityKey!==buildEntityKey(row.identity,row.entity))throw new Error('TikTok canonical entity key mismatch');if(row.provenance.synthetic!==false)throw new Error('Synthetic TikTok row reached Dataset V2 writer');return row;});
    let persisted;try{persisted=await writeBoundary.write(rows);}catch(error){throw mark(error,'TIKTOK_DATASET_V2_WRITE');}
    if(!Array.isArray(persisted)||persisted.length!==rows.length)throw new Error('TikTok Dataset V2 write result cardinality mismatch');
    return Object.freeze({attempted:rows.length,persisted:persisted.length,empty_provider_result:input.rows.length===0,isolated_synthetic_rows:normalized.isolation.isolated_synthetic_rows,synthetic_written_to_canonical:0,rows:persisted});
  }});
}

module.exports=Object.freeze({createTikTokDatasetWriter});
