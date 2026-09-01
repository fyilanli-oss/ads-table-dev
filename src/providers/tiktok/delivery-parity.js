'use strict';

const {validateCanonicalRow}=require('../../../funnel-core/canonical-contract');
const {validateEntityHierarchy}=require('../../../funnel-core/entity-hierarchy');
const {isSyntheticTikTokFallback}=require('./synthetic-isolation');

const DELIVERY_TOLERANCE=1e-9;
function number(value,field){const parsed=Number(value);if(!Number.isFinite(parsed)||parsed<0)throw new Error(`${field} must be a non-negative number`);return parsed;}
function legacyId(row){const value=row?.ad_id??row?.id_in_platform??row?.id;if(typeof value!=='string'&&typeof value!=='number')throw new Error('Legacy TikTok Ad id is required');const id=String(value).trim();if(!id)throw new Error('Legacy TikTok Ad id is required');return id;}
function close(left,right){return Math.abs(left-right)<=DELIVERY_TOLERANCE;}
function delivery(value){return Object.freeze({impressions:number(value.impressions,'impressions'),clicks:number(value.clicks??value.ad_clicks,'clicks'),spend:number(value.spend,'spend')});}
function eventPolicy(row){return ['add_to_cart','add_to_cart_value','checkout','checkout_value','purchase','purchase_value'].every(field=>row.raw_metrics[field]===null&&row.metric_support[field]==='unsupported');}

function evaluateTikTokDeliveryParity({legacyRows,v2Rows}={}){
  if(!Array.isArray(legacyRows)||!Array.isArray(v2Rows))throw new TypeError('legacyRows and v2Rows must be arrays');
  const legacy=new Map();let isolated=0;
  for(const row of legacyRows){if(isSyntheticTikTokFallback(row)){isolated+=1;continue;}if(String(row.level??'').toLowerCase()!=='ad')continue;const id=legacyId(row);if(legacy.has(id))throw new Error('Duplicate legacy TikTok Ad row');legacy.set(id,delivery(row));}
  const canonical=new Map();let eventsOk=true,syntheticOk=true;
  for(const row of v2Rows){validateCanonicalRow(row);validateEntityHierarchy(row.identity,row.entity);if(row.identity.platform!=='tiktok'||row.entity.entity_type!=='ad')throw new Error('Parity accepts only canonical TikTok Ad rows');const id=row.entity.entity_id;if(canonical.has(id))throw new Error('Duplicate canonical TikTok Ad row');syntheticOk=syntheticOk&&row.provenance.synthetic===false;eventsOk=eventsOk&&eventPolicy(row);canonical.set(id,Object.freeze({impressions:number(row.raw_metrics.impression,'raw_metrics.impression'),clicks:number(row.raw_metrics.ad_click,'raw_metrics.ad_click'),spend:number(row.raw_metrics.spend_value,'raw_metrics.spend_value')}));}
  const entitySetMatch=legacy.size===canonical.size&&[...legacy.keys()].every(id=>canonical.has(id));
  const deliveryFactsMatch=entitySetMatch&&[...legacy].every(([id,left])=>{const right=canonical.get(id);return close(left.impressions,right.impressions)&&close(left.clicks,right.clicks)&&close(left.spend,right.spend);});
  const passed=entitySetMatch&&deliveryFactsMatch&&eventsOk&&syntheticOk;
  return Object.freeze({evidence_version:'e6-tiktok-delivery-parity-v1',status:passed?'PASS':'FAIL',legacy_ad_rows:legacy.size,v2_ad_rows:canonical.size,isolated_legacy_synthetic_rows:isolated,entity_set_match:entitySetMatch,delivery_facts_match:deliveryFactsMatch,event_policy_match:eventsOk,synthetic_policy_match:syntheticOk,synthetic_written_to_canonical:syntheticOk?0:1});
}

function assertTikTokDeliveryParity(input){const evidence=evaluateTikTokDeliveryParity(input);if(evidence.status!=='PASS'){const error=new Error('TikTok delivery parity failed');Object.defineProperty(error,'parity_evidence',{value:evidence,enumerable:false});throw error;}return evidence;}

module.exports=Object.freeze({DELIVERY_TOLERANCE,assertTikTokDeliveryParity,evaluateTikTokDeliveryParity});
