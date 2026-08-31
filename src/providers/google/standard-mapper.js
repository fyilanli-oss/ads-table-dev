'use strict';

const { validateCanonicalRow } = require('../../../funnel-core/canonical-contract');
const { buildEntityKey, validateEntityHierarchy } = require('../../../funnel-core/entity-hierarchy');
const { mapGoogleConversions } = require('./conversion-mapping');

const GOOGLE_STANDARD_ADAPTER_VERSION = 'google-standard-v1';
function text(value, field) { if (typeof value !== 'string' && typeof value !== 'number') throw new Error(`${field} is required`); const normalized=String(value).trim(); if(!normalized)throw new Error(`${field} is required`); return normalized; }
function metric(value, field) { if(value===null||value===undefined||value==='')return null;const n=Number(value);if(!Number.isFinite(n)||n<0)throw new Error(`${field} must be a non-negative number`);return n; }
function support(value){return value===null?'unknown':'supported';}

function mapGoogleStandardAd(input,{userId,customer,sourceJobId=null}={}){
  if(!input||typeof input!=='object'||Array.isArray(input))throw new TypeError('Google Standard row is required');
  if(!customer||typeof customer!=='object')throw new TypeError('normalized Google customer metadata is required');
  const campaign=input.campaign||{},adGroup=input.adGroup||input.ad_group||{},ad=(input.adGroupAd||input.ad_group_ad||{}).ad||{},metrics=input.metrics||{},segments=input.segments||{};
  const channel=String(campaign.advertisingChannelType??campaign.advertising_channel_type??'').toUpperCase();
  if(channel==='PERFORMANCE_MAX')throw new Error('Performance Max cannot use the Google Standard adapter');
  const date=text(segments.date,'segments.date');if(date!==customer.business_date)throw new Error('Google Standard row business date mismatch');
  const conversions=mapGoogleConversions(input.conversion_actions||input.__conversion_actions||[]);
  const impressions=metric(metrics.impressions,'metrics.impressions'),clicks=metric(metrics.clicks,'metrics.clicks'),costMicros=metric(metrics.costMicros??metrics.cost_micros,'metrics.cost_micros');
  const raw_metrics={impression:impressions,ad_click:clicks,session:null,spend_value:costMicros===null?null:costMicros/1000000,add_to_cart:conversions.add_to_cart.count,add_to_cart_value:conversions.add_to_cart.value,checkout:conversions.checkout.count,checkout_value:conversions.checkout.value,purchase:conversions.purchase.count,purchase_value:conversions.purchase.value};
  const metric_support=Object.fromEntries(Object.entries(raw_metrics).map(([key,value])=>[key,key==='session'?'unsupported':support(value)]));
  const identity={user_id:text(userId,'userId'),platform:'google',traffic_type:'paid',source_system:'google_ads',channel:null,platform_account_id:text(customer.id,'customer.id'),date};
  const entity={campaign_type:'standard',root_entity_type:'campaign',root_entity_id:text(campaign.id,'campaign.id'),root_entity_name:text(campaign.name,'campaign.name'),parent_entity_type:'adgroup',parent_entity_id:text(adGroup.id,'ad_group.id'),parent_entity_name:text(adGroup.name,'ad_group.name'),entity_type:'ad',entity_id:text(ad.id,'ad.id'),entity_name:text(ad.name,'ad.name')};
  const confidence=Object.values(metric_support).includes('unknown')?'partial':Object.values(conversions).some(item=>item.provenance.fallback_used)?'fallback':'real';
  const row={identity,entity,raw_metrics,metric_support,currency:{source_currency:customer.source_currency,target_currency:customer.source_currency,fx_rate:1,fx_rate_date:date,fx_provider:'same_currency',fx_engine_version:'v1'},time:{source_timezone:customer.source_timezone,business_date:date,time_engine_version:customer.time_engine_version},provenance:{source_system:'google_ads',adapter_version:GOOGLE_STANDARD_ADAPTER_VERSION,source_confidence:confidence,synthetic:false,ga4_property_id:null,source_job_id:sourceJobId,raw_reference:{conversion_mapping_version:conversions.purchase.provenance.mapping_version,conversion_sources:Object.fromEntries(Object.entries(conversions).map(([key,value])=>[key,value.provenance]))}}};
  validateCanonicalRow(row);validateEntityHierarchy(identity,entity);return Object.freeze({row:Object.freeze(row),entityKey:buildEntityKey(identity,entity)});
}
module.exports=Object.freeze({GOOGLE_STANDARD_ADAPTER_VERSION,mapGoogleStandardAd});
