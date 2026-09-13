'use strict';

const {validateCanonicalRow}=require('../../../funnel-core/canonical-contract');
const {buildEntityKey,validateEntityHierarchy}=require('../../../funnel-core/entity-hierarchy');
const {TIKTOK_REPORT_CONTRACT}=require('./report-contract');

const TIKTOK_DELIVERY_ADAPTER_VERSION='tiktok-delivery-v1';
const DELIVERY_FIELDS=Object.freeze(['impressions','clicks','spend']);
const EVENT_METRICS=Object.freeze(['add_to_cart','add_to_cart_value','checkout','checkout_value','purchase','purchase_value']);

function object(value,field){if(!value||typeof value!=='object'||Array.isArray(value))throw new TypeError(`${field} is required`);return value;}
function text(value,field){if(typeof value!=='string'&&typeof value!=='number')throw new Error(`${field} is required`);const normalized=String(value).trim();if(!normalized)throw new Error(`${field} is required`);return normalized;}
function metric(value,field){if(value===null||value===undefined||value==='')return null;const parsed=Number(value);if(!Number.isFinite(parsed)||parsed<0)throw new Error(`${field} must be a non-negative number`);return parsed;}
function support(value){return value===null?'unknown':'supported';}

function mapTikTokDeliveryAd(input,context={}){
  object(input,'TikTok delivery row');object(context,'TikTok mapping context');
  if(input.synthetic===true||input.raw?.synthetic===true)throw new Error('Synthetic TikTok rows cannot enter the delivery adapter');
  const level=String(input.level??input.data_level??'AUCTION_AD').toUpperCase();
  if(level!=='AD'&&level!=='AUCTION_AD')throw new Error('TikTok production facts must be AUCTION_AD leaf rows');
  const dimensions=object(input.dimensions,'dimensions'),metrics=object(input.metrics,'metrics');
  const date=text(context.businessDate??dimensions.stat_time_day??input.date,'context.businessDate');
  const identity={user_id:text(context.userId,'context.userId'),platform:'tiktok',traffic_type:'paid',source_system:'tiktok_ads',channel:null,platform_account_id:text(context.advertiserId,'context.advertiserId'),date};
  const entity={campaign_type:null,root_entity_type:'campaign',root_entity_id:text(dimensions.campaign_id,'dimensions.campaign_id'),root_entity_name:text(dimensions.campaign_name,'dimensions.campaign_name'),parent_entity_type:'adgroup',parent_entity_id:text(dimensions.adgroup_id??dimensions.ad_group_id,'dimensions.adgroup_id'),parent_entity_name:text(dimensions.adgroup_name??dimensions.ad_group_name,'dimensions.adgroup_name'),entity_type:'ad',entity_id:text(dimensions.ad_id,'dimensions.ad_id'),entity_name:text(dimensions.ad_name,'dimensions.ad_name')};
  const impression=metric(metrics.impressions,'metrics.impressions'),adClick=metric(metrics.clicks,'metrics.clicks'),spend=metric(metrics.spend,'metrics.spend');
  const raw_metrics={impression,ad_click:adClick,session:null,spend_value:spend,add_to_cart:null,add_to_cart_value:null,checkout:null,checkout_value:null,purchase:null,purchase_value:null};
  const metric_support={impression:support(impression),ad_click:support(adClick),session:'unsupported',spend_value:support(spend),add_to_cart:'unsupported',add_to_cart_value:'unsupported',checkout:'unsupported',checkout_value:'unsupported',purchase:'unsupported',purchase_value:'unsupported'};
  const sourceCurrency=text(context.sourceCurrency,'context.sourceCurrency'),targetCurrency=text(context.targetCurrency??sourceCurrency,'context.targetCurrency');
  if(sourceCurrency!==targetCurrency)throw new Error('Cross-currency TikTok mapping requires E6-T6 FX binding');
  const row={identity,entity,raw_metrics,metric_support,currency:{source_currency:sourceCurrency,target_currency:targetCurrency,fx_rate:1,fx_rate_date:date,fx_provider:'same_currency',fx_engine_version:text(context.fxEngineVersion??'v1','context.fxEngineVersion')},time:{source_timezone:text(context.sourceTimezone,'context.sourceTimezone'),business_date:date,time_engine_version:text(context.timeEngineVersion??'v1','context.timeEngineVersion')},provenance:{source_system:'tiktok_ads',adapter_version:TIKTOK_DELIVERY_ADAPTER_VERSION,source_confidence:[impression,adClick,spend].includes(null)?'partial':'real',synthetic:false,ga4_property_id:null,source_job_id:context.sourceJobId??null,raw_reference:{contract_version:TIKTOK_REPORT_CONTRACT.version,data_level:'AUCTION_AD',delivery_fields:DELIVERY_FIELDS,event_fields_ignored:EVENT_METRICS}}};
  validateCanonicalRow(row);validateEntityHierarchy(identity,entity);
  return Object.freeze({row:Object.freeze(row),entityKey:buildEntityKey(identity,entity)});
}

function mapTikTokDeliveryRows(inputs,context={}){
  if(!Array.isArray(inputs))throw new TypeError('TikTok delivery rows must be an array');
  const mapped=inputs.map(input=>mapTikTokDeliveryAd(input,context)),keys=new Set();
  for(const item of mapped){const dateKey=`${item.row.identity.date}:${item.entityKey}`;if(keys.has(dateKey))throw new Error('Duplicate TikTok Ad leaf would double-count delivery facts');keys.add(dateKey);}
  return Object.freeze(mapped);
}

module.exports=Object.freeze({DELIVERY_FIELDS,EVENT_METRICS,TIKTOK_DELIVERY_ADAPTER_VERSION,mapTikTokDeliveryAd,mapTikTokDeliveryRows});
