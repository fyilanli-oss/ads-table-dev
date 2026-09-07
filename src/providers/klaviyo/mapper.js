'use strict';

const {validateCanonicalRow}=require('../../../funnel-core/canonical-contract');
const {buildEntityKey,validateEntityHierarchy}=require('../../../funnel-core/entity-hierarchy');

const ADAPTER_VERSION='klaviyo-v1';
const CHANNELS=Object.freeze(['email','sms']);
const BRANCHES=Object.freeze({campaign:'campaign_message',flow:'flow_message'});
const JOURNEY_METRICS=Object.freeze(['add_to_cart','add_to_cart_value','checkout','checkout_value','purchase','purchase_value']);

function text(value,field){if(typeof value!=='string'||!value.trim())throw new Error(`${field} is required`);return value.trim();}
function number(value,field){if(value===null||value===undefined||value==='')return null;const parsed=Number(value);if(!Number.isFinite(parsed)||parsed<0)throw new Error(`${field} must be a non-negative number`);return parsed;}
function support(value,declared,field){if(value!==null){if(declared!==undefined&&declared!=='supported')throw new Error(`${field} measured value requires supported status`);return'supported';}if(declared===undefined)return'unknown';if(!['unknown','unsupported'].includes(declared))throw new Error(`${field} support must be unknown|unsupported when value is absent`);return declared;}

function mapKlaviyoMessage(input,context={}){
  if(!input||typeof input!=='object'||Array.isArray(input))throw new TypeError('Klaviyo message input is required');
  const branch=text(input.branch,'branch').toLowerCase(),channel=text(input.channel,'channel').toLowerCase();
  if(!Object.hasOwn(BRANCHES,branch))throw new Error('branch must be campaign|flow');
  if(!CHANNELS.includes(channel))throw new Error('channel must be email|sms');
  const identity={user_id:text(context.userId,'context.userId'),platform:'klaviyo',traffic_type:'paid',source_system:'klaviyo',channel,platform_account_id:text(context.accountId,'context.accountId'),date:text(context.businessDate,'context.businessDate')};
  const entity={campaign_type:null,root_entity_type:branch,root_entity_id:text(input.root?.id,'root.id'),root_entity_name:text(input.root?.name,'root.name'),parent_entity_type:null,parent_entity_id:null,parent_entity_name:null,entity_type:BRANCHES[branch],entity_id:text(input.message?.id,'message.id'),entity_name:text(input.message?.name,'message.name')};
  const metrics=input.metrics||{},declared=input.metric_support||{};
  const impression=number(metrics.delivered,'metrics.delivered'),adClick=number(metrics.unique_clicks,'metrics.unique_clicks');
  if(impression===null||adClick===null)throw new Error('Klaviyo delivered and unique_clicks are required measured facts');
  const values={impression,ad_click:adClick,session:null,spend_value:null};
  const metricSupport={impression:'supported',ad_click:'supported',session:'unsupported',spend_value:'unsupported'};
  if(channel==='sms'){
    values.spend_value=number(metrics.provider_spend,'metrics.provider_spend');
    metricSupport.spend_value=values.spend_value===null?'unsupported':'supported';
  }
  for(const field of JOURNEY_METRICS){values[field]=number(metrics[field],`metrics.${field}`);metricSupport[field]=support(values[field],declared[field],field);}
  const sourceCurrency=text(context.sourceCurrency,'context.sourceCurrency'),targetCurrency=text(context.targetCurrency||sourceCurrency,'context.targetCurrency');
  if(sourceCurrency!==targetCurrency)throw new Error('Cross-currency Klaviyo mapping requires E7-T8 FX binding');
  const opens=number(metrics.unique_opens,'metrics.unique_opens');
  const hasUnknown=Object.values(metricSupport).includes('unknown');
  const row={identity,entity,raw_metrics:values,metric_support:metricSupport,currency:{source_currency:sourceCurrency,target_currency:targetCurrency,fx_rate:context.fxRate??null,fx_rate_date:context.fxRateDate??null,fx_provider:context.fxProvider??null,fx_engine_version:context.fxEngineVersion??null},time:{source_timezone:text(context.sourceTimezone,'context.sourceTimezone'),business_date:identity.date,time_engine_version:text(context.timeEngineVersion||'v1','context.timeEngineVersion')},provenance:{source_system:'klaviyo',adapter_version:ADAPTER_VERSION,source_confidence:hasUnknown?'partial':'real',synthetic:false,ga4_property_id:null,source_job_id:context.sourceJobId||null,raw_reference:{branch,channel,open_count_present:opens!==null,click_source:'unique_clicks',spend_source:metricSupport.spend_value==='supported'?'provider_spend':null}}};
  validateCanonicalRow(row);validateEntityHierarchy(identity,entity);
  return Object.freeze({row,entityKey:buildEntityKey(identity,entity)});
}

module.exports=Object.freeze({ADAPTER_VERSION,BRANCHES,CHANNELS,JOURNEY_METRICS,mapKlaviyoMessage});
