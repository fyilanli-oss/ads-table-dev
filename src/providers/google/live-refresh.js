'use strict';

const { CanonicalWriteBoundary } = require('../../../funnel-core/canonical-write-boundary');
const { SupabaseDatasetRepository } = require('../../../funnel-core/supabase-dataset-repository');
const { createGoogleAdapter } = require('./adapter');
const { GOOGLE_CUSTOMER_METADATA_QUERY, normalizeGoogleCustomerMetadata } = require('./account-metadata');
const { createGoogleDatasetWriter } = require('./dataset-writer');
const { createGoogleV2PrimaryCoordinator } = require('./v2-primary');

function rows(response) {
  if (!response || !Array.isArray(response.results)) throw new Error('Google search response must contain results[]');
  return response.results;
}
function uuidIdentity(value, field) {
  if (value === null || value === undefined) return null;
  const candidate = typeof value === 'string' ? value : value?.id ?? value?.sourceJobId ?? value?.source_job_id;
  if (typeof candidate !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(candidate)) {
    throw new Error(`${field} identity is invalid`);
  }
  return candidate;
}
function sourceJobIdentity(value) { return uuidIdentity(value, 'Google source job'); }
async function atStage(stage, work) {
  try { return await work(); }
  catch (error) {
    if (error && typeof error === 'object' && Object.isExtensible(error) && !error.safe_stage) {
      Object.defineProperty(error, 'safe_stage', { value: stage, enumerable: false });
    }
    throw error;
  }
}
function leafKey(row, campaignType) {
  const campaignId=String(row?.campaign?.id||'');
  const leafId=campaignType==='standard'?String((row?.adGroupAd||row?.ad_group_ad)?.ad?.id||''):String((row?.assetGroup||row?.asset_group)?.id||'');
  if(!campaignId||!leafId)throw new Error(`Google ${campaignType} result hierarchy is incomplete`);
  return `${campaignId}:${leafId}`;
}
function conversionAction(row) {
  const segments=row.segments||{},metrics=row.metrics||{};
  return Object.freeze({
    category:segments.conversionActionCategory??segments.conversion_action_category??null,
    name:segments.conversionActionName??segments.conversion_action_name??null,
    conversions:metrics.conversions??null,
    conversions_value:metrics.conversionsValue??metrics.conversions_value??null
  });
}
function mergeConversions(performanceRows, conversionRows, campaignType) {
  const actions=new Map();
  for(const row of conversionRows){const key=leafKey(row,campaignType);if(!actions.has(key))actions.set(key,[]);actions.get(key).push(conversionAction(row));}
  return performanceRows.map(row=>Object.freeze({...row,conversion_actions:Object.freeze(actions.get(leafKey(row,campaignType))||[])}));
}
function querySet(campaignType, businessDate) {
  if(!/^\d{4}-\d{2}-\d{2}$/.test(businessDate))throw new Error('Google business date is invalid');
  const date=`segments.date = '${businessDate}'`;
  if(campaignType==='standard')return Object.freeze({
    performance:`SELECT segments.date, campaign.id, campaign.name, campaign.advertising_channel_type, ad_group.id, ad_group.name, ad_group_ad.ad.id, ad_group_ad.ad.name, metrics.impressions, metrics.clicks, metrics.cost_micros FROM ad_group_ad WHERE ${date} AND campaign.advertising_channel_type != 'PERFORMANCE_MAX' LIMIT 10000`,
    conversions:`SELECT campaign.id, ad_group_ad.ad.id, segments.conversion_action_name, segments.conversion_action_category, metrics.conversions, metrics.conversions_value FROM ad_group_ad WHERE ${date} AND campaign.advertising_channel_type != 'PERFORMANCE_MAX' AND metrics.conversions > 0 LIMIT 10000`
  });
  if(campaignType==='performance_max')return Object.freeze({
    performance:`SELECT segments.date, campaign.id, campaign.name, campaign.advertising_channel_type, asset_group.id, asset_group.name, metrics.impressions, metrics.clicks, metrics.cost_micros FROM asset_group WHERE ${date} AND campaign.advertising_channel_type = 'PERFORMANCE_MAX' LIMIT 10000`,
    conversions:`SELECT campaign.id, asset_group.id, segments.conversion_action_name, segments.conversion_action_category, metrics.conversions, metrics.conversions_value FROM asset_group WHERE ${date} AND campaign.advertising_channel_type = 'PERFORMANCE_MAX' AND metrics.conversions > 0 LIMIT 10000`
  });
  throw new Error('Unsupported Google campaign type');
}
function createGoogleLiveRefresh({supabaseClient,search,resolveTargetCurrency,resolveFxRate,now=()=>new Date()}={}){
  if(!supabaseClient)throw new TypeError('Supabase client is required');
  if(typeof search!=='function')throw new TypeError('Google search is required');
  if(typeof resolveTargetCurrency!=='function')throw new TypeError('target currency resolver is required');
  if(typeof resolveFxRate!=='function')throw new TypeError('FX resolver is required');
  const repository=new SupabaseDatasetRepository(supabaseClient),writeBoundary=new CanonicalWriteBoundary({repository});
  return Object.freeze({async run({userId,customerId,loginCustomerId,sourceJobId}={}){
    const userUuid=uuidIdentity(userId,'Google user');
    const sourceJobUuid=sourceJobIdentity(sourceJobId);
    const request=query=>search({userId:userUuid,customerId,loginCustomerId,query});
    const metadata=await atStage('GOOGLE_CUSTOMER_METADATA',()=>request(GOOGLE_CUSTOMER_METADATA_QUERY));
    const customer=normalizeGoogleCustomerMetadata(metadata,{requestedCustomerId:customerId,observedAt:now()});
    const client={
      fetchStandardAdRows:async()=>{const queries=querySet('standard',customer.business_date),performance=rows(await request(queries.performance)),conversions=rows(await request(queries.conversions));return{results:mergeConversions(performance,conversions,'standard')};},
      fetchPmaxAssetGroupRows:async()=>{const queries=querySet('performance_max',customer.business_date),performance=rows(await request(queries.performance)),conversions=rows(await request(queries.conversions));return{results:mergeConversions(performance,conversions,'performance_max')};}
    };
    const adapter=createGoogleAdapter({client}),writer=createGoogleDatasetWriter({adapter,writeBoundary,resolveFxRate});
    const coordinator=createGoogleV2PrimaryCoordinator({runBranch:async({campaignType})=>{
      const targetCurrency=await atStage('GOOGLE_TARGET_CURRENCY',()=>resolveTargetCurrency(userUuid));
      const write=await atStage(campaignType==='standard'?'GOOGLE_STANDARD_INGEST':'GOOGLE_PMAX_INGEST',()=>writer.ingest({campaignType,customerId,context:{userId:userUuid,customer,targetCurrency,sourceJobId:sourceJobUuid}}));
      return{dataset_v2:{attempted:write.attempted,persisted:write.persisted,empty_provider_result:write.persisted===0}};
    }});
    return coordinator.run({userId:userUuid,customerId});
  }});
}

module.exports=Object.freeze({atStage,createGoogleLiveRefresh,mergeConversions,querySet,sourceJobIdentity,uuidIdentity});
