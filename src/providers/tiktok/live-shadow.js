'use strict';

const {CanonicalWriteBoundary}=require('../../../funnel-core/canonical-write-boundary');
const {SupabaseDatasetRepository}=require('../../../funnel-core/supabase-dataset-repository');
const {createTikTokDatasetWriter}=require('./dataset-writer');
const {createTikTokShadowDualWrite}=require('./shadow-dual-write');

function required(value,field){if(typeof value!=='string'||!value.trim())throw new Error(`${field} is required`);return value.trim();}
function legacyRows(result){const rows=result?.snapshot?.performance_summary?.rows;if(!Array.isArray(rows))throw new Error('TikTok legacy result rows are required for shadow parity');return rows;}
function providerRows(rows,providerDate){return rows.filter(row=>String(row?.level||'').toLowerCase()==='ad').map(row=>({level:'AUCTION_AD',synthetic:row?.raw?.synthetic===true||row?.source_confidence==='sandbox_empty_report_fallback',raw:row?.raw||{},dimensions:{stat_time_day:providerDate,campaign_id:row.campaign_id,campaign_name:row.campaign_name,adgroup_id:row.adgroup_id,adgroup_name:row.adgroup_name,ad_id:row.ad_id,id:row.ad_id,ad_name:row.ad_name},metrics:{impressions:row.impressions,clicks:row.clicks,spend:row.spend}}));}

function createTikTokLiveShadow({supabaseClient,resolveFxRate}={}){
  if(!supabaseClient)throw new TypeError('Supabase client is required');
  if(typeof resolveFxRate!=='function')throw new TypeError('FX resolver is required');
  const repository=new SupabaseDatasetRepository(supabaseClient),writeBoundary=new CanonicalWriteBoundary({repository}),writer=createTikTokDatasetWriter({writeBoundary,resolveFxRate});
  return Object.freeze({async run({request,legacyWrite,advertiser,targetCurrency,sourceJobId}={}){
    if(typeof legacyWrite!=='function')throw new TypeError('legacyWrite is required');
    const userId=required(request?.userId,'userId'),advertiserId=required(request?.advertiserId,'advertiserId'),providerDate=required(request?.providerDate,'providerDate');
    let canonicalRows=[],lastLegacyResult;
    const wrapped=async value=>(lastLegacyResult=await legacyWrite(value));
    const runtime=createTikTokShadowDualWrite({legacyWrite:wrapped,v2Run:async()=>{const source=providerRows(legacyRows(lastLegacyResult),providerDate);const result=await writer.ingest({advertiserId,rows:source,context:{userId,advertiser,providerDate,targetCurrency,sourceJobId}});canonicalRows=result.rows;return result;},loadLegacyRows:async({legacyResult})=>legacyRows(legacyResult),loadV2Rows:async()=>canonicalRows});
    return runtime.run({userId,advertiserId,providerDate});
  }});
}

module.exports=Object.freeze({createTikTokLiveShadow,legacyRows,providerRows});
