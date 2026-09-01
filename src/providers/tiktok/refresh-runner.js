'use strict';

function required(value,field){if(typeof value!=='string'||!value.trim())throw new Error(`${field} is required`);return value.trim();}
async function atStage(stage,work){try{return await work();}catch(error){if(error&&typeof error==='object'&&Object.isExtensible(error)&&!error.safe_stage)Object.defineProperty(error,'safe_stage',{value:stage,enumerable:false});throw error;}}
function rows(response){if(!response||typeof response!=='object'||Array.isArray(response))throw new Error('TikTok delivery response must be an object');if(!Object.prototype.hasOwnProperty.call(response,'rows'))return[];if(!Array.isArray(response.rows))throw new Error('TikTok delivery response rows must be an array');return response.rows;}
function advertiser(value,requestedId){if(!value||typeof value!=='object'||Array.isArray(value))throw new Error('TikTok advertiser metadata is required');const id=required(value.id,'advertiser.id');if(id!==requestedId)throw new Error('TikTok advertiser metadata identity mismatch');return Object.freeze({id,currency:required(value.currency,'advertiser.currency'),timezone:required(value.timezone,'advertiser.timezone')});}

function createTikTokRefreshRunner({client,writer,jobBoundary,resolveTargetCurrency}={}){
  if(!client||typeof client.fetchAdvertiserMetadata!=='function'||typeof client.fetchDeliveryRows!=='function')throw new TypeError('TikTok live client is required');
  if(!writer||typeof writer.ingest!=='function')throw new TypeError('TikTok writer is required');
  if(!jobBoundary||typeof jobBoundary.run!=='function')throw new TypeError('refresh job boundary is required');
  if(typeof resolveTargetCurrency!=='function')throw new TypeError('target currency resolver is required');
  return Object.freeze({async run({userId,advertiserId,providerDate}={}){
    const owner=required(userId,'userId'),account=required(advertiserId,'advertiserId'),date=required(providerDate,'providerDate');if(!/^\d{4}-\d{2}-\d{2}$/.test(date))throw new Error('TikTok providerDate must be YYYY-MM-DD');
    return jobBoundary.run({userId:owner,platform:'tiktok',platformAccountId:account,metadata:{trigger:'manual_v2',mode:'delivery_only'},work:async({jobId})=>{
      const metadata=await atStage('TIKTOK_ADVERTISER_METADATA',()=>client.fetchAdvertiserMetadata({userId:owner,advertiserId:account})),accountMetadata=advertiser(metadata,account);
      const response=await atStage('TIKTOK_PROVIDER_REPORT',()=>client.fetchDeliveryRows({userId:owner,advertiserId:account,providerDate:date,dataLevel:'AUCTION_AD',metrics:['spend','impressions','clicks']}));
      const providerRows=rows(response),targetCurrency=required(await atStage('TIKTOK_TARGET_CURRENCY',()=>resolveTargetCurrency(owner)),'targetCurrency');
      const write=await writer.ingest({advertiserId:account,rows:providerRows,context:{userId:owner,advertiser:accountMetadata,providerDate:date,targetCurrency,sourceJobId:jobId}});
      const datasetV2=Object.freeze({attempted:write.attempted,persisted:write.persisted,empty_provider_result:providerRows.length===0,isolated_synthetic_rows:write.isolated_synthetic_rows,synthetic_written_to_canonical:write.synthetic_written_to_canonical});
      const evidence=Object.freeze({evidence_version:'e6-tiktok-v2-v1',mode:'delivery_only',mapping:Object.freeze({provider_row_count:providerRows.length,accepted_row_count:write.attempted,isolated_synthetic_rows:write.isolated_synthetic_rows,event_metrics_written:0}),dataset_v2:datasetV2});
      return Object.freeze({mode:'v2_upsert',dataset_v2:datasetV2,tiktok_v2_evidence:evidence});
    },completed:(result,job)=>({metadata:{...(job.metadata||{}),tiktok_v2_evidence:result.tiktok_v2_evidence}})});
  }});
}

module.exports=Object.freeze({atStage,createTikTokRefreshRunner,rows});
