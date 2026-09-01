'use strict';

const {mapTikTokDeliveryRows}=require('./delivery-mapper');

const FALLBACK_MARKERS=Object.freeze(['fallback','synthetic','sandbox_empty_report','empty_period']);

function marker(value){const normalized=String(value??'').toLowerCase();return FALLBACK_MARKERS.some(item=>normalized.includes(item));}
function isSyntheticTikTokFallback(row){
  if(!row||typeof row!=='object'||Array.isArray(row))throw new TypeError('TikTok source row must be an object');
  if(row.synthetic===true||row.raw?.synthetic===true||row.provenance?.synthetic===true)return true;
  if(row.raw?.fallback_reason||marker(row.source_confidence)||marker(row.provenance?.source_confidence))return true;
  const dimensions=row.dimensions||{};
  return [row.status,row.campaign_status,row.adgroup_status,row.ad_status,row.id,row.id_in_platform,dimensions.campaign_id,dimensions.adgroup_id,dimensions.ad_id].some(marker);
}

function isolateTikTokProductionRows(rows){
  if(!Array.isArray(rows))throw new TypeError('TikTok source rows must be an array');
  const productionRows=[];let isolatedSyntheticRows=0;
  for(const row of rows){if(isSyntheticTikTokFallback(row))isolatedSyntheticRows+=1;else productionRows.push(row);}
  return Object.freeze({productionRows:Object.freeze(productionRows),evidence:Object.freeze({source_rows:rows.length,production_rows:productionRows.length,isolated_synthetic_rows:isolatedSyntheticRows,synthetic_written_to_canonical:0})});
}

function mapTikTokProductionDeliveryRows(rows,context={}){
  const isolated=isolateTikTokProductionRows(rows);
  return Object.freeze({mapped:mapTikTokDeliveryRows(isolated.productionRows,context),isolation:isolated.evidence});
}

module.exports=Object.freeze({FALLBACK_MARKERS,isSyntheticTikTokFallback,isolateTikTokProductionRows,mapTikTokProductionDeliveryRows});
