'use strict';

const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const test=require('node:test');
const {createTikTokLiveShadow,providerRows}=require('../src/providers/tiktok/live-shadow');

const legacyAd={platform:'TikTok',level:'ad',id:'ad-1',id_in_platform:'ad-1',campaign_id:'campaign-1',campaign_name:'Campaign',adgroup_id:'adgroup-1',adgroup_name:'Ad group',ad_id:'ad-1',ad_name:'Ad',spend:10,impressions:100,clicks:5,source_confidence:'tiktok_report_api',raw:{synthetic:false}};
function supabase(){let payload=[];return{from(){return{upsert(rows){payload=rows;return{select:async()=>({data:payload,error:null})}}}}};}

test('E6-T6D2 bridges only provider-derived Ad leaves and preserves synthetic markers',()=>{
  const rows=providerRows([{...legacyAd,level:'campaign'},{...legacyAd},{...legacyAd,ad_id:'fallback',raw:{synthetic:true}}],'2026-09-01');
  assert.equal(rows.length,2);assert.equal(rows[0].dimensions.ad_id,'ad-1');assert.equal(rows[1].synthetic,true);assert.deepEqual(rows[0].metrics,{impressions:100,clicks:5,spend:10});
});

test('approved live shadow keeps legacy authoritative and returns redacted PASS evidence',async()=>{
  const live=createTikTokLiveShadow({supabaseClient:supabase(),resolveFxRate:async()=>({fx_rate:1,fx_rate_date:'2026-09-01',fx_provider:'same_currency'})});
  const result=await live.run({request:{userId:'11111111-1111-4111-8111-111111111111',advertiserId:'advertiser-1',providerDate:'2026-09-01'},advertiser:{id:'advertiser-1',currency:'USD',timezone:'UTC'},targetCurrency:'USD',sourceJobId:'22222222-2222-4222-8222-222222222222',legacyWrite:async()=>({snapshot:{id:'legacy-snapshot',performance_summary:{rows:[legacyAd]}}})});
  assert.equal(result.legacy_result.snapshot.id,'legacy-snapshot');assert.equal(result.shadow_evidence.status,'PASS');assert.equal(result.shadow_evidence.production_activation,false);assert.equal(result.shadow_evidence.parity.synthetic_written_to_canonical,0);
});

test('server registration is explicit flag-gated and reports shadow evidence without primary activation',()=>{
  const server=fs.readFileSync(path.join(__dirname,'..','server.js'),'utf8');const config=fs.readFileSync(path.join(__dirname,'..','security','production-config.js'),'utf8');
  assert.match(config,/TIKTOK_V2_SHADOW_ENABLED/);assert.match(server,/productionConfig\.tiktokV2ShadowEnabled\?createTikTokLiveShadow/);assert.match(server,/tiktok_shadow:shadowEvidence/);assert.doesNotMatch(server,/TIKTOK_V2_PRIMARY_REFRESH_ENABLED/);
});
