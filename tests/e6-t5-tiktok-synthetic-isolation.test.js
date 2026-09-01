'use strict';

const assert=require('node:assert/strict');
const test=require('node:test');
const fixture=require('./fixtures/e6-t4-tiktok-delivery-row.json');
const {isSyntheticTikTokFallback,isolateTikTokProductionRows,mapTikTokProductionDeliveryRows}=require('../src/providers/tiktok/synthetic-isolation');

function fallback(overrides={}){return{level:'ad',dimensions:{campaign_id:'fallback-campaign',campaign_name:'Fallback Campaign',adgroup_id:'fallback-adgroup',adgroup_name:'Fallback AdGroup',ad_id:'fallback-ad',ad_name:'Fallback Ad'},metrics:{impressions:0,clicks:0,spend:0},raw:{fallback_reason:'empty_report_level_fallback',synthetic:true},...overrides};}

test('E6-T5 isolates explicit legacy fallback rows before canonical mapping',()=>{
  const production=structuredClone(fixture.input),synthetic=fallback();
  const result=isolateTikTokProductionRows([production,synthetic]);
  assert.deepEqual(result.productionRows,[production]);
  assert.deepEqual(result.evidence,{source_rows:2,production_rows:1,isolated_synthetic_rows:1,synthetic_written_to_canonical:0});
});

test('all legacy fallback marker families are isolated fail-closed',()=>{
  const rows=[fallback({raw:{synthetic:true}}),fallback({raw:{fallback_reason:'empty'}}),fallback({raw:{},source_confidence:'sandbox_empty_report_fallback'}),fallback({raw:{},provenance:{synthetic:true}}),fallback({raw:{},dimensions:{campaign_id:'c',campaign_name:'c',adgroup_id:'g',adgroup_name:'g',ad_id:'ad_fallback',ad_name:'a'}})];
  assert.equal(rows.every(isSyntheticTikTokFallback),true);
  assert.equal(isolateTikTokProductionRows(rows).productionRows.length,0);
});

test('production wrapper maps only real Ad leaves and reports isolation counts',()=>{
  const result=mapTikTokProductionDeliveryRows([structuredClone(fixture.input),fallback()],fixture.context);
  assert.equal(result.mapped.length,1);assert.equal(result.mapped[0].row.provenance.synthetic,false);
  assert.equal(result.isolation.isolated_synthetic_rows,1);assert.equal(result.isolation.synthetic_written_to_canonical,0);
});

test('synthetic-only input produces no canonical row instead of a measured zero',()=>{
  const result=mapTikTokProductionDeliveryRows([fallback()],fixture.context);
  assert.deepEqual(result.mapped,[]);assert.equal(result.isolation.production_rows,0);assert.equal(result.isolation.synthetic_written_to_canonical,0);
});

test('malformed source shapes fail instead of bypassing the isolation boundary',()=>{
  assert.throws(()=>isolateTikTokProductionRows([null]),/must be an object/);
  assert.throws(()=>isolateTikTokProductionRows({}),/must be an array/);
});
