'use strict';

const assert=require('node:assert/strict');
const test=require('node:test');
const fixture=require('./fixtures/e6-t4-tiktok-delivery-row.json');
const {mapTikTokTimeFxAd,mapTikTokProductionRowsWithTimeFx}=require('../src/providers/tiktok/time-fx-normalization');

const context={...fixture.context,providerDate:'2026-09-01',advertiser:{id:fixture.context.advertiserId,currency:'USD',timezone:'America/Los_Angeles'},targetCurrency:'USD'};

test('E6-T6A binds provider business date, timezone and same-currency FX metadata',()=>{
  const {row}=mapTikTokTimeFxAd(fixture.input,context);
  assert.deepEqual(row.time,{source_timezone:'America/Los_Angeles',business_date:'2026-09-01',time_engine_version:'v1'});
  assert.deepEqual(row.currency,{source_currency:'USD',target_currency:'USD',fx_rate:1,fx_rate_date:'2026-09-01',fx_provider:'same_currency',fx_engine_version:'v1'});
  assert.equal(row.raw_metrics.spend_value,42.5);
});

test('cross-currency normalization converts only supported spend exactly once',()=>{
  const {row}=mapTikTokTimeFxAd(fixture.input,{...context,targetCurrency:'TRY',fxRate:32,fxRateDate:'2026-09-01',fxProvider:'approved-fixture'});
  assert.equal(row.raw_metrics.spend_value,1360);
  for(const field of ['add_to_cart_value','checkout_value','purchase_value']){assert.equal(row.raw_metrics[field],null);assert.equal(row.metric_support[field],'unsupported');}
  assert.deepEqual(row.currency,{source_currency:'USD',target_currency:'TRY',fx_rate:32,fx_rate_date:'2026-09-01',fx_provider:'approved-fixture',fx_engine_version:'v1'});
});

test('advertiser identity, currency, timezone and provider date fail closed',()=>{
  assert.throws(()=>mapTikTokTimeFxAd(fixture.input,{...context,advertiserId:'other'}),/identity mismatch/);
  assert.throws(()=>mapTikTokTimeFxAd(fixture.input,{...context,advertiser:{...context.advertiser,currency:'US'}}),/3-letter currency/);
  assert.throws(()=>mapTikTokTimeFxAd(fixture.input,{...context,advertiser:{...context.advertiser,timezone:'Not\/A_Zone'}}),/Invalid IANA timezone/);
  assert.throws(()=>mapTikTokTimeFxAd(fixture.input,{...context,providerDate:'09-01-2026'}),/providerDate must be YYYY-MM-DD/);
});

test('cross-currency mapping requires an explicit rate and named provider',()=>{
  assert.throws(()=>mapTikTokTimeFxAd(fixture.input,{...context,targetCurrency:'TRY',fxProvider:'approved'}),/positive fx_rate/);
  assert.throws(()=>mapTikTokTimeFxAd(fixture.input,{...context,targetCurrency:'TRY',fxRate:32}),/context.fxProvider is required/);
  assert.throws(()=>mapTikTokTimeFxAd(fixture.input,{...context,fxRate:2}),/Same-currency normalization/);
});

test('time/FX wrapper preserves synthetic isolation and normalized duplicate guard',()=>{
  const fallback={...structuredClone(fixture.input),synthetic:true,dimensions:{...fixture.input.dimensions,ad_id:'fallback'}};
  const result=mapTikTokProductionRowsWithTimeFx([fixture.input,fallback],context);
  assert.equal(result.mapped.length,1);assert.equal(result.isolation.isolated_synthetic_rows,1);assert.equal(result.isolation.synthetic_written_to_canonical,0);
  assert.throws(()=>mapTikTokProductionRowsWithTimeFx([fixture.input,structuredClone(fixture.input)],context),/double-count/);
});
