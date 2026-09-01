'use strict';

const assert=require('node:assert/strict');
const test=require('node:test');
const fixture=require('./fixtures/e6-t4-tiktok-delivery-row.json');
const {mapTikTokTimeFxAd}=require('../src/providers/tiktok/time-fx-normalization');
const {assertTikTokDeliveryParity,evaluateTikTokDeliveryParity}=require('../src/providers/tiktok/delivery-parity');

const context={...fixture.context,providerDate:'2026-09-01',advertiser:{id:fixture.context.advertiserId,currency:'USD',timezone:'UTC'},targetCurrency:'USD'};
const canonical=()=>mapTikTokTimeFxAd(fixture.input,context).row;
const legacy=()=>({level:'ad',ad_id:'ad-1',impressions:1250,clicks:75,spend:42.5,raw:{synthetic:false}});

test('E6-T6C1 produces redacted PASS evidence for entity-level delivery parity',()=>{
  const evidence=assertTikTokDeliveryParity({legacyRows:[legacy()],v2Rows:[canonical()]});
  assert.equal(evidence.status,'PASS');assert.equal(evidence.entity_set_match,true);assert.equal(evidence.delivery_facts_match,true);assert.equal(evidence.event_policy_match,true);assert.equal(evidence.synthetic_written_to_canonical,0);
  assert.doesNotMatch(JSON.stringify(evidence),/ad-1|42\.5|1250|advertiser-fixture/);
});

test('metric drift fails without exposing the differing values',()=>{
  const drift=legacy();drift.spend=43;const evidence=evaluateTikTokDeliveryParity({legacyRows:[drift],v2Rows:[canonical()]});
  assert.equal(evidence.status,'FAIL');assert.equal(evidence.delivery_facts_match,false);assert.doesNotMatch(JSON.stringify(evidence),/42\.5|43/);
  assert.throws(()=>assertTikTokDeliveryParity({legacyRows:[drift],v2Rows:[canonical()]}),error=>error.message==='TikTok delivery parity failed'&&error.parity_evidence.status==='FAIL'&&!JSON.stringify(error).includes('43'));
});

test('legacy synthetic placeholders are isolated and cannot create canonical parity rows',()=>{
  const fallback={...legacy(),ad_id:'fallback',synthetic:true,raw:{synthetic:true,fallback_reason:'empty_report'}};
  const evidence=assertTikTokDeliveryParity({legacyRows:[legacy(),fallback],v2Rows:[canonical()]});assert.equal(evidence.isolated_legacy_synthetic_rows,1);assert.equal(evidence.legacy_ad_rows,1);assert.equal(evidence.v2_ad_rows,1);
});

test('entity mismatch and event-policy violation produce FAIL evidence',()=>{
  const wrong=legacy();wrong.ad_id='other';assert.equal(evaluateTikTokDeliveryParity({legacyRows:[wrong],v2Rows:[canonical()]}).entity_set_match,false);
  const event=structuredClone(canonical());event.raw_metrics.purchase=1;event.metric_support.purchase='supported';const evidence=evaluateTikTokDeliveryParity({legacyRows:[legacy()],v2Rows:[event]});assert.equal(evidence.status,'FAIL');assert.equal(evidence.event_policy_match,false);
});

test('duplicate and malformed inputs fail closed without mutating either side',()=>{
  const left=[legacy()],right=[canonical()],before=JSON.stringify({left,right});
  assert.throws(()=>evaluateTikTokDeliveryParity({legacyRows:[legacy(),legacy()],v2Rows:right}),/Duplicate legacy/);
  const negative=legacy();negative.clicks=-1;assert.throws(()=>evaluateTikTokDeliveryParity({legacyRows:[negative],v2Rows:right}),/non-negative/);
  assert.equal(JSON.stringify({left,right}),before);
});
