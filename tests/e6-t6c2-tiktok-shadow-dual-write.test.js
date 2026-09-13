'use strict';

const assert=require('node:assert/strict');
const test=require('node:test');
const {createTikTokShadowDualWrite}=require('../src/providers/tiktok/shadow-dual-write');

const request={userId:'user-fixture',advertiserId:'advertiser-fixture',providerDate:'2026-09-01',legacy_option:'unchanged'};
const pass={evidence_version:'e6-tiktok-delivery-parity-v1',status:'PASS',legacy_ad_rows:1,v2_ad_rows:1,isolated_legacy_synthetic_rows:0,entity_set_match:true,delivery_facts_match:true,event_policy_match:true,synthetic_policy_match:true,synthetic_written_to_canonical:0};
function harness({legacyError=null,v2Error=null,parity=pass,mutate=false}={}){const calls=[];const coordinator=createTikTokShadowDualWrite({legacyWrite:async input=>{calls.push('legacy');if(mutate)input.legacy_option='changed';if(legacyError)throw legacyError;return{id:'legacy-result'};},v2Run:async()=>{calls.push('v2');if(v2Error)throw v2Error;return{result:{dataset_v2:{persisted:1}}};},loadLegacyRows:async()=>{calls.push('legacyRows');return[];},loadV2Rows:async()=>{calls.push('v2Rows');return[];},evaluateParity:()=>{calls.push('parity');return parity;}});return{coordinator,calls};}

test('E6-T6C2 keeps legacy authoritative while shadow V2 parity passes',async()=>{
  const h=harness(),before=structuredClone(request),result=await h.coordinator.run(request);
  assert.deepEqual(h.calls,['legacy','v2','legacyRows','v2Rows','parity']);assert.deepEqual(request,before);assert.deepEqual(result.legacy_result,{id:'legacy-result'});
  assert.equal(result.shadow_evidence.status,'PASS');assert.equal(result.shadow_evidence.parity_passed,true);assert.equal(result.shadow_evidence.production_activation,false);
});

test('shadow V2 failure never converts a completed legacy write into a failed legacy result',async()=>{
  const error=new Error('raw provider body');Object.defineProperty(error,'safe_stage',{value:'TIKTOK_PROVIDER_REPORT'});const h=harness({v2Error:error}),result=await h.coordinator.run(request);
  assert.deepEqual(h.calls,['legacy','v2']);assert.deepEqual(result.legacy_result,{id:'legacy-result'});assert.deepEqual(result.shadow_evidence,{evidence_version:'e6-tiktok-shadow-v1',status:'FAIL',legacy_completed:true,v2_completed:false,parity_evaluated:false,parity_passed:false,failure_stage:'TIKTOK_PROVIDER_REPORT',production_activation:false});assert.doesNotMatch(JSON.stringify(result),/raw provider body/);
});

test('parity drift is visible but cannot activate production or alter legacy output',async()=>{
  const fail={...pass,status:'FAIL',delivery_facts_match:false},result=await harness({parity:fail}).coordinator.run(request);
  assert.equal(result.shadow_evidence.status,'FAIL');assert.equal(result.shadow_evidence.parity_evaluated,true);assert.equal(result.shadow_evidence.parity_passed,false);assert.equal(result.shadow_evidence.production_activation,false);assert.deepEqual(result.legacy_result,{id:'legacy-result'});
});

test('legacy failure stops before V2 and carries a dedicated non-enumerable stage',async()=>{
  const h=harness({legacyError:new Error('legacy raw')});await assert.rejects(()=>h.coordinator.run(request),error=>error.message==='legacy raw'&&error.safe_stage==='TIKTOK_LEGACY_WRITE'&&!JSON.stringify(error).includes('legacy raw'));assert.deepEqual(h.calls,['legacy']);
});

test('caller request is isolated from legacy mutation and shadow failures are allowlisted',async()=>{
  const before=structuredClone(request),unknown=harness({mutate:true,v2Error:new Error('secret')});const result=await unknown.coordinator.run(request);assert.deepEqual(request,before);assert.equal(result.shadow_evidence.failure_stage,'UNCLASSIFIED');assert.doesNotMatch(JSON.stringify(result.shadow_evidence),/secret|advertiser-fixture|user-fixture/);
});
