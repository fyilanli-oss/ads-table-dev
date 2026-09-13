'use strict';

const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const test=require('node:test');
const {evaluateTikTokActivationReadiness,REQUIRED_CODE_GATES}=require('../src/providers/tiktok/activation-readiness');

const code=Object.fromEntries(REQUIRED_CODE_GATES.map(gate=>[gate,true]));
const input=(patch={})=>({code_gates:code,main_checks_passed:true,rollback_ready:true,shadow_rollout_approved:false,shadow_runtime_registered:false,live_shadow_runs:0,live_parity_passes:0,live_synthetic_writes_zero:true,primary_activation_approved:false,...patch});

test('E6-T6D1 remains blocked before separate shadow and primary approvals',()=>{
  const evidence=evaluateTikTokActivationReadiness(input());assert.equal(evidence.status,'BLOCKED');assert.equal(evidence.code_ready,true);assert.equal(evidence.shadow_ready,false);assert.equal(evidence.production_activation_performed,false);assert.deepEqual(evidence.reason_codes,['SHADOW_ROLLOUT_APPROVAL_REQUIRED','SHADOW_RUNTIME_NOT_REGISTERED','LIVE_PARITY_EVIDENCE_REQUIRED','PRIMARY_ACTIVATION_APPROVAL_REQUIRED']);
});

test('shadow approval alone cannot replace live parity evidence or primary approval',()=>{
  const evidence=evaluateTikTokActivationReadiness(input({shadow_rollout_approved:true,shadow_runtime_registered:true}));assert.equal(evidence.shadow_ready,true);assert.equal(evidence.live_evidence_ready,false);assert.equal(evidence.status,'BLOCKED');assert.equal(evidence.reason_codes.includes('LIVE_PARITY_EVIDENCE_REQUIRED'),true);assert.equal(evidence.reason_codes.includes('PRIMARY_ACTIVATION_APPROVAL_REQUIRED'),true);
});

test('three clean live parity passes still require explicit primary activation approval',()=>{
  const evidence=evaluateTikTokActivationReadiness(input({shadow_rollout_approved:true,shadow_runtime_registered:true,live_shadow_runs:3,live_parity_passes:3}));assert.equal(evidence.live_evidence_ready,true);assert.equal(evidence.status,'BLOCKED');assert.deepEqual(evidence.reason_codes,['PRIMARY_ACTIVATION_APPROVAL_REQUIRED']);
});

test('READY is evidence only and never performs production activation',()=>{
  const evidence=evaluateTikTokActivationReadiness(input({shadow_rollout_approved:true,shadow_runtime_registered:true,live_shadow_runs:3,live_parity_passes:3,primary_activation_approved:true}));assert.equal(evidence.status,'READY');assert.equal(evidence.production_activation_performed,false);assert.deepEqual(evidence.reason_codes,[]);
});

test('invalid counters and incomplete code gates fail closed',()=>{
  assert.throws(()=>evaluateTikTokActivationReadiness(input({live_shadow_runs:1,live_parity_passes:2})),/cannot exceed/);assert.throws(()=>evaluateTikTokActivationReadiness(input({live_shadow_runs:-1})),/non-negative integer/);const incomplete={...code,dataset_writer:false},evidence=evaluateTikTokActivationReadiness(input({code_gates:incomplete}));assert.equal(evidence.code_ready,false);assert.equal(evidence.reason_codes.includes('CODE_GATES_INCOMPLETE'),true);
});

test('committed readiness artifact is blocked, redacted and performs no activation',()=>{
  const artifact=JSON.parse(fs.readFileSync(path.join(__dirname,'..','artifacts','e6-tiktok','e6-t6d1-activation-readiness.json'),'utf8'));assert.equal(artifact.status,'BLOCKED');assert.equal(artifact.production_activation_performed,false);assert.equal(artifact.live_shadow_runs,0);assert.doesNotMatch(JSON.stringify(artifact),/advertiser|user|token|secret|metric_value/i);
});
