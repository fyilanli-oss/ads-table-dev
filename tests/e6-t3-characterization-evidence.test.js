'use strict';

const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const test=require('node:test');

const evidence=JSON.parse(fs.readFileSync(path.join(__dirname,'..','artifacts','e6-tiktok','e6-t2-t3-sandbox-characterization-result.json'),'utf8'));
const audit=fs.readFileSync(path.join(__dirname,'..','docs','E6_T2_TIKTOK_ACCOUNT_DISCOVERY_AUDIT.md'),'utf8');

test('sandbox characterization evidence is redacted, read-only and zero-row',()=>{
  assert.equal(evidence.writes_performed,false);
  assert.equal(evidence.raw_response_included,false);
  assert.equal(evidence.probes.length,9);
  assert.equal(evidence.probes.every(probe=>probe.accepted&&!probe.field_present&&probe.result_shape==='zero_row'),true);
  assert.doesNotMatch(JSON.stringify(evidence),/token|advertiser|secret|id\b|value":\s*[-\d]/i);
});

test('zero-row evidence cannot be treated as event semantic acceptance',()=>{
  assert.equal(evidence.probes.some(probe=>probe.field_present),false);
  assert.match(audit,/delivery-only ilerleme seçildi/);
  assert.match(audit,/ATC, Checkout, Purchase.*`unsupported\/null`/);
  assert.match(audit,/Generic conversion fallback.*yasaktır/);
});
