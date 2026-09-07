const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');

const root=path.join(__dirname,'..');
const server=fs.readFileSync(path.join(root,'server.js'),'utf8');

test('Klaviyo account discovery fails closed and never persists raw account payloads',()=>{
  assert.match(server,/Klaviyo did not return an accessible account\./);
  assert.match(server,/raw_account:null/);
  assert.doesNotMatch(server,/raw=\{error:e\.message\}/);
  assert.doesNotMatch(server,/const fallbackId=id\|\|`klaviyo_/);
});

test('Klaviyo account-selection server errors are redacted',()=>{
  assert.match(server,/status>=500\?"Account selection could not be saved\. Please try again\."/);
});

test('Klaviyo empty reports persist an empty result without a synthetic campaign',()=>{
  assert.doesNotMatch(server,/source_confidence:"klaviyo_empty_period_fallback"/);
  assert.match(server,/snapshot\.performance_summary\.empty_result=rows\.length===0/);
});

test('dashboard describes the approved Email allocation instead of estimated daily spend',()=>{
  for(const file of ['dashboard.html','dashboard-patch17H-fixed.html','dashboard-patch17H-fixed-v2.html']){
    const html=fs.readFileSync(path.join(root,'public',file),'utf8');
    assert.match(html,/Email Monthly Plan Cost/);
    assert.match(html,/allocated by daily sent volume/);
    assert.doesNotMatch(html,/Estimated Monthly Spend/);
  }
});
