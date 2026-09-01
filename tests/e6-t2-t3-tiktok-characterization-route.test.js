'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { CANDIDATE_METRICS, registerTikTokCharacterizationRoute } = require('../src/providers/tiktok/characterization-route');

function harness({enabled=true, fetchReport=async ({params}) => ({data:{list:[{metrics:{[params.metrics.at(-1)]:'secret-value'}}]}})}={}) {
  let handler; const app={get:(path,fn)=>{assert.equal(path,'/api/tiktok/sandbox/characterize');handler=fn;}};
  registerTikTokCharacterizationRoute({app,requireUser:async()=>({id:'user'}),fetchReport,enabled,sandboxBase:'https://sandbox-ads.tiktok.com/open_api',accessToken:'secret-token',advertiserId:'secret-advertiser',now:()=>new Date('2026-09-01T12:00:00Z')});
  return handler;
}
function response(){return{statusCode:200,body:null,status(code){this.statusCode=code;return this;},json(body){this.body=body;return body;}};}

test('read-only characterization probes candidates without returning values or identities',async()=>{const res=response();await harness()({},res);assert.equal(res.statusCode,200);assert.equal(res.body.writes_performed,false);assert.equal(res.body.raw_response_included,false);assert.equal(res.body.probes.length,CANDIDATE_METRICS.length);assert.equal(res.body.probes.every(x=>x.accepted&&x.field_present),true);const output=JSON.stringify(res.body);assert.doesNotMatch(output,/secret|advertiser|access.?token/i);});
test('provider rejection is reduced to safe booleans',async()=>{const res=response();await harness({fetchReport:async()=>{throw new Error('raw provider secret');}})({},res);assert.equal(res.body.probes.every(x=>!x.accepted&&!x.field_present&&x.result_shape==='rejected'),true);assert.doesNotMatch(JSON.stringify(res.body),/raw provider secret/);});
test('disabled characterization is hidden',async()=>{const res=response();await harness({enabled:false})({},res);assert.equal(res.statusCode,404);});
