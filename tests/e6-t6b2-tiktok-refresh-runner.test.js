'use strict';

const assert=require('node:assert/strict');
const test=require('node:test');
const fixture=require('./fixtures/e6-t4-tiktok-delivery-row.json');
const {createTikTokRefreshRunner}=require('../src/providers/tiktok/refresh-runner');

function harness({response={rows:[fixture.input]},metadata={id:fixture.context.advertiserId,currency:'USD',timezone:'America/Los_Angeles'},writerResult={attempted:1,persisted:1,isolated_synthetic_rows:0,synthetic_written_to_canonical:0},fetchError=null}={}){const calls=[],completions=[];const client={fetchAdvertiserMetadata:async input=>{calls.push(['metadata',input]);return metadata;},fetchDeliveryRows:async input=>{calls.push(['rows',input]);if(fetchError)throw fetchError;return response;}},writer={ingest:async input=>{calls.push(['write',input]);return writerResult;}},jobBoundary={run:async spec=>{const job={id:'job-fixture',metadata:spec.metadata};const result=await spec.work({jobId:job.id});completions.push(spec.completed(result,job));return{job,result};}},runner=createTikTokRefreshRunner({client,writer,jobBoundary,resolveTargetCurrency:async()=> 'TRY'});return{runner,calls,completions};}
const request={userId:'user-fixture',advertiserId:fixture.context.advertiserId,providerDate:'2026-09-01'};

test('E6-T6B2 composes metadata, AUCTION_AD delivery read, writer and redacted job evidence',async()=>{
  const h=harness(),execution=await h.runner.run(request),result=execution.result;
  assert.deepEqual(result.dataset_v2,{attempted:1,persisted:1,empty_provider_result:false,isolated_synthetic_rows:0,synthetic_written_to_canonical:0});
  assert.deepEqual(h.calls[1][1].metrics,['spend','impressions','clicks']);assert.equal(h.calls[1][1].dataLevel,'AUCTION_AD');assert.equal(h.calls[2][1].context.sourceJobId,'job-fixture');
  assert.deepEqual(h.completions[0].metadata.tiktok_v2_evidence,result.tiktok_v2_evidence);
  assert.doesNotMatch(JSON.stringify(result.tiktok_v2_evidence),/advertiser-fixture|Delivery Campaign|42\.50|job-fixture/);
});

test('omitted rows is a successful zero-row result with no fake write input',async()=>{
  const h=harness({response:{},writerResult:{attempted:0,persisted:0,isolated_synthetic_rows:0,synthetic_written_to_canonical:0}}),result=(await h.runner.run(request)).result;
  assert.equal(h.calls[2][1].rows.length,0);assert.equal(result.dataset_v2.empty_provider_result,true);assert.equal(result.tiktok_v2_evidence.mapping.accepted_row_count,0);
});

test('malformed provider shape and metadata identity mismatch fail closed',async()=>{
  await assert.rejects(()=>harness({response:{rows:{}}}).runner.run(request),/rows must be an array/);
  await assert.rejects(()=>harness({metadata:{id:'other',currency:'USD',timezone:'UTC'}}).runner.run(request),/identity mismatch/);
  await assert.rejects(()=>harness().runner.run({...request,providerDate:'09-01-2026'}),/YYYY-MM-DD/);
});

test('provider failure carries safe stage and never creates a successful evidence object',async()=>{
  const raw=new Error('provider raw body'),h=harness({fetchError:raw});await assert.rejects(()=>h.runner.run(request),error=>error.message==='provider raw body'&&error.safe_stage==='TIKTOK_PROVIDER_REPORT');assert.equal(h.completions.length,0);
});

test('evidence reports event_metrics_written zero regardless of isolated rows',async()=>{
  const result=(await harness({response:{rows:[fixture.input,{synthetic:true}]},writerResult:{attempted:1,persisted:1,isolated_synthetic_rows:1,synthetic_written_to_canonical:0}}).runner.run(request)).result;
  assert.equal(result.tiktok_v2_evidence.mapping.provider_row_count,2);assert.equal(result.tiktok_v2_evidence.mapping.isolated_synthetic_rows,1);assert.equal(result.tiktok_v2_evidence.mapping.event_metrics_written,0);
});
