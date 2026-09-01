'use strict';

const assert=require('node:assert/strict');
const test=require('node:test');
const fixture=require('./fixtures/e6-t4-tiktok-delivery-row.json');
const {createTikTokDatasetWriter}=require('../src/providers/tiktok/dataset-writer');

const advertiser={id:fixture.context.advertiserId,currency:'USD',timezone:'America/Los_Angeles'};
function harness({write=async rows=>rows,resolveFxRate=async(_source,_target,{rateDate})=>({fx_rate:32,fx_rate_date:rateDate,fx_provider:'approved-test'})}={}){const writes=[],writer=createTikTokDatasetWriter({writeBoundary:{write:async rows=>{writes.push(rows);return write(rows);}},resolveFxRate});return{writer,writes};}
function input(rows=[fixture.input],patch={}){return{advertiserId:advertiser.id,rows,context:{userId:'user-fixture',advertiser,providerDate:'2026-09-01',targetCurrency:'TRY',sourceJobId:'job-fixture',...patch}};}

test('E6-T6B1 writes only canonical delivery facts to the V2 boundary',async()=>{
  const h=harness(),result=await h.writer.ingest(input()),row=h.writes[0][0];
  assert.deepEqual({attempted:result.attempted,persisted:result.persisted,isolated_synthetic_rows:result.isolated_synthetic_rows,synthetic_written_to_canonical:result.synthetic_written_to_canonical},{attempted:1,persisted:1,isolated_synthetic_rows:0,synthetic_written_to_canonical:0});
  assert.equal(row.raw_metrics.spend_value,1360);assert.equal(row.raw_metrics.purchase,null);assert.equal(row.metric_support.purchase,'unsupported');assert.equal(row.provenance.source_job_id,'job-fixture');
});

test('zero-row and synthetic-only provider results write no fake canonical facts',async()=>{
  const empty=harness(),emptyResult=await empty.writer.ingest(input([]));assert.equal(emptyResult.empty_provider_result,true);assert.deepEqual(empty.writes,[[]]);
  const fallback={...structuredClone(fixture.input),synthetic:true},synthetic=harness(),syntheticResult=await synthetic.writer.ingest(input([fallback]));assert.equal(syntheticResult.attempted,0);assert.equal(syntheticResult.isolated_synthetic_rows,1);assert.equal(syntheticResult.synthetic_written_to_canonical,0);assert.deepEqual(synthetic.writes,[[]]);
});

test('ownership and write cardinality mismatches fail closed',async()=>{
  const ownership=harness();await assert.rejects(()=>ownership.writer.ingest({...input(),advertiserId:'other'}),/ownership mismatch/);
  const cardinality=harness({write:async()=>[]});await assert.rejects(()=>cardinality.writer.ingest(input()),/cardinality mismatch/);
});

test('FX, adapter and V2 failures carry safe stages without changing messages',async()=>{
  const fx=harness({resolveFxRate:async()=>{throw new Error('fx raw');}});await assert.rejects(()=>fx.writer.ingest(input()),error=>error.message==='fx raw'&&error.safe_stage==='TIKTOK_FX_LOOKUP');
  const adapter=harness();await assert.rejects(()=>adapter.writer.ingest(input([{bad:true}])),error=>error.safe_stage==='TIKTOK_DELIVERY_ADAPTER');
  const db=harness({write:async()=>{throw new Error('db raw');}});await assert.rejects(()=>db.writer.ingest(input()),error=>error.message==='db raw'&&error.safe_stage==='TIKTOK_DATASET_V2_WRITE');
});

test('writer evidence contains counts only and no advertiser, metric or provider payload',async()=>{
  const result=await harness().writer.ingest(input()),evidence={attempted:result.attempted,persisted:result.persisted,empty_provider_result:result.empty_provider_result,isolated_synthetic_rows:result.isolated_synthetic_rows,synthetic_written_to_canonical:result.synthetic_written_to_canonical};
  const serialized=JSON.stringify(evidence);assert.doesNotMatch(serialized,/advertiser-fixture|1360|Delivery Campaign|job-fixture/);
});
