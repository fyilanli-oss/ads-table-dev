'use strict';

const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const test=require('node:test');
const {ORGANIC_GA4_INGEST_ENABLED,ORGANIC_GA4_PARK_REASON,requireOrganicGa4Ingest}=require('../src/providers/organic/ingest-policy');
const {aggregateScope}=require('../funnel-core/analysis-scope');
const fixtures=require('../funnel-core/fixtures');

test('E7 T7 parks GA4 Organic ingestion with no environment bypass',()=>{assert.equal(ORGANIC_GA4_INGEST_ENABLED,false);assert.equal(ORGANIC_GA4_PARK_REASON,'utm_attribution_not_reliable');assert.throws(requireOrganicGa4Ingest,error=>error.status===410&&error.code==='ORGANIC_GA4_INGESTION_PARKED')});
test('OAuth, discovery, binding, manual snapshot and automation use the parked policy',()=>{const server=fs.readFileSync(path.join(__dirname,'..','server.js'),'utf8');assert.match(server,/ingestEnabled:ORGANIC_GA4_INGEST_ENABLED/);assert.match(server,/async function listOrganicGa4Properties\(userId\)\{\s*requireOrganicGa4Ingest\(\)/);assert.match(server,/async function bindOrganicGa4Property\(userId,body=\{\}\)\{\s*requireOrganicGa4Ingest\(\)/);assert.match(server,/async function writeOrganicSnapshotV1\([^)]*\)\{\s*requireOrganicGa4Ingest\(\)/);assert.match(server,/runOrganicAutoRefreshForSchedule\(schedule\)\{\s*if\(!ORGANIC_GA4_INGEST_ENABLED\)/)});
test('Paid Organic Blend analysis capability remains intact while ingestion is parked',()=>{const result=aggregateScope([fixtures.metaPaid(),fixtures.metaOrganic()],'blend');assert.equal(result.row_count,2);assert.equal(result.analysis_scope,'blend')});
