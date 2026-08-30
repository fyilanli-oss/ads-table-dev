'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),test=require('node:test');
const server=fs.readFileSync(path.join(__dirname,'../server.js'),'utf8');

test('Meta V2 primary refresh is wired behind one production-approved default-on gate',()=>{assert.match(server,/META_V2_PRIMARY_REFRESH_ENABLED=parseExplicitBoolean\(process\.env\.META_V2_PRIMARY_REFRESH_ENABLED,true/);assert.match(server,/metaV2LiveRefresh=supabaseAdmin\?createMetaLiveRefresh/);assert.match(server,/META_V2_PRIMARY_REFRESH_ENABLED\?metaV2LiveRefresh\.run\(/);assert.match(server,/:writeMetaSnapshotImmutable\(/);});
test('enabled Meta refresh reports Dataset V2 outcome and does not invoke shadow dual-write',()=>{assert.match(server,/row_counts:writeResult\.row_counts,dataset_v2:writeResult\.dataset_v2\|\|null/);const handler=server.slice(server.indexOf('async function handleMetaSnapshotWrite'),server.indexOf('app.post("/api/snapshots/meta/write"'));assert.doesNotMatch(handler,/createMetaDualWriteCoordinator|dualWrite/);});
test('Meta live acceptance requests only the business date and stores redacted evidence on the refresh job',()=>{assert.match(server,/since:snapshotDate,until:snapshotDate/);assert.doesNotMatch(server,/dayCount:30/);assert.match(server,/meta_v2_evidence:result\.meta_v2_evidence\|\|null/);});
