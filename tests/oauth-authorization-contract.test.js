'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const source = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
function body(name, next) { const start=source.indexOf(`async function ${name}(`); const end=source.indexOf(`async function ${next}(`,start+1); assert.notEqual(start,-1); return source.slice(start,end<0?source.length:end); }
test('provider ownership binds active ownership to authenticated user',()=>{const b=body('requireActiveOwnership','disconnectPlatformLifecycle');assert.match(b,/ownership\.owner_user_id!==userId/);assert.match(b,/activeOwnershipStatuses\(\)\.includes\(ownership\.status\)/);});
test('connection reads and disconnect lifecycle remain user scoped',()=>{assert.match(body('getConnection','requireConnection'),/\.eq\("user_id",userId\).*\.eq\("platform",platform\)/s);const b=body('disconnectPlatformLifecycle','createSnapshotJob');assert.match(b,/\.eq\("user_id",userId\)/);assert.match(b,/\.eq\("owner_user_id",userId\)/);});
test('snapshot, connection, and account lists use verified user.id',()=>{assert.match(source,/from\("snapshot_jobs"\)\.select\("\*"\)\.eq\("user_id",user\.id\)/);assert.match(source,/from\("platform_connections"\)\.select\([^\n]+\.eq\("user_id",user\.id\)/);assert.match(source,/from\("platform_ad_accounts"\)\.select\("\*"\)\.eq\("user_id",user\.id\)/);});
test('disconnect routes ignore body ownership identity',()=>{for(const route of ['/api/disconnect/:platform','/api/platform/meta/disconnect','/api/platform/google/disconnect']){const start=source.indexOf(`app.post("${route}"`),end=source.indexOf('\napp.',start+1),b=source.slice(start,end);assert.match(b,/requireUser\(req,res\)/);assert.match(b,/disconnectPlatformLifecycle\(user\.id,/);assert.doesNotMatch(b,/req\.body\?\.(?:user_id|owner_user_id)/);}});
