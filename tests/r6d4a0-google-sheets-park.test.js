'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {
  GOOGLE_SHEETS_EXPORT_ENABLED,
  GOOGLE_SHEETS_PARK_REASON,
  requireGoogleSheetsExport,
} = require('../src/providers/google-sheets/availability-policy');

const root = path.resolve(__dirname, '..');
const read = relative => fs.readFileSync(path.join(root, relative), 'utf8');
const contract = JSON.parse(read('contracts/r6d4a0-google-sheets-park-v1.json'));

test('Google Sheets is parked until a Dataset V2 workspace export exists', () => {
  assert.equal(GOOGLE_SHEETS_EXPORT_ENABLED, false);
  assert.equal(GOOGLE_SHEETS_PARK_REASON, 'dataset_v2_export_not_designed');
  assert.throws(() => requireGoogleSheetsExport(), error =>
    error.code === 'GOOGLE_SHEETS_EXPORT_PARKED' && error.status === 410);
});

test('park contract preserves history and forbids provider or Dataset V1 work', () => {
  assert.equal(contract.decision.status, 'parked');
  assert.equal(contract.decision.embedded_connect_surface, false);
  assert.equal(contract.runtime.new_oauth, false);
  assert.equal(contract.runtime.token_refresh, false);
  assert.equal(contract.runtime.manual_sync, false);
  assert.equal(contract.runtime.auto_sync, false);
  assert.equal(contract.data.dataset_v1_export, false);
  assert.equal(contract.data.dataset_v2_export, false);
  assert.equal(contract.preservation.existing_spreadsheet_unchanged, true);
  assert.equal(contract.preservation.historical_connection_unchanged, true);
  assert.equal(contract.preservation.encrypted_tokens_unchanged, true);
  assert.equal(contract.preservation.google_grant_revoked, false);
});

test('server keeps every Google Sheets execution edge closed and reports retirement', () => {
  const server = read('server.js');
  assert.match(server, /createGoogleSheetsOAuthHandlers\([\s\S]*enabled:GOOGLE_SHEETS_EXPORT_ENABLED/);
  assert.match(server, /async function getFreshGoogleSheetsClient\(userId\)\{\s*requireGoogleSheetsExport\(\)/);
  assert.match(server, /async function updateGoogleSheetsMetadata\(userId,patch=\{\}\)\{\s*requireGoogleSheetsExport\(\)/);
  assert.match(server, /async function fetchGoogleSheetsDatasetRows\(userId\)\{\s*requireGoogleSheetsExport\(\)/);
  assert.match(server, /async function syncPerformanceDatasetToGoogleSheets\(userId,options=\{\}\)\{\s*requireGoogleSheetsExport\(\)/);
  assert.match(server, /async function maybeAutoSyncGoogleSheets\(userId\)\{\s*if\(!GOOGLE_SHEETS_EXPORT_ENABLED\)return/);
  assert.match(server, /\/api\/google-sheets\/status[\s\S]*status:"retired",retired:true/);
  assert.match(server, /\/api\/google-sheets\/disconnect[\s\S]*requireGoogleSheetsExport\(\)/);
  assert.match(server, /platform==="google_sheets"\)requireGoogleSheetsExport\(\)/);
});

test('A0 artifacts never revoke, delete, migrate, refresh, or sync a live credential', () => {
  const artifacts = [
    read('contracts/r6d4a0-google-sheets-park-v1.json'),
    read('docs/R6D4A0_GOOGLE_SHEETS_PARK.md'),
  ].join('\n');
  assert.doesNotMatch(artifacts, /oauth\/revoke|delete from|truncate|refreshAccessToken|values\.update/i);
});
