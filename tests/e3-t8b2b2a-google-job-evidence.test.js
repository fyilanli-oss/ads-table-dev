"use strict";
const assert = require("node:assert/strict");
const { test } = require("node:test");
const { googleSnapshotJobEvidence } = require("../src/jobs/google-snapshot-job-evidence");

function report(overrides = {}) { return { rawCount: 3, rows: [{}, {}], entityFallback: false, entityRawCount: 2, entityFallbackError: null, entityDiagnosticFallback: false, conversionBreakdownError: null, landingPageViewError: null, ...overrides }; }

test("builds allowlisted Google completion diagnostics for every entity grain", () => {
  const evidence = googleSnapshotJobEvidence({ snapshot: { id: "snapshot" }, row_counts: { campaign: 2 }, performance_spread_result: { ok: true }, google_api: { campaign: report(), adgroup: report({ rawCount: 1 }), ad: report({ entityFallback: true }) } }, { metadata: { dateRange: "today" } });
  assert.equal(evidence.snapshot_id, "snapshot");
  assert.equal(evidence.metadata.dateRange, "today");
  assert.deepEqual(evidence.metadata.row_counts, { campaign: 2 });
  assert.equal(evidence.metadata.google_api.campaign.effectiveRows, 2);
  assert.equal(evidence.metadata.google_api.adgroup.rawCount, 1);
  assert.equal(evidence.metadata.google_api.ad.entityFallback, true);
  assert.equal("rows" in evidence.metadata.google_api.campaign, false);
});

test("preserves null/default diagnostic semantics", () => {
  const evidence = googleSnapshotJobEvidence({ google_api: { campaign: {}, adgroup: {}, ad: {} } }, null);
  assert.equal(evidence.snapshot_id, null);
  assert.equal(evidence.metadata.performance_spread_result, null);
  assert.deepEqual(evidence.metadata.google_api.campaign, { rawCount: undefined, effectiveRows: 0, entityFallback: false, entityRawCount: 0, entityFallbackError: null, entityDiagnosticFallback: false, conversionBreakdownError: undefined, landingPageViewError: undefined });
});

test("fails closed without provider diagnostic evidence", () => {
  assert.throws(() => googleSnapshotJobEvidence({}, {}), /Google snapshot API evidence/);
});
