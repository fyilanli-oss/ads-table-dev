"use strict";
const assert = require("node:assert/strict");
const { test } = require("node:test");
const { snapshotSpreadJobEvidence, recoverySnapshotSpreadJobEvidence } = require("../src/jobs/snapshot-job-evidence");

test("primary spread evidence preserves canonical job metadata", () => {
  assert.deepEqual(snapshotSpreadJobEvidence({ snapshot: { id: "snapshot" }, row_counts: { ad: 2 }, performance_spread_result: { ok: true } }, { metadata: { loginCustomerId: "login", pairedPrimaryJobId: "primary" } }), { snapshot_id: "snapshot", metadata: { loginCustomerId: "login", pairedPrimaryJobId: "primary", row_counts: { ad: 2 }, performance_spread_result: { ok: true } } });
});

test("recovery spread evidence preserves legacy narrow metadata contract", () => {
  assert.deepEqual(recoverySnapshotSpreadJobEvidence({ snapshot: null, row_counts: null }), { snapshot_id: null, metadata: { row_counts: null, performance_spread_result: null } });
});
