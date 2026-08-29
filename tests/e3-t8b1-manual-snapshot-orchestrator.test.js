"use strict";
const assert = require("node:assert/strict");
const { test } = require("node:test");
const { createManualSnapshotOrchestrator } = require("../src/jobs/manual-snapshot-orchestrator");

function boundary(overrides = {}) {
  const calls = [];
  return { calls, jobBoundary: { async run(input) { calls.push(input); const job = { id: "job-1", metadata: input.metadata }; const result = await input.work({ jobId: job.id, job }); calls.push(input.completed(result, job)); return { job, result }; }, ...overrides } };
}

test("binds manual lifecycle metadata and source job identity around provider work", async () => {
  const target = boundary();
  const writes = [];
  const orchestrator = createManualSnapshotOrchestrator({ jobBoundary: target.jobBoundary });
  const output = await orchestrator.run({ userId: "user", platform: "organic", platformAccountId: "property", datePreset: "today", snapshotDate: "2026-08-28", write: async context => (writes.push(context), { snapshot: { id: "snapshot" }, performance_spread_result: { ok: true } }) });
  const input = target.calls[0];
  assert.deepEqual({ userId: input.userId, platform: input.platform, platformAccountId: input.platformAccountId, metadata: input.metadata }, { userId: "user", platform: "organic", platformAccountId: "property", metadata: { trigger: "manual", datePreset: "today", snapshotDate: "2026-08-28", captureReason: "manual_refresh", snapshotClass: "primary" } });
  assert.deepEqual(writes, [{ sourceJobId: "job-1", captureReason: "manual_refresh", snapshotClass: "primary" }]);
  assert.equal(Object.isFrozen(writes[0]), true);
  assert.equal(output.result.snapshot.id, "snapshot");
  assert.deepEqual(target.calls[1], { snapshot_id: "snapshot", metadata: { ...input.metadata, performance_spread_result: { ok: true } } });
});

test("preserves null snapshot and spread evidence in completion contract", async () => {
  const target = boundary();
  await createManualSnapshotOrchestrator({ jobBoundary: target.jobBoundary }).run({ userId: "user", platform: "tiktok", platformAccountId: "account", write: async () => ({}) });
  assert.deepEqual(target.calls[1], { snapshot_id: null, metadata: { trigger: "manual", datePreset: undefined, snapshotDate: undefined, captureReason: "manual_refresh", snapshotClass: "primary", performance_spread_result: null } });
});

test("does not intercept provider errors owned by the lifecycle boundary", async () => {
  const failure = new Error("provider failed");
  const orchestrator = createManualSnapshotOrchestrator({ jobBoundary: { run: async input => input.work({ jobId: "job" }) } });
  await assert.rejects(orchestrator.run({ write: async () => { throw failure; } }), failure);
});


test("merges provider job metadata and delegates custom completion evidence", async () => {
  const target = boundary();
  const custom = (result, job) => ({ snapshot_id: result.snapshot.id, metadata: { ...job.metadata, custom: true } });
  await createManualSnapshotOrchestrator({ jobBoundary: target.jobBoundary }).run({ platform: "google", jobMetadata: { dateRange: "today", trigger: "untrusted" }, complete: custom, write: async () => ({ snapshot: { id: "snapshot" } }) });
  assert.equal(target.calls[0].metadata.trigger, "manual");
  assert.equal(target.calls[0].metadata.dateRange, "today");
  assert.equal(target.calls[0].completed, custom);
  assert.equal(target.calls[1].metadata.custom, true);
});

test("fails closed for missing boundary and work dependencies", async () => {
  assert.throws(() => createManualSnapshotOrchestrator(), /jobBoundary.run/);
  const orchestrator = createManualSnapshotOrchestrator({ jobBoundary: { run() {} } });
  await assert.rejects(orchestrator.run({}), /write must be a function/);
  await assert.rejects(orchestrator.run({ write() {}, complete: true }), /complete must be a function/);
});
