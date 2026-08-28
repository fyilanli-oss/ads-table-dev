"use strict";
const assert = require("node:assert/strict");
const { test } = require("node:test");
const { createAutomationSnapshotOrchestrator } = require("../src/jobs/automation-snapshot-orchestrator");

function fakeBoundary({ failRecovery = false } = {}) {
  const calls = [];
  return { calls, boundary: { async run(input) { calls.push(input); const recovery = Boolean(input.metadata.pairedPrimaryJobId), job = { id: recovery ? "recovery-job" : "primary-job", metadata: input.metadata }; if (recovery && failRecovery) { const error = new Error("recovery failed"); error.refreshJob = job; throw error; } const result = await input.work({ jobId: job.id }); calls.push(input.completed(result, job)); return { job, result }; } } };
}
const policy = { datePreset: "yesterday", captureReason: "automation", snapshotClass: "primary", shouldRunRecoverySnapshot: true, recoveryDatePreset: "last_7d", recoveryCaptureReason: "recovery", recoverySnapshotClass: "recovery" };

test("runs primary and paired recovery through the canonical job boundary", async () => {
  const target = fakeBoundary(), writes = [];
  const result = await createAutomationSnapshotOrchestrator({ jobBoundary: target.boundary }).run({ userId: "user", platform: "tiktok", platformAccountId: "account", snapshotDate: "2026-08-28", scheduleId: "schedule", policy, write: async context => (writes.push(context), { snapshot: { id: context.sourceJobId + "-snapshot" } }) });
  assert.deepEqual(writes, [
    { sourceJobId: "primary-job", datePreset: "yesterday", snapshotDate: "2026-08-28", captureReason: "automation", snapshotClass: "primary" },
    { sourceJobId: "recovery-job", datePreset: "last_7d", snapshotDate: "2026-08-28", captureReason: "recovery", snapshotClass: "recovery" },
  ]);
  assert.equal(Object.isFrozen(writes[0]), true);
  assert.equal(target.calls[0].metadata.pairedPrimaryJobId, undefined);
  assert.equal(target.calls[2].metadata.pairedPrimaryJobId, "primary-job");
  assert.deepEqual(result.recoveryResult, { ok: true, job_id: "recovery-job", snapshot_id: "recovery-job-snapshot" });
});

test("keeps recovery failure isolated with correlated job evidence", async () => {
  const target = fakeBoundary({ failRecovery: true });
  const result = await createAutomationSnapshotOrchestrator({ jobBoundary: target.boundary }).run({ policy, write: async () => ({ snapshot: { id: "primary-snapshot" } }) });
  assert.equal(result.result.snapshot.id, "primary-snapshot");
  assert.deepEqual(result.recoveryResult, { ok: false, job_id: "recovery-job", error: "recovery failed" });
});

test("skips recovery when policy disables it", async () => {
  const target = fakeBoundary();
  const result = await createAutomationSnapshotOrchestrator({ jobBoundary: target.boundary }).run({ policy: { ...policy, shouldRunRecoverySnapshot: false }, write: async () => ({}) });
  assert.equal(target.calls.filter(item => item.work).length, 1);
  assert.equal(result.recoveryResult, null);
});

test("fails closed for missing dependencies", async () => {
  assert.throws(() => createAutomationSnapshotOrchestrator(), /jobBoundary.run/);
  const orchestrator = createAutomationSnapshotOrchestrator({ jobBoundary: { run() {} } });
  await assert.rejects(orchestrator.run({ write() {} }), /policy is required/);
  await assert.rejects(orchestrator.run({ policy: {} }), /write must be a function/);
});
