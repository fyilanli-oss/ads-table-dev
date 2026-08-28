"use strict";

function createAutomationSnapshotOrchestrator({ jobBoundary } = {}) {
  if (!jobBoundary || typeof jobBoundary.run !== "function") throw new TypeError("jobBoundary.run must be a function");

  function execute({ userId, platform, platformAccountId, snapshotDate, scheduleId, datePreset, captureReason, snapshotClass, pairedPrimaryJobId = null, write }) {
    return jobBoundary.run({
      userId,
      platform,
      platformAccountId,
      metadata: { trigger: "automation", datePreset, snapshotDate, captureReason, snapshotClass, scheduleId, ...(pairedPrimaryJobId ? { pairedPrimaryJobId } : {}) },
      work: ({ jobId }) => write(Object.freeze({ sourceJobId: jobId, datePreset, snapshotDate, captureReason, snapshotClass })),
      completed: result => ({ snapshot_id: result.snapshot?.id || null }),
    });
  }

  async function run({ userId, platform, platformAccountId, snapshotDate, scheduleId, policy, write } = {}) {
    if (!policy || typeof policy !== "object") throw new TypeError("policy is required");
    if (typeof write !== "function") throw new TypeError("write must be a function");
    const primary = await execute({ userId, platform, platformAccountId, snapshotDate, scheduleId, datePreset: policy.datePreset, captureReason: policy.captureReason, snapshotClass: policy.snapshotClass, write });
    let recoveryResult = null;
    if (policy.shouldRunRecoverySnapshot) {
      try {
        const recovery = await execute({ userId, platform, platformAccountId, snapshotDate, scheduleId, datePreset: policy.recoveryDatePreset, captureReason: policy.recoveryCaptureReason, snapshotClass: policy.recoverySnapshotClass, pairedPrimaryJobId: primary.job.id, write });
        recoveryResult = { ok: true, job_id: recovery.job.id, snapshot_id: recovery.result.snapshot?.id || null };
      } catch (error) {
        recoveryResult = { ok: false, job_id: error.refreshJob?.id || null, error: error.message };
      }
    }
    return Object.freeze({ job: primary.job, result: primary.result, recoveryResult });
  }

  return Object.freeze({ run });
}

module.exports = { createAutomationSnapshotOrchestrator };
