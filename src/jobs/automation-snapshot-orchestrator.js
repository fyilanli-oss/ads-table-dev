"use strict";

function createAutomationSnapshotOrchestrator({ jobBoundary } = {}) {
  if (!jobBoundary || typeof jobBoundary.run !== "function") throw new TypeError("jobBoundary.run must be a function");

  function execute({ userId, platform, platformAccountId, snapshotDate, scheduleId, datePreset, captureReason, snapshotClass, pairedPrimaryJobId = null, extraMetadata = {}, complete, write }) {
    return jobBoundary.run({
      userId,
      platform,
      platformAccountId,
      metadata: { trigger: "automation", datePreset, snapshotDate, captureReason, snapshotClass, scheduleId, ...extraMetadata, ...(pairedPrimaryJobId ? { pairedPrimaryJobId } : {}) },
      work: ({ jobId }) => write(Object.freeze({ sourceJobId: jobId, datePreset, snapshotDate, captureReason, snapshotClass })),
      completed: complete || (result => ({ snapshot_id: result.snapshot?.id || null })),
    });
  }

  async function run({ userId, platform, platformAccountId, snapshotDate, scheduleId, policy, primaryMetadata = {}, recoveryMetadata = {}, primaryComplete, recoveryComplete, write } = {}) {
    if (!policy || typeof policy !== "object") throw new TypeError("policy is required");
    if (typeof write !== "function") throw new TypeError("write must be a function");
    if (primaryComplete !== undefined && typeof primaryComplete !== "function") throw new TypeError("primaryComplete must be a function");
    if (recoveryComplete !== undefined && typeof recoveryComplete !== "function") throw new TypeError("recoveryComplete must be a function");
    const primary = await execute({ userId, platform, platformAccountId, snapshotDate, scheduleId, datePreset: policy.datePreset, captureReason: policy.captureReason, snapshotClass: policy.snapshotClass, extraMetadata: primaryMetadata, complete: primaryComplete, write });
    let recoveryResult = null;
    if (policy.shouldRunRecoverySnapshot) {
      try {
        const recovery = await execute({ userId, platform, platformAccountId, snapshotDate, scheduleId, datePreset: policy.recoveryDatePreset, captureReason: policy.recoveryCaptureReason, snapshotClass: policy.recoverySnapshotClass, pairedPrimaryJobId: primary.job.id, extraMetadata: recoveryMetadata, complete: recoveryComplete, write });
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
