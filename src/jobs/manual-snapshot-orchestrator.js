"use strict";

function createManualSnapshotOrchestrator({ jobBoundary } = {}) {
  if (!jobBoundary || typeof jobBoundary.run !== "function") throw new TypeError("jobBoundary.run must be a function");

  async function run({ userId, platform, platformAccountId, datePreset, snapshotDate, jobMetadata = {}, complete, write } = {}) {
    if (typeof write !== "function") throw new TypeError("write must be a function");
    if (complete !== undefined && typeof complete !== "function") throw new TypeError("complete must be a function");
    return jobBoundary.run({
      userId,
      platform,
      platformAccountId,
      metadata: { ...jobMetadata, trigger: "manual", datePreset, snapshotDate, captureReason: "manual_refresh", snapshotClass: "primary" },
      work: ({ jobId }) => write(Object.freeze({ sourceJobId: jobId, captureReason: "manual_refresh", snapshotClass: "primary" })),
      completed: complete || ((result, job) => ({ snapshot_id: result.snapshot?.id || null, metadata: { ...(job.metadata || {}), performance_spread_result: result.performance_spread_result || null } })),
    });
  }

  return Object.freeze({ run });
}

module.exports = { createManualSnapshotOrchestrator };
