"use strict";

function snapshotSpreadJobEvidence(result, job) {
  return { snapshot_id: result.snapshot?.id || null, metadata: { ...(job?.metadata || {}), row_counts: result.row_counts || null, performance_spread_result: result.performance_spread_result || null } };
}

function recoverySnapshotSpreadJobEvidence(result) {
  return { snapshot_id: result.snapshot?.id || null, metadata: { row_counts: result.row_counts || null, performance_spread_result: result.performance_spread_result || null } };
}

module.exports = { snapshotSpreadJobEvidence, recoverySnapshotSpreadJobEvidence };
